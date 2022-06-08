use rusqlite::{named_params, params, OptionalExtension};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::path::Path;

use thiserror::Error;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(usize);

#[derive(Clone, Debug, PartialEq, Eq)]
struct Segment {
    level: usize,
    parent_ids: Vec<NodeId>,
    start_id: NodeId,
    end_id: NodeId,
}

impl Segment {
    fn contains(&self, id: NodeId) -> bool {
        self.start_id <= id && id <= self.end_id
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Could not open database: {0}")]
    OpenDatabase(#[source] rusqlite::Error),

    #[error("Could not initialize database: {0}")]
    InitializeDatabase(#[source] rusqlite::Error),

    #[error("Could not insert into database: {0}")]
    InsertIntoDatabase(#[source] rusqlite::Error),

    #[error("Could not query for node with ID {node_id:?}: {error}")]
    QueryNode {
        node_id: NodeId,
        #[source]
        error: Box<dyn std::error::Error>,
    },

    #[error("Could not find node with ID {node_id:?}")]
    QueryNodeNotFound { node_id: NodeId },

    #[error("Could not query for ID for node {node:?}: {error}")]
    QueryNodeId {
        node: Box<dyn Debug>,
        #[source]
        error: Box<dyn std::error::Error>,
    },

    #[error("Could not query for segment containing node {node:?}: {error}")]
    QuerySegment {
        node: Box<dyn Debug>,
        #[source]
        error: rusqlite::Error,
    },

    #[error("Could not query parents for node {node:?}: {error}")]
    QueryParents {
        node: Box<dyn Debug>,
        #[source]
        error: Box<dyn std::error::Error>,
    },

    #[error("Could not build in-memory graph: {0}")]
    BuildGraph(#[source] Box<Error>),

    #[error("Could not assign ID for node {node:?}: {error}")]
    AssignNodeIds {
        node: Box<dyn Debug>,
        #[source]
        error: Box<Error>,
    },

    #[error("Could not create node {node:?}: {error}")]
    AssignNodeId {
        node: Box<dyn Debug>,
        #[source]
        error: rusqlite::Error,
    },

    #[error("Could not save segment: {0}")]
    SaveSegment(#[source] rusqlite::Error),

    #[error("Backend error: {0}")]
    BackendError(#[source] Box<dyn std::error::Error>),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait DagBackend {
    type Error: Debug + std::error::Error + 'static;
    type Node: Clone + Debug + Eq + Ord + Hash + AsRef<[u8]> + 'static;

    fn get_parents(&self, node: &Self::Node) -> std::result::Result<Vec<Self::Node>, Self::Error>;
    fn try_from_bytes(&self, bytes: Vec<u8>) -> std::result::Result<Self::Node, Self::Error>;
}

pub struct Dag<B: DagBackend> {
    backend: B,
    db: rusqlite::Connection,
}

impl<B: DagBackend> Dag<B> {
    pub fn open(backend: B, path: &Path) -> Result<Self> {
        let conn = rusqlite::Connection::open(path).map_err(Error::OpenDatabase)?;
        let dag = Self::open_from_conn(backend, conn)?;
        Ok(dag)
    }

    pub fn open_from_conn(backend: B, conn: rusqlite::Connection) -> Result<Self> {
        let mut dag = Dag { backend, db: conn };
        dag.init_db()?;
        Ok(dag)
    }

    fn init_db(&mut self) -> Result<()> {
        self.db
            .execute_batch(
                r#"
CREATE TABLE IF NOT EXISTS
names (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    name BLOB NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS
segments (
    level INTEGER NOT NULL,
    parents TEXT NOT NULL,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    PRIMARY KEY (level, start)
);
"#,
            )
            .map_err(Error::InitializeDatabase)?;
        Ok(())
    }

    pub fn make_set(&mut self, node: &B::Node) -> Result<CommitSet> {
        let node_id = self.make_node(node)?;
        let parents = self.query_parents(node)?;
        let parent_ids: Vec<NodeId> = parents
            .into_iter()
            .map(|parent| {
                let node_id = self.query_node_id(&parent)?;
                Ok(node_id.expect("Parent ID should exist"))
            })
            .collect::<Result<_>>()?;

        let segment = Segment {
            level: 0,
            parent_ids,
            start_id: node_id,
            end_id: node_id,
        };
        Ok(CommitSet {
            segments: vec![segment],
        })
    }

    pub fn make_node(&mut self, node: &B::Node) -> Result<NodeId> {
        let graph = self
            .build_graph(node.clone())
            .map_err(|error| Error::BuildGraph(Box::new(error)))?;
        self.assign_node_ids(&graph, node)
            .map_err(|error| Error::AssignNodeIds {
                node: Box::new(node.clone()),
                error: Box::new(error),
            })?;

        let id = self
            .query_node_id(node)?
            .expect("Should have just created the node ID");
        let segment = self.calculate_segment(node, &graph)?;
        self.save_segment(&segment)?;
        Ok(id)
    }

    /// Build an in-memory representation of the DAG, determined by repeatedly
    /// traversing the parents of `node`.
    ///
    /// The graph is a mapping from node to parent nodes.
    ///
    /// This graph may not be complete. If a node has already been registered
    /// in the database, then it may be omitted as a key from the resulting graph.
    fn build_graph(&self, node: B::Node) -> Result<HashMap<B::Node, Vec<B::Node>>> {
        let mut current: HashSet<B::Node> = {
            let mut current = HashSet::new();
            current.insert(node);
            current
        };
        let mut graph = HashMap::new();
        while !current.is_empty() {
            let mut next = HashSet::new();
            for node in current {
                if graph.contains_key(&node) {
                    continue;
                }
                if self.query_node_id(&node)?.is_some() {
                    // FIXME: Issuing a database query for each node may be
                    // expensive. It might be better to issue a query only
                    // once every few nodes instead.
                    continue;
                }

                let parents =
                    self.backend
                        .get_parents(&node)
                        .map_err(|error| Error::QueryParents {
                            node: Box::new(node.clone()),
                            error: Box::new(error),
                        })?;
                next.extend(parents.iter().cloned());
                graph.insert(node.to_owned(), parents);
            }
            current = next;
        }
        Ok(graph)
    }

    /// Assign a unique ID to each node in the graph, starting from
    /// `initial_node` and traversing its parents. The IDs are stored in the
    /// database. Any node which already has an ID keeps it.
    fn assign_node_ids(
        &mut self,
        graph: &HashMap<B::Node, Vec<B::Node>>,
        initial_node: &B::Node,
    ) -> Result<()> {
        #[derive(Debug)]
        enum State<T> {
            WaitingForParents(T),
            ReadyToAssign(T),
        }

        let mut nodes_to_assign: Vec<State<&B::Node>> =
            vec![State::WaitingForParents(initial_node)];
        let mut assigned_nodes: HashSet<&B::Node> = HashSet::new();
        let mut assigned_nodes_in_order: Vec<&B::Node> = Vec::new();
        while let Some(node_to_assign) = nodes_to_assign.pop() {
            match node_to_assign {
                State::WaitingForParents(node_to_assign) => {
                    if !assigned_nodes.insert(node_to_assign) {
                        continue;
                    }

                    let parents = match graph.get(node_to_assign) {
                        Some(parents) => parents,
                        None => {
                            // This was a leaf node, which indicates that it
                            // was already assigned an ID. We can skip this
                            // node.
                            continue;
                        }
                    };

                    // Be sure to insert the parent nodes such that the first
                    // parent is at the top. We want to traverse the first
                    // parents first to provide a contiguous sequence of IDs.
                    nodes_to_assign.push(State::ReadyToAssign(node_to_assign));
                    nodes_to_assign.extend(parents.iter().rev().map(State::WaitingForParents));
                }

                State::ReadyToAssign(node_to_assign) => {
                    // Skip reassigning the same node if we've already seen it (such as
                    // if two nodes have the same parent).
                    assigned_nodes_in_order.push(node_to_assign);
                }
            }
        }

        let tx = self.db.transaction().map_err(Error::InsertIntoDatabase)?;
        {
            let mut stmt = tx
                .prepare(
                    "
INSERT INTO names (name)
VALUES (?)
",
                )
                .map_err(Error::InsertIntoDatabase)?;
            for node in assigned_nodes_in_order {
                let result = stmt.execute(params![node.as_ref()]);
                match result {
                    Ok(_) => {
                        // ID assignment succeeded, do nothing.
                    }

                    Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error {
                            code: rusqlite::ErrorCode::ConstraintViolation,
                            ..
                        },
                        _,
                    )) => {
                        // An ID was already assigned for this element, do nothing.
                    }

                    Err(error) => {
                        return Err(Error::AssignNodeId {
                            node: Box::new(initial_node.clone()),
                            error,
                        });
                    }
                }
            }
        }
        tx.commit().map_err(Error::InsertIntoDatabase)?;

        Ok(())
    }

    fn query_parents(&self, node: &B::Node) -> Result<Vec<B::Node>> {
        Ok(self
            .backend
            .get_parents(node)
            .map_err(|error| Error::QueryParents {
                node: Box::new(node.clone()),
                error: Box::new(error),
            })?)
    }

    fn query_node_or_fail(&self, node_id: NodeId) -> Result<B::Node> {
        let node = self
            .query_node(node_id)
            .map_err(|error| Error::QueryNode {
                node_id,
                error: Box::new(error),
            })?
            .ok_or_else(|| Error::QueryNodeNotFound { node_id })?;
        Ok(node)
    }

    fn query_node(&self, node_id: NodeId) -> Result<Option<B::Node>> {
        let NodeId(id) = node_id;
        let bytes: Option<Vec<u8>> = self
            .db
            .query_row(
                "
SELECT name
FROM names
WHERE id = :id
        ",
                named_params! {
                    ":id": id,
                },
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| Error::QueryNode {
                node_id,
                error: Box::new(error),
            })?;

        match bytes {
            Some(bytes) => {
                let node = self
                    .backend
                    .try_from_bytes(bytes)
                    .map_err(|error| Error::BackendError(Box::new(error)))?;
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    fn query_node_id(&self, node: &B::Node) -> Result<Option<NodeId>> {
        let node_id: Option<usize> = self
            .db
            .query_row(
                "
SELECT id
FROM names
WHERE name = :name
        ",
                named_params! {
                    ":name": node.as_ref(),
                },
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| Error::QueryNodeId {
                node: Box::new(node.clone()),
                error: Box::new(error),
            })?;
        Ok(node_id.map(NodeId))
    }

    fn calculate_segment(
        &self,
        end: &B::Node,
        graph: &HashMap<B::Node, Vec<B::Node>>,
    ) -> Result<Segment> {
        let end_id = self
            .query_node_id(end)?
            .expect("End of segment should exist");
        let mut start = end.clone();
        let parents = loop {
            let parents = match graph.get(&start) {
                Some(parents) => parents.to_vec(),
                None => self.query_parents(&start)?,
            };

            match parents.as_slice() {
                [only_parent] => {
                    start = only_parent.clone();
                }
                parents => {
                    break parents.to_vec();
                }
            }
        };

        let parent_ids = parents
            .iter()
            .map(|parent| -> Result<_> {
                let node_id = self.query_node_id(parent)?;
                Ok(node_id.expect("Parent should exist"))
            })
            .collect::<Result<_>>()?;
        let start_id = self
            .query_node_id(&start)?
            .expect("Start of segment should exist");
        Ok(Segment {
            level: 0, // higher-level segments not yet supported
            parent_ids,
            start_id,
            end_id,
        })
    }

    fn save_segment(&self, segment: &Segment) -> Result<()> {
        let Segment {
            level,
            parent_ids: parents,
            start_id: NodeId(start_id),
            end_id: NodeId(end_id),
        } = segment;
        let parents = format!(
            "/{}/",
            parents
                .iter()
                .map(|NodeId(parent_id)| parent_id.to_string())
                .collect::<Vec<_>>()
                .join("/")
        );
        self.db
            .execute(
                "
INSERT INTO segments (level, parents, start, end)
VALUES (:level, :parents, :start, :end)
ON CONFLICT DO UPDATE
SET end = :end
",
                named_params! {
                    ":level": level,
                    ":parents": parents,
                    ":start": start_id,
                    ":end": end_id,
                },
            )
            .map_err(Error::SaveSegment)?;
        Ok(())
    }

    fn get_singleton_segment(&self, segment: &Segment, node_id: NodeId) -> Segment {
        assert!(segment.start_id <= node_id);
        assert!(node_id <= segment.end_id);

        let parent_ids = if node_id == segment.start_id {
            segment.parent_ids.clone()
        } else {
            let NodeId(node_id) = node_id;
            vec![NodeId(node_id.checked_sub(1).unwrap())]
        };
        Segment {
            level: 0,
            parent_ids,
            start_id: node_id,
            end_id: node_id,
        }
    }

    fn query_segment(&self, node_id: NodeId) -> Result<Option<Segment>> {
        let NodeId(node_id) = node_id;
        let result = self
            .db
            .query_row(
                "
SELECT level, parents, start, end
FROM segments
WHERE :id BETWEEN start AND end
        ",
                named_params! {
                    ":id": node_id,
                },
                |row| {
                    let level: usize = row.get(0)?;
                    let parents: String = row.get(1)?;
                    let start: usize = row.get(2)?;
                    let end: usize = row.get(3)?;
                    Ok((level, parents, start, end))
                },
            )
            .optional()
            .map_err(|error| Error::QuerySegment {
                node: Box::new(node_id.clone()),
                error,
            })?;

        let (level, parents, start, end) = match result {
            None => {
                return Ok(None);
            }
            Some(result) => result,
        };

        let parents: Vec<_> = parents
            .split("/")
            .filter(|piece| !piece.is_empty())
            .map(|parent| {
                let node_id: usize = parent.parse()?;
                Ok(NodeId(node_id))
            })
            .collect::<std::result::Result<_, _>>()
            .map_err(|error| Error::QueryNodeId {
                node: Box::new(node_id.clone()),
                error,
            })?;
        Ok(Some(Segment {
            level,
            parent_ids: parents,
            start_id: NodeId(start),
            end_id: NodeId(end),
        }))
    }

    pub fn query(&mut self) -> Query<B> {
        Query { dag: self }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CommitSet {
    segments: Vec<Segment>,
}

impl CommitSet {
    pub fn union(&self, other: &CommitSet) -> CommitSet {
        let Self { segments } = self;
        let CommitSet {
            segments: other_segments,
        } = other;
        CommitSet {
            segments: [segments.clone(), other_segments.clone()].concat(),
        }
    }
}

pub struct Query<'a, B: DagBackend> {
    dag: &'a mut Dag<B>,
}

impl<'a, B: DagBackend> Query<'a, B> {
    pub fn commits(&mut self, nodes: &[B::Node]) -> Result<CommitSet> {
        let nodes: Vec<_> = nodes
            .into_iter()
            .map(|node| self.dag.make_set(&node))
            .collect::<Result<_>>()?;
        let commits: CommitSet = nodes
            .into_iter()
            .fold(Default::default(), |acc, item| acc.union(&item));
        Ok(commits)
    }

    pub fn resolve(&mut self, commits: &CommitSet) -> Result<Vec<B::Node>> {
        let mut result = Vec::new();
        for segment in &commits.segments {
            let NodeId(start) = segment.start_id;
            let NodeId(end) = segment.end_id;
            for i in start..=end {
                result.push(self.dag.query_node_or_fail(NodeId(i))?);
            }
        }
        Ok(result)
    }

    pub fn heads(&self, commits: CommitSet) -> Result<CommitSet> {
        let head_segments = commits.segments.iter().filter(|segment| {
            for other in &commits.segments {
                for parent in &segment.parent_ids {
                    if other.contains(*parent) {
                        return false;
                    }
                }
            }
            true
        });

        let segments = head_segments
            .into_iter()
            .map(|head_segment| {
                self.dag
                    .get_singleton_segment(head_segment, head_segment.start_id)
            })
            .collect();
        Ok(CommitSet { segments })
    }

    pub fn roots(&self, commits: CommitSet) -> Result<CommitSet> {
        let root_segments = commits.segments.iter().filter(|segment| {
            for other in &commits.segments {
                for parent in &other.parent_ids {
                    if segment.contains(*parent) {
                        return false;
                    }
                }
            }
            true
        });

        let segments = root_segments
            .into_iter()
            .map(|head_segment| {
                self.dag
                    .get_singleton_segment(head_segment, head_segment.start_id)
            })
            .collect();
        Ok(CommitSet { segments })
    }

    pub fn gca_one(&self, commits: &CommitSet) -> Result<CommitSet> {
        let CommitSet { segments } = commits;

        // Pointers to the nodes that we'll be traversing in the segment DAG.
        let target_length = segments.len();
        let mut roots: Vec<Segment> = segments.clone();

        // Set of seen node counts, indexed by their end IDs.
        let mut end_to_segments: HashMap<NodeId, usize> =
            segments.iter().map(|segment| (segment.end_id, 0)).collect();

        while !roots.is_empty() {
            // Visit the parent of each current segment.
            let parent_segments = {
                let mut result = Vec::new();
                for root in roots {
                    for parent_id in &root.parent_ids {
                        let parent_segment = match self.dag.query_segment(*parent_id)? {
                            Some(segment) => segment,
                            None => continue,
                        };

                        // Mark this node as visited.
                        let count = end_to_segments.entry(parent_segment.end_id).or_default();
                        *count += 1;
                        if *count == target_length {
                            // If the node was already visited, return it as a GCA.
                            // FIXME: not correct if the original `roots`
                            // has more than two nodes -- we need to
                            // confirm that *all* nodes in the set reach
                            // the same intermediate node.
                            let segment = self.dag.query_segment(parent_segment.end_id)?.unwrap();
                            return Ok(CommitSet {
                                segments: vec![self
                                    .dag
                                    .get_singleton_segment(&segment, parent_segment.start_id)],
                            });
                        } else {
                            // Otherwise, save it for the next iteration.
                            result.push(parent_segment);
                        }
                    }
                }
                result
            };
            roots = parent_segments;
        }

        // Reached the roots of the entire graph without finding a GCA; return.
        Ok(CommitSet::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestingDagBackend;

    impl DagBackend for TestingDagBackend {
        type Error = std::string::FromUtf8Error;
        type Node = String;

        fn get_parents(
            &self,
            node: &Self::Node,
        ) -> std::result::Result<Vec<Self::Node>, Self::Error> {
            let parents = match node.as_str() {
                "foo" => vec!["bar".to_string()],
                "baz" => vec!["bar".to_string(), "foo".to_string()],
                "qux" => vec!["bar".to_string()],
                "qux2" => vec!["qux".to_string()],
                "qux3" => vec!["qux2".to_string()],
                _ => vec![],
            };
            Ok(parents)
        }

        fn try_from_bytes(&self, bytes: Vec<u8>) -> std::result::Result<Self::Node, Self::Error> {
            String::from_utf8(bytes)
        }
    }

    type TestingDag = Dag<TestingDagBackend>;

    fn make_in_memory_dag() -> Result<TestingDag> {
        let conn = rusqlite::Connection::open_in_memory().map_err(Error::InitializeDatabase)?;
        let dag = Dag::open_from_conn(TestingDagBackend, conn)?;
        Ok(dag)
    }

    #[test]
    fn test_init() -> Result<()> {
        let _: TestingDag = make_in_memory_dag()?;
        Ok(())
    }

    #[test]
    fn test_id_assignment() -> Result<()> {
        let mut dag = make_in_memory_dag()?;
        let foo_id = dag.make_node(&"foo".to_string())?;
        let foo_id2 = dag.make_node(&"foo".to_string())?;
        assert_eq!(foo_id, foo_id2);

        let bar_id = dag.make_node(&"bar".to_string())?;
        assert!(bar_id < foo_id, "parent node {bar_id:?} should have smallerID (been assigned first) than child node {foo_id:?}");
        assert_eq!(foo_id, NodeId(2));
        assert_eq!(bar_id, NodeId(1));

        let baz_id = dag.make_node(&"baz".to_string())?;
        assert!(bar_id < baz_id);
        assert!(foo_id < baz_id);

        Ok(())
    }

    #[test]
    fn test_query_node() -> Result<()> {
        let mut dag = make_in_memory_dag()?;
        let foo_id = dag.make_node(&"foo".to_string())?;
        let bar_id = dag.make_node(&"bar".to_string())?;
        let commits = dag
            .query()
            .commits(&["foo".to_string(), "bar".to_string()])?;
        assert_eq!(
            commits.segments,
            vec![
                Segment {
                    level: 0,
                    parent_ids: vec![bar_id],
                    start_id: foo_id,
                    end_id: foo_id,
                },
                Segment {
                    level: 0,
                    parent_ids: vec![],
                    start_id: bar_id,
                    end_id: bar_id,
                }
            ]
        );
        Ok(())
    }

    #[test]
    fn test_query_heads() -> Result<()> {
        let mut dag = make_in_memory_dag()?;
        let commits = dag
            .query()
            .commits(&["foo".to_string(), "bar".to_string()])?;
        let commits = dag.query().heads(commits)?;
        assert_eq!(dag.query().resolve(&commits)?, vec!["bar".to_string()]);
        Ok(())
    }

    #[test]
    fn test_query_roots() -> Result<()> {
        let mut dag = make_in_memory_dag()?;
        let commits = dag
            .query()
            .commits(&["foo".to_string(), "bar".to_string()])?;
        let commits = dag.query().roots(commits)?;
        assert_eq!(dag.query().resolve(&commits)?, vec!["foo".to_string()]);
        Ok(())
    }

    #[test]
    fn test_query_gca_one() -> Result<()> {
        let mut dag = make_in_memory_dag()?;
        let commits = dag
            .query()
            .commits(&["foo".to_string(), "qux3".to_string()])?;
        let commits = dag.query().gca_one(&commits)?;
        assert_eq!(dag.query().resolve(&commits)?, vec!["bar".to_string()]);
        Ok(())
    }
}
