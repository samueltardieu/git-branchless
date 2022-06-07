use rusqlite::{named_params, OptionalExtension};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::path::Path;

use thiserror::Error;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeId(usize);

#[derive(Debug, Error)]
pub enum Error {
    #[error("Could not open database: {0}")]
    OpenDatabase(rusqlite::Error),

    #[error("Could not initialize database: {0}")]
    InitializeDatabase(rusqlite::Error),

    #[error("Could not query for ID for node {node:?}: {error}")]
    QueryNodeId {
        node: Box<dyn Debug>,
        error: rusqlite::Error,
    },

    #[error("Could not build in-memory graph: {0}")]
    BuildGraph(Box<Error>),

    #[error("Could not assign ID for node {node:?}: {error}")]
    AssignNodeIds {
        node: Box<dyn Debug>,
        error: Box<Error>,
    },

    #[error("Could not create node {node:?}: {error}")]
    AssignNodeId {
        node: Box<dyn Debug>,
        error: rusqlite::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait DagBackend {
    type Node: Clone + Debug + Eq + Ord + Hash + AsRef<[u8]> + 'static;

    fn get_parents(&self, node: &Self::Node) -> Vec<Self::Node>;
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
    parents TEXT NOT NULL,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL
);
"#,
            )
            .map_err(Error::InitializeDatabase)?;
        Ok(())
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

        let id: usize = self
            .db
            .query_row(
                "
SELECT id
FROM names
WHERE name = :name
",
                named_params! {":name": node.as_ref()},
                |row| row.get(0),
            )
            .map_err(|error| Error::QueryNodeId {
                node: Box::new(node.clone()),
                error,
            })?;
        let id = NodeId(id);
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
        let mut current = {
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

                let parents = self.backend.get_parents(&node);
                graph.insert(node, parents.clone());
                next.extend(parents);
            }
            current = next;
        }
        Ok(graph)
    }

    fn assign_node_ids(
        &self,
        graph: &HashMap<B::Node, Vec<B::Node>>,
        initial_node: &B::Node,
    ) -> Result<()> {
        #[derive(Debug)]
        enum State<T> {
            WaitingForParents(T),
            ReadyToAssign(T),
        }

        let mut nodes_to_assign: Vec<State<&B::Node>> = vec![State::WaitingForParents(initial_node)];
        let mut assigned_nodes: HashSet<&B::Node> = HashSet::new();
        while let Some(node_to_assign) = nodes_to_assign.pop() {
            match node_to_assign {
                State::WaitingForParents(node_to_assign) => {
                    let parents = match graph.get(node_to_assign) {
                        Some(parents) => parents,
                        None => {
                            // This was a leaf node, which indicates that it was already assigned an ID. We can skip this node.
                            continue;
                        }
                    };

                    // Be sure to insert the parent nodes such that the first parent is at the top. We want to traverse the first parents first to provide a contiguous sequence of IDs.
                    nodes_to_assign.push(State::ReadyToAssign(node_to_assign));
                    nodes_to_assign.extend(parents.iter().rev().map(State::WaitingForParents));
                }

                State::ReadyToAssign(node_to_assign) => {
                    // Skip reassigning the same node if we've already seen it (such as
                    // if two nodes have the same parent).
                    if !assigned_nodes.insert(node_to_assign) {
                        continue;
                    }

                    let result = self.db.execute(
                        "
INSERT INTO names (name)
VALUES (:name)
",
                        named_params! {
                            ":name": node_to_assign.as_ref(),
                        },
                    );
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
        }

        Ok(())
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
                error,
            })?;
        Ok(node_id.map(NodeId))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestingDagBackend;

    impl DagBackend for TestingDagBackend {
        type Node = String;

        fn get_parents(&self, node: &Self::Node) -> Vec<Self::Node> {
            match node.as_str() {
                "foo" => vec!["bar".to_string()],
                _ => vec![],
            }
        }
    }

    type TestingDag = Dag<TestingDagBackend>;

    fn make_in_memory_dag() -> Result<TestingDag> {
        let conn = rusqlite::Connection::open_in_memory().map_err(Error::InitializeDatabase)?;
        let dag = Dag::open_from_conn(TestingDagBackend, conn)?;
        Ok(dag)
    }

    #[test]
    fn test_id_assignment() -> Result<()> {
        let mut dag = make_in_memory_dag()?;
        let foo_id = dag.make_node(&"foo".to_string())?;
        let foo_id2 = dag.make_node(&"foo".to_string())?;
        assert_eq!(foo_id, foo_id2);

        let bar_id = dag.make_node(&"bar".to_string())?;
        assert!(bar_id < foo_id, "parent node {bar_id:?} should have smaller ID (been assigned first) than child node {foo_id:?}");
        assert_eq!(foo_id, NodeId(2));
        assert_eq!(bar_id, NodeId(1));

        Ok(())
    }
}
