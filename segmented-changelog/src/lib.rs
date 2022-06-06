use rusqlite::named_params;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::Path;

use thiserror::Error;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct NodeId(usize);

#[derive(Debug, Error)]
pub enum Error {
    #[error("Could not open database: {0}")]
    OpenDatabase(rusqlite::Error),

    #[error("Could not initialize database: {0}")]
    InitializeDatabase(rusqlite::Error),

    #[error("Could not create node {node:?}: {error}")]
    CreateNode {
        node: Box<dyn Debug>,
        error: rusqlite::Error,
    },

    #[error("Could not query for ID for node {node:?}: {error}")]
    QueryNodeId {
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
    id INTEGER PRIMARY KEY NOT NULL,
    name BLOB NOT NULL
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
        let all_nodes = {
            let mut acc = HashSet::new();
            let mut next = {
                let mut next = HashSet::new();
                next.insert(node.clone());
                next
            };

            // TODO: Be sure to traverse depth-first, in order to assign contiguous sequences of IDs.
            while !next.is_empty() {
                let parents: HashSet<_> = next
                    .iter()
                    .flat_map(|node| self.backend.get_parents(node))
                    .collect();
                acc.extend(next.into_iter());

                // TODO: Filter any parents which have already been registered with the database.
                next = parents;
            }
            let mut acc: Vec<_> = acc.into_iter().collect();
            acc.sort();
            acc
        };

        for node in all_nodes {
            self.db
                .execute(
                    "
INSERT INTO names (name)
VALUES (:name)
",
                    named_params! {
                        ":name": node.as_ref(),
                    },
                )
                .map_err(|error| Error::CreateNode {
                    node: Box::new(node.clone()),
                    error,
                })?;
        }

        let id: usize = self
            .db
            .query_row(
                "
SELECT rowid
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
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestingDagBackend;

    impl DagBackend for TestingDagBackend {
        type Node = Vec<u8>;

        fn get_parents(&self, node: &Self::Node) -> Vec<Self::Node> {
            match node.as_slice() {
                b"foo" => vec![b"bar".to_vec()],
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
    fn test_init() -> Result<()> {
        let mut dag = make_in_memory_dag()?;
        let foo_id = dag.make_node(&b"foo".to_vec())?;
        assert_eq!(foo_id, NodeId(2));

        let bar_id = dag.make_node(&b"bar".to_vec())?;
        assert_eq!(bar_id, NodeId(1));

        Ok(())
    }
}
