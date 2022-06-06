use rusqlite::named_params;
use std::collections::HashSet;
use std::path::Path;

use thiserror::Error;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct NodeId(usize);

pub type NodeName = [u8];

pub type NodeNameOwned = <NodeName as ToOwned>::Owned;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Could not open database: {0}")]
    OpenDatabase(rusqlite::Error),

    #[error("Could not initialize database: {0}")]
    InitializeDatabase(rusqlite::Error),

    #[error("Could not create node {name:?}: {error}")]
    CreateNode {
        name: Vec<u8>,
        error: rusqlite::Error,
    },

    #[error("Could not query for ID for node {name:?}: {error}")]
    QueryNodeId {
        name: Vec<u8>,
        error: rusqlite::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Dag {
    db: rusqlite::Connection,
}

impl Dag {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = rusqlite::Connection::open(path).map_err(Error::OpenDatabase)?;
        let dag = Self::open_from_conn(conn)?;
        Ok(dag)
    }

    pub fn open_from_conn(conn: rusqlite::Connection) -> Result<Self> {
        let mut dag = Dag { db: conn };
        dag.init_db()?;
        Ok(dag)
    }

    fn init_db(&mut self) -> Result<()> {
        self.db
            .execute(
                r#"
CREATE TABLE IF NOT EXISTS
names (name BLOB)
"#,
                [],
            )
            .map_err(Error::InitializeDatabase)?;
        Ok(())
    }

    pub fn make_node(
        &mut self,
        node: &NodeName,
        get_parents: impl Fn(&NodeName) -> Vec<NodeNameOwned>,
    ) -> Result<NodeId> {
        let all_nodes = {
            let mut acc = HashSet::new();
            let mut next = {
                let mut next = HashSet::new();
                next.insert(node.to_owned());
                next
            };

            // TODO: Be sure to traverse depth-first, in order to assign contiguous sequences of IDs.
            while !next.is_empty() {
                let parents: HashSet<_> = next.iter().flat_map(|node| get_parents(node)).collect();
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
INSERT INTO names
VALUES (:name)
",
                    named_params! {
                        ":name": node,
                    },
                )
                .map_err(|error| Error::CreateNode {
                    name: node.to_vec(),
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
                named_params! {":name": node},
                |row| row.get(0),
            )
            .map_err(|error| Error::QueryNodeId {
                name: node.to_vec(),
                error,
            })?;
        let id = NodeId(id);
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_in_memory_dag() -> Result<Dag> {
        let conn = rusqlite::Connection::open_in_memory().map_err(Error::InitializeDatabase)?;
        let dag = Dag::open_from_conn(conn)?;
        Ok(dag)
    }

    #[test]
    fn test_init() -> Result<()> {
        let mut dag = make_in_memory_dag()?;
        let foo_id = dag.make_node(b"foo", |name| match name {
            b"foo" => vec![b"bar".to_vec()],
            _ => vec![],
        })?;
        assert_eq!(foo_id, NodeId(2));

        let bar_id = dag.make_node(b"bar", |_| Default::default())?;
        assert_eq!(bar_id, NodeId(1));

        Ok(())
    }
}
