use segmented_changelog::{Dag, DagBackend};
use std::path::PathBuf;

struct RepoDagBackend {
    repo: git2::Repository,
}

impl DagBackend for RepoDagBackend {
    type Error = git2::Error;
    type Node = git2::Oid;

    fn get_parents(&self, node: &Self::Node) -> Result<Vec<Self::Node>, Self::Error> {
        let parents = self.repo.find_commit(*node)?.parent_ids().collect();
        Ok(parents)
    }

    fn try_from_bytes(&self, bytes: Vec<u8>) -> Result<Self::Node, Self::Error> {
        git2::Oid::from_bytes(&bytes)
    }
}

type RepoDag = Dag<RepoDagBackend>;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let args: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let exit_code = match args.as_slice() {
        [_, repo_path, oid1, oid2] => {
            let oid1 = git2::Oid::from_str(oid1).unwrap();
            let oid2 = git2::Oid::from_str(oid2).unwrap();

            let repo_path = PathBuf::from(repo_path);
            let repo = git2::Repository::discover(&repo_path).unwrap();
            let expected_merge_bases = repo.merge_bases_many(&[oid1, oid2]).unwrap();
            let expected_merge_bases = {
                let mut merge_bases = expected_merge_bases.to_vec();
                merge_bases.sort();
                merge_bases
            };

            let repo_path = repo.path().to_path_buf();
            let backend = RepoDagBackend { repo };
            let mut dag =
                RepoDag::open(backend, &repo_path.join("segmented-changelog-db.sqlite3")).unwrap();

            let commit_set = dag.query().commits(&[oid1, oid2]).unwrap();
            let actual_merge_bases = dag.query().gca_one(&commit_set).unwrap();
            let actual_merge_bases = dag.query().resolve(&actual_merge_bases).unwrap();

            println!("Expected: {expected_merge_bases:?}");
            println!("Actual: {actual_merge_bases:?}");

            assert_eq!(actual_merge_bases.len(), 1);
            if !expected_merge_bases.contains(&actual_merge_bases[0]) {
                println!("This is a bug!");
                1
            } else {
                0
            }
        }
        args => {
            panic!("Bad arguments, try: cargo run --example git-gca -- path/to/repo abc123 def456: {:?}", args);
        }
    };
    std::process::exit(exit_code);
}
