use criterion::{criterion_group, criterion_main, Criterion};
use segmented_changelog::{Dag, DagBackend, NodeId};

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
}

type RepoDag = Dag<RepoDagBackend>;

struct Context {
    head_oid: git2::Oid,
    db_file: tempfile::NamedTempFile,
    dag: RepoDag,
}

fn make_repo_dag() -> Context {
    let repo_dir =
        std::env::var("PATH_TO_REPO").expect("`PATH_TO_REPO` environment variable not set");
    let repo = git2::Repository::open(repo_dir).unwrap();
    let head_oid = repo.head().unwrap().peel_to_commit().unwrap().id();
    let backend = RepoDagBackend { repo };
    let db_file = tempfile::NamedTempFile::new().unwrap();
    let db = rusqlite::Connection::open(db_file.path()).unwrap();
    let dag = Dag::open_from_conn(backend, db).unwrap();
    Context {
        head_oid,
        db_file,
        dag,
    }
}

fn bench_assign_ids(c: &mut Criterion) {
    c.bench_function("assign_ids", |b| {
        let Context {
            head_oid,
            db_file: _db_file,
            mut dag,
        } = make_repo_dag();

        b.iter(|| -> NodeId { dag.make_node(&head_oid).unwrap() });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets =
        bench_assign_ids,
);
criterion_main!(benches);
