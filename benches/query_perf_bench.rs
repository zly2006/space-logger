use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use space_logger::{
    DbOptions, IntPredicate, LongPredicate, Query, Row, SpaceLoggerDb, VERB_BREAK, VERB_HURT,
    VERB_PLACE, VERB_USE, verb_mask_single,
};

const HOT_DATASET_ROWS: usize = 1_200_000;
const COLD_DATASET_ROWS: usize = 1_200_000;
const COLD_SEGMENT_ROWS: usize = 2_000;
const FANOUT_DATASET_ROWS: usize = 600_000;
const FANOUT_SEGMENT_ROWS: usize = 2_000;
const INSERT_CHUNK_SIZE: usize = 500;
const HOT_QUERY_LIMIT: usize = 20;

fn bench_row(seed: i32) -> Row {
    Row {
        x: ((seed * 13) % 20_000) - 10_000,
        y: ((seed * 29) % 20_000) - 10_000,
        z: ((seed * 47) % 20_000) - 10_000,
        subject: format!("subject-{}", seed.rem_euclid(2_000)),
        object: format!("object-{}", seed.rem_euclid(700)),
        verb: match seed.rem_euclid(4) {
            0 => VERB_PLACE,
            1 => VERB_BREAK,
            2 => VERB_USE,
            _ => VERB_HURT,
        },
        time_ms: 1_700_000_000_000 + (seed as i64) * 17,
        subject_extra: format!("extra-{seed}"),
        data: vec![(seed & 0xff) as u8, ((seed * 5) & 0xff) as u8],
    }
}

fn build_rows(rows: usize) -> Vec<Row> {
    (0..rows).map(|idx| bench_row(idx as i32)).collect()
}

fn fresh_db_dir(prefix: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic from unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("space_logger_bench_{prefix}_{ts}"))
}

fn write_rows(db: &SpaceLoggerDb, rows: &[Row]) {
    let mut version = db.current_version();
    for chunk in rows.chunks(INSERT_CHUNK_SIZE) {
        version = db
            .insert_batch_with_version(chunk.to_vec(), version)
            .expect("batch insert should succeed");
    }
}

fn build_query_db(name: &str, rows: usize, flush_rows: usize) -> (PathBuf, SpaceLoggerDb) {
    let db_dir = fresh_db_dir(name);
    let db = SpaceLoggerDb::open(
        &db_dir,
        DbOptions {
            memtable_flush_rows: flush_rows,
            ..DbOptions::default()
        },
    )
    .expect("open should succeed");

    let data = build_rows(rows);
    write_rows(&db, &data);
    db.flush().expect("flush should succeed");

    (db_dir, db)
}

fn build_multi_segment_db(
    name: &str,
    rows: usize,
    rows_per_segment: usize,
) -> (PathBuf, SpaceLoggerDb) {
    let db_dir = fresh_db_dir(name);
    let db = SpaceLoggerDb::open(
        &db_dir,
        DbOptions {
            memtable_flush_rows: rows_per_segment,
            ..DbOptions::default()
        },
    )
    .expect("open should succeed");

    let data = build_rows(rows);
    let mut version = db.current_version();
    for chunk in data.chunks(rows_per_segment) {
        version = db
            .insert_batch_with_version(chunk.to_vec(), version)
            .expect("batch insert should succeed");
        db.flush().expect("flush should succeed");
    }

    (db_dir, db)
}

fn query_fixture() -> Query {
    Query {
        x: Some(IntPredicate {
            gte: Some(-9_500),
            lte: Some(9_500),
            ..IntPredicate::default()
        }),
        y: None,
        z: None,
        time_ms: Some(LongPredicate {
            gte: Some(1_700_000_100_000),
            lte: Some(1_700_020_000_000),
            ..LongPredicate::default()
        }),
        subject: None,
        object: None,
        verb_mask: verb_mask_single(VERB_PLACE),
    }
}

fn bench_query_hot_limit20(c: &mut Criterion) {
    let (db_dir, db) = build_query_db("query_hot", HOT_DATASET_ROWS, 4096);
    let query = query_fixture();

    let warmup_rows = db
        .query(&query, Some(HOT_QUERY_LIMIT))
        .expect("warmup query should succeed");

    let mut group = c.benchmark_group("query_hot_limit20");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(6));
    group.throughput(Throughput::Elements(warmup_rows.len().max(1) as u64));

    group.bench_function("xyz_time_subject_verb", |b| {
        b.iter(|| {
            let rows = db
                .query(black_box(&query), Some(HOT_QUERY_LIMIT))
                .expect("query should succeed during benchmark");
            black_box(rows.len())
        });
    });

    group.finish();
    let _ = std::fs::remove_dir_all(db_dir);
}

fn bench_query_cold_open_and_first_query(c: &mut Criterion) {
    let (db_dir, db) = build_multi_segment_db("query_cold", COLD_DATASET_ROWS, COLD_SEGMENT_ROWS);
    drop(db);

    let query = query_fixture();

    let mut group = c.benchmark_group("query_cold_open_and_first_query");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(HOT_QUERY_LIMIT as u64));

    group.bench_function("reopen_then_first_query_limit20", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let reopened = SpaceLoggerDb::open(
                    &db_dir,
                    DbOptions {
                        memtable_flush_rows: 4096,
                        ..DbOptions::default()
                    },
                )
                .expect("reopen should succeed");

                let rows = reopened
                    .query(black_box(&query), Some(HOT_QUERY_LIMIT))
                    .expect("query should succeed on reopened db");
                black_box(rows.len());
                drop(reopened);

                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
    let _ = std::fs::remove_dir_all(db_dir);
}

fn bench_query_multi_segment_fanout(c: &mut Criterion) {
    let (db_dir, db) =
        build_multi_segment_db("query_fanout", FANOUT_DATASET_ROWS, FANOUT_SEGMENT_ROWS);
    let query = query_fixture();

    let warmup_rows = db
        .query(&query, Some(HOT_QUERY_LIMIT))
        .expect("warmup query should succeed");

    let mut group = c.benchmark_group("query_multi_segment_fanout");
    group.sample_size(15);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(6));
    group.throughput(Throughput::Elements(warmup_rows.len().max(1) as u64));

    group.bench_with_input(
        BenchmarkId::new("segments", FANOUT_DATASET_ROWS / FANOUT_SEGMENT_ROWS),
        &query,
        |b, q| {
            b.iter(|| {
                let rows = db
                    .query(black_box(q), Some(HOT_QUERY_LIMIT))
                    .expect("query should succeed during benchmark");
                black_box(rows.len())
            });
        },
    );

    group.finish();
    let _ = std::fs::remove_dir_all(db_dir);
}

criterion_group!(
    benches,
    bench_query_hot_limit20,
    bench_query_cold_open_and_first_query,
    bench_query_multi_segment_fanout
);
criterion_main!(benches);
