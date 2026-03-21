use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use space_logger::{DbOptions, IntPredicate, LongPredicate, Query, Row, SpaceLoggerDb};

const FLUSH_ROWS: usize = 4096;
const WRITE_BATCH_ROWS: [usize; 2] = [10_000, 50_000];
const QUERY_DATASET_ROWS: usize = 2_000_000;
const INSERT_CHUNK_SIZE: usize = 500;

fn bench_row(seed: i32) -> Row {
    Row {
        x: seed % 10_000,
        y: (seed * 3) % 10_000,
        z: (seed * 7) % 10_000,
        subject: format!("subject-{}", seed % 500),
        object: format!("object-{}", seed % 200),
        verb: if seed % 2 == 0 {
            "click".to_string()
        } else {
            "view".to_string()
        },
        time_ms: 1_700_000_000_000 + (seed as i64) * 10,
        subject_extra: format!("extra-{seed}"),
        data: vec![(seed & 0xff) as u8, ((seed + 1) & 0xff) as u8],
    }
}

fn fresh_db_dir(prefix: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic from unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("space_logger_bench_{prefix}_{ts}"))
}

fn build_rows(rows: usize) -> Vec<Row> {
    (0..rows).map(|idx| bench_row(idx as i32)).collect()
}

fn write_rows(db: &SpaceLoggerDb, rows: &[Row]) {
    let mut version = db.current_version();
    for chunk in rows.chunks(INSERT_CHUNK_SIZE) {
        version = db
            .insert_batch_with_version(chunk.to_vec(), version)
            .expect("batch insert should succeed");
    }
}

fn bench_write_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_ingest");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(4));

    for rows in WRITE_BATCH_ROWS {
        let data = build_rows(rows);
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for iter in 0..iters {
                    let db_dir = fresh_db_dir(&format!("write_{rows}_{iter}"));
                    let db = SpaceLoggerDb::open(
                        &db_dir,
                        DbOptions {
                            memtable_flush_rows: FLUSH_ROWS,
                        },
                    )
                    .expect("open should succeed");

                    let start = Instant::now();
                    write_rows(&db, &data);
                    db.flush().expect("flush should succeed");
                    total += start.elapsed();

                    let _ = std::fs::remove_dir_all(db_dir);
                }
                total
            });
        });
    }

    group.finish();
}

fn build_query_db(rows: usize) -> (PathBuf, SpaceLoggerDb) {
    let db_dir = fresh_db_dir("query_dataset");
    let db = SpaceLoggerDb::open(
        &db_dir,
        DbOptions {
            memtable_flush_rows: FLUSH_ROWS,
        },
    )
    .expect("open should succeed");

    let data = build_rows(rows);
    write_rows(&db, &data);
    db.flush().expect("flush should succeed");

    (db_dir, db)
}

fn bench_range_query(c: &mut Criterion) {
    let (db_dir, db) = build_query_db(QUERY_DATASET_ROWS);

    let query = Query {
        x: Some(IntPredicate {
            gte: Some(1200),
            lte: Some(3200),
            ..IntPredicate::default()
        }),
        y: Some(IntPredicate {
            gte: Some(800),
            lte: Some(6000),
            ..IntPredicate::default()
        }),
        z: Some(IntPredicate {
            gte: Some(2000),
            lte: Some(7200),
            ..IntPredicate::default()
        }),
        time_ms: Some(LongPredicate {
            gte: Some(1_700_000_500_000),
            lte: Some(1_700_001_200_000),
            ..LongPredicate::default()
        }),
        subject: Some("subject-42".to_string()),
        object: None,
        verb: Some("click".to_string()),
    };

    let warmup_hits = db.query(&query).expect("warmup query should succeed").len() as u64;

    let mut group = c.benchmark_group("range_query");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(6));
    group.throughput(Throughput::Elements(warmup_hits.max(1)));

    group.bench_function("xyz_time_subject_verb", |b| {
        b.iter(|| {
            let rows = db
                .query(black_box(&query))
                .expect("query should succeed during benchmark");
            black_box(rows.len())
        });
    });

    group.finish();
    let _ = std::fs::remove_dir_all(db_dir);
}

criterion_group!(benches, bench_write_ingest, bench_range_query);
criterion_main!(benches);
