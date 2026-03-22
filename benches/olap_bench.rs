use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use space_logger::{DbOptions, Row, SpaceLoggerDb, VERB_PLACE, VERB_USE};

const FLUSH_ROWS: usize = 4096;
const WRITE_BATCH_ROWS: [usize; 2] = [10_000, 50_000];
const INSERT_CHUNK_SIZE: usize = 500;

fn bench_row(seed: i32) -> Row {
    Row {
        x: seed % 10_000,
        y: (seed * 3) % 10_000,
        z: (seed * 7) % 10_000,
        subject: format!("subject-{}", seed % 500),
        object: format!("object-{}", seed % 200),
        verb: if seed % 2 == 0 { VERB_PLACE } else { VERB_USE },
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
                            ..DbOptions::default()
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

criterion_group!(benches, bench_write_ingest);
criterion_main!(benches);
