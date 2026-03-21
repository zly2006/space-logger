use std::collections::BTreeSet;
use std::path::PathBuf;

use space_logger::{DbOptions, IntPredicate, LongPredicate, Query, Row, SpaceLoggerDb};

fn sample_row(seed: i32) -> Row {
    Row {
        x: seed,
        y: seed + 1,
        z: seed + 2,
        subject: format!("subject-{seed}"),
        object: format!("object-{seed}"),
        verb: "click".to_string(),
        time_ms: 1_700_000_000_000 + seed as i64,
        subject_extra: format!("extra-{seed}"),
        data: vec![seed as u8, (seed + 1) as u8],
    }
}

fn query_by_subject(subject: &str) -> Query {
    Query {
        subject: Some(subject.to_string()),
        ..Query::default()
    }
}

fn fresh_db_dir(name: &str) -> PathBuf {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("space_logger_{name}_{ts}"))
}

#[test]
fn wal_recover_after_reopen() {
    let db_dir = fresh_db_dir("wal_recover_after_reopen");

    {
        let db = SpaceLoggerDb::open(
            &db_dir,
            DbOptions {
                memtable_flush_rows: 1024,
            },
        )
        .expect("open should succeed");
        let row = sample_row(42);
        let version = db.current_version();
        db.insert_with_version(row.clone(), version)
            .expect("insert should succeed");

        let rows = db
            .query(&query_by_subject(&row.subject))
            .expect("query works");
        assert_eq!(rows.len(), 1);
    }

    {
        let db = SpaceLoggerDb::open(
            &db_dir,
            DbOptions {
                memtable_flush_rows: 1024,
            },
        )
        .expect("reopen should recover");
        let rows = db
            .query(&query_by_subject("subject-42"))
            .expect("query after recover works");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].subject_extra, "extra-42");
        assert_eq!(rows[0].data, vec![42, 43]);
    }

    std::fs::remove_dir_all(db_dir).ok();
}

#[test]
fn lsm_flush_creates_segment_and_query_hits_persisted_data() {
    let db_dir = fresh_db_dir("lsm_flush_creates_segment_and_query_hits_persisted_data");

    let db = SpaceLoggerDb::open(
        &db_dir,
        DbOptions {
            memtable_flush_rows: 2,
        },
    )
    .expect("open should succeed");

    let version = db.current_version();
    db.insert_with_version(sample_row(1), version)
        .expect("first insert");

    let version = db.current_version();
    db.insert_with_version(sample_row(2), version)
        .expect("second insert triggers flush");

    let segment_dir = db_dir.join("segments");
    let entries = std::fs::read_dir(&segment_dir)
        .expect("segment dir exists")
        .collect::<Result<Vec<_>, _>>()
        .expect("segment dir readable");
    assert!(!entries.is_empty(), "expected at least one segment file");

    let rows = db
        .query(&query_by_subject("subject-2"))
        .expect("query should scan segments");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].object, "object-2");

    std::fs::remove_dir_all(db_dir).ok();
}

#[test]
fn optimistic_lock_rejects_stale_write() {
    let db_dir = fresh_db_dir("optimistic_lock_rejects_stale_write");

    let db = SpaceLoggerDb::open(
        &db_dir,
        DbOptions {
            memtable_flush_rows: 1024,
        },
    )
    .expect("open should succeed");

    let v0 = db.current_version();
    db.insert_with_version(sample_row(10), v0)
        .expect("first write should pass");

    let stale = db.insert_with_version(sample_row(11), v0);
    assert!(stale.is_err(), "stale expected_version should fail");

    std::fs::remove_dir_all(db_dir).ok();
}

#[test]
fn range_query_uses_xyz_and_time_filters() {
    let db_dir = fresh_db_dir("range_query_uses_xyz_and_time_filters");

    let db = SpaceLoggerDb::open(
        &db_dir,
        DbOptions {
            memtable_flush_rows: 3,
        },
    )
    .expect("open should succeed");

    for seed in 0..8 {
        let version = db.current_version();
        db.insert_with_version(sample_row(seed), version)
            .expect("insert should succeed");
    }

    let query = Query {
        x: Some(IntPredicate {
            gt: Some(1),
            lt: Some(6),
            ..IntPredicate::default()
        }),
        y: Some(IntPredicate {
            gte: Some(4),
            lte: Some(7),
            ..IntPredicate::default()
        }),
        z: Some(IntPredicate {
            gte: Some(5),
            lte: Some(8),
            ..IntPredicate::default()
        }),
        time_ms: Some(LongPredicate {
            gte: Some(1_700_000_000_003),
            lte: Some(1_700_000_000_006),
            ..LongPredicate::default()
        }),
        ..Query::default()
    };

    let rows = db.query(&query).expect("range query should pass");
    let subjects = rows.into_iter().map(|r| r.subject).collect::<BTreeSet<_>>();

    let expected = BTreeSet::from([
        "subject-3".to_string(),
        "subject-4".to_string(),
        "subject-5".to_string(),
    ]);
    assert_eq!(subjects, expected);

    std::fs::remove_dir_all(db_dir).ok();
}

#[test]
fn flush_truncates_wal_and_reopen_can_read_segment_data() {
    let db_dir = fresh_db_dir("flush_truncates_wal_and_reopen_can_read_segment_data");

    {
        let db = SpaceLoggerDb::open(
            &db_dir,
            DbOptions {
                memtable_flush_rows: 1024,
            },
        )
        .expect("open should succeed");

        let row = sample_row(88);
        let version = db.current_version();
        db.insert_with_version(row.clone(), version)
            .expect("insert should succeed");

        let wal_path = db.wal_path().expect("wal path should be readable");
        let wal_len_before = std::fs::metadata(&wal_path)
            .expect("wal metadata should exist")
            .len();
        assert!(
            wal_len_before > 0,
            "wal should contain at least one frame before flush"
        );

        db.flush().expect("flush should succeed");

        let wal_len_after = std::fs::metadata(&wal_path)
            .expect("wal metadata should exist")
            .len();
        assert_eq!(wal_len_after, 0, "wal should be truncated after flush");

        let rows = db
            .query(&query_by_subject("subject-88"))
            .expect("query should work");
        assert_eq!(rows.len(), 1);
    }

    {
        let reopened = SpaceLoggerDb::open(
            &db_dir,
            DbOptions {
                memtable_flush_rows: 1024,
            },
        )
        .expect("reopen should succeed");

        let rows = reopened
            .query(&query_by_subject("subject-88"))
            .expect("query after reopen should work");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].object, "object-88");
    }

    std::fs::remove_dir_all(db_dir).ok();
}

#[test]
fn batch_insert_appends_multiple_rows_and_enforces_optimistic_lock() {
    let db_dir = fresh_db_dir("batch_insert_appends_multiple_rows_and_enforces_optimistic_lock");

    let db = SpaceLoggerDb::open(
        &db_dir,
        DbOptions {
            memtable_flush_rows: 1024,
        },
    )
    .expect("open should succeed");

    let rows = vec![sample_row(20), sample_row(21), sample_row(22)];
    let v0 = db.current_version();
    let new_version = db
        .insert_batch_with_version(rows.clone(), v0)
        .expect("batch insert should succeed");
    assert_eq!(new_version, v0 + rows.len() as u64);

    let query = Query {
        x: Some(IntPredicate {
            gte: Some(20),
            lte: Some(22),
            ..IntPredicate::default()
        }),
        ..Query::default()
    };
    let matched = db.query(&query).expect("range query should work");
    assert_eq!(matched.len(), 3);

    let stale_write = db.insert_batch_with_version(vec![sample_row(23)], v0);
    assert!(
        stale_write.is_err(),
        "stale expected_version should fail for batch insert"
    );

    std::fs::remove_dir_all(db_dir).ok();
}

#[test]
fn manual_compaction_merges_segments_and_preserves_query_results() {
    let db_dir = fresh_db_dir("manual_compaction_merges_segments_and_preserves_query_results");

    let db = SpaceLoggerDb::open(
        &db_dir,
        DbOptions {
            memtable_flush_rows: 1,
        },
    )
    .expect("open should succeed");

    for seed in [31, 32, 33] {
        let version = db.current_version();
        db.insert_with_version(sample_row(seed), version)
            .expect("insert should succeed");
    }

    let segments_before = std::fs::read_dir(db_dir.join("segments"))
        .expect("segment dir should exist")
        .collect::<Result<Vec<_>, _>>()
        .expect("segment dir should be readable")
        .len();
    assert!(
        segments_before >= 3,
        "flush_rows=1 should generate multiple segments"
    );

    db.compact().expect("compaction should succeed");

    let segments_after = std::fs::read_dir(db_dir.join("segments"))
        .expect("segment dir should exist")
        .collect::<Result<Vec<_>, _>>()
        .expect("segment dir should be readable")
        .len();
    assert_eq!(
        segments_after, 1,
        "compaction should merge into one segment"
    );

    let query = Query {
        x: Some(IntPredicate {
            gte: Some(31),
            lte: Some(33),
            ..IntPredicate::default()
        }),
        ..Query::default()
    };
    let rows = db.query(&query).expect("query should still work");
    assert_eq!(rows.len(), 3, "all records must remain after compaction");

    std::fs::remove_dir_all(db_dir).ok();
}
