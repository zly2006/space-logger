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

fn no_match_query() -> Query {
    query_by_subject("subject-does-not-exist")
}

fn varied_row(seed: i32) -> Row {
    let subject = format!("subject-{}", seed % 17);
    let object = format!("object-{}", seed % 11);
    let verb = match seed % 3 {
        0 => "click",
        1 => "view",
        _ => "purchase",
    }
    .to_string();

    Row {
        x: (seed * 37) % 2000 - 1000,
        y: (seed * 19) % 2000 - 1000,
        z: (seed * 11) % 2000 - 1000,
        subject,
        object,
        verb,
        time_ms: 1_700_000_000_000 + (seed as i64) * 13,
        subject_extra: format!("extra-{seed}"),
        data: vec![(seed & 0xff) as u8, ((seed * 3) & 0xff) as u8],
    }
}

fn matches_int_predicate(value: i32, predicate: &Option<IntPredicate>) -> bool {
    if let Some(pred) = predicate {
        if let Some(eq) = pred.eq {
            if value != eq {
                return false;
            }
        }
        if let Some(gt) = pred.gt {
            if value <= gt {
                return false;
            }
        }
        if let Some(gte) = pred.gte {
            if value < gte {
                return false;
            }
        }
        if let Some(lt) = pred.lt {
            if value >= lt {
                return false;
            }
        }
        if let Some(lte) = pred.lte {
            if value > lte {
                return false;
            }
        }
    }
    true
}

fn matches_long_predicate(value: i64, predicate: &Option<LongPredicate>) -> bool {
    if let Some(pred) = predicate {
        if let Some(eq) = pred.eq {
            if value != eq {
                return false;
            }
        }
        if let Some(gt) = pred.gt {
            if value <= gt {
                return false;
            }
        }
        if let Some(gte) = pred.gte {
            if value < gte {
                return false;
            }
        }
        if let Some(lt) = pred.lt {
            if value >= lt {
                return false;
            }
        }
        if let Some(lte) = pred.lte {
            if value > lte {
                return false;
            }
        }
    }
    true
}

fn row_matches_query(row: &Row, query: &Query) -> bool {
    matches_int_predicate(row.x, &query.x)
        && matches_int_predicate(row.y, &query.y)
        && matches_int_predicate(row.z, &query.z)
        && matches_long_predicate(row.time_ms, &query.time_ms)
        && query
            .subject
            .as_ref()
            .is_none_or(|subject| row.subject == *subject)
        && query
            .object
            .as_ref()
            .is_none_or(|object| row.object == *object)
        && query.verb.as_ref().is_none_or(|verb| row.verb == *verb)
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
            .query(&query_by_subject(&row.subject), None)
            .expect("query works");
        assert_eq!(rows.len(), 1);

        let no_rows = db.query(&no_match_query(), None).expect("query works");
        assert_eq!(no_rows.len(), 0, "no match query should return empty");
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
            .query(&query_by_subject("subject-42"), None)
            .expect("query after recover works");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].subject_extra, "extra-42");
        assert_eq!(rows[0].data, vec![42, 43]);

        let no_rows = db.query(&no_match_query(), None).expect("query works");
        assert_eq!(no_rows.len(), 0, "no match query should return empty");
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
        .query(&query_by_subject("subject-2"), None)
        .expect("query should scan segments");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].object, "object-2");

    let no_rows = db
        .query(&no_match_query(), None)
        .expect("query should work");
    assert_eq!(no_rows.len(), 0, "no match query should return empty");

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

    let ok_rows = db
        .query(&query_by_subject("subject-10"), None)
        .expect("query should work");
    assert_eq!(ok_rows.len(), 1);

    let no_rows = db
        .query(&no_match_query(), None)
        .expect("query should work");
    assert_eq!(no_rows.len(), 0, "no match query should return empty");

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

    let rows = db.query(&query, None).expect("range query should pass");
    let subjects = rows.into_iter().map(|r| r.subject).collect::<BTreeSet<_>>();

    let expected = BTreeSet::from([
        "subject-3".to_string(),
        "subject-4".to_string(),
        "subject-5".to_string(),
    ]);
    assert_eq!(subjects, expected);

    let fail_query = Query {
        x: Some(IntPredicate {
            gt: Some(1000),
            ..IntPredicate::default()
        }),
        ..Query::default()
    };
    let no_rows = db
        .query(&fail_query, None)
        .expect("range query should pass");
    assert_eq!(no_rows.len(), 0, "out-of-range query should return empty");

    std::fs::remove_dir_all(db_dir).ok();
}

#[test]
fn query_returns_latest_first_and_respects_limit() {
    let db_dir = fresh_db_dir("query_returns_latest_first_and_respects_limit");

    let db = SpaceLoggerDb::open(
        &db_dir,
        DbOptions {
            memtable_flush_rows: 1024,
        },
    )
    .expect("open should succeed");

    for seed in 0..6 {
        let version = db.current_version();
        db.insert_with_version(sample_row(seed), version)
            .expect("insert should succeed");
    }

    let all_rows = db
        .query(&Query::default(), None)
        .expect("query should succeed");
    let all_subjects = all_rows
        .into_iter()
        .map(|row| row.subject)
        .collect::<Vec<_>>();
    assert_eq!(
        all_subjects,
        vec![
            "subject-5".to_string(),
            "subject-4".to_string(),
            "subject-3".to_string(),
            "subject-2".to_string(),
            "subject-1".to_string(),
            "subject-0".to_string(),
        ],
        "default query should return newest rows first"
    );

    let limited_rows = db
        .query(&Query::default(), Some(3))
        .expect("query with limit should succeed");
    let limited_subjects = limited_rows
        .into_iter()
        .map(|row| row.subject)
        .collect::<Vec<_>>();
    assert_eq!(
        limited_subjects,
        vec![
            "subject-5".to_string(),
            "subject-4".to_string(),
            "subject-3".to_string(),
        ],
        "limit should truncate result set from newest to oldest"
    );

    let zero_rows = db
        .query(&Query::default(), Some(0))
        .expect("limit zero should succeed");
    assert_eq!(zero_rows.len(), 0, "limit zero should return empty");

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
            .query(&query_by_subject("subject-88"), None)
            .expect("query should work");
        assert_eq!(rows.len(), 1);

        let no_rows = db
            .query(&no_match_query(), None)
            .expect("query should work");
        assert_eq!(no_rows.len(), 0, "no match query should return empty");
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
            .query(&query_by_subject("subject-88"), None)
            .expect("query after reopen should work");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].object, "object-88");

        let no_rows = reopened
            .query(&no_match_query(), None)
            .expect("query should work");
        assert_eq!(no_rows.len(), 0, "no match query should return empty");
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
    let matched = db.query(&query, None).expect("range query should work");
    assert_eq!(matched.len(), 3);

    let no_rows = db
        .query(&no_match_query(), None)
        .expect("query should work");
    assert_eq!(no_rows.len(), 0, "no match query should return empty");

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
    let rows = db.query(&query, None).expect("query should still work");
    assert_eq!(rows.len(), 3, "all records must remain after compaction");

    let no_rows = db
        .query(&no_match_query(), None)
        .expect("query should work");
    assert_eq!(no_rows.len(), 0, "no match query should return empty");

    std::fs::remove_dir_all(db_dir).ok();
}

#[test]
fn delete_where_removes_rows_and_returns_zero_for_non_matching_delete() {
    let db_dir = fresh_db_dir("delete_where_removes_rows_and_returns_zero_for_non_matching_delete");

    let db = SpaceLoggerDb::open(
        &db_dir,
        DbOptions {
            memtable_flush_rows: 8,
        },
    )
    .expect("open should succeed");

    for seed in 0..20 {
        let version = db.current_version();
        db.insert_with_version(sample_row(seed), version)
            .expect("insert should succeed");
    }
    db.flush().expect("flush should succeed");

    let target_query = query_by_subject("subject-5");
    let before = db.query(&target_query, None).expect("query should work");
    assert_eq!(
        before.len(),
        1,
        "success case should have one row before delete"
    );

    let deleted = db
        .delete_where(&target_query)
        .expect("delete should succeed for matched rows");
    assert_eq!(deleted, 1, "should delete one matched row");

    let after = db.query(&target_query, None).expect("query should work");
    assert_eq!(after.len(), 0, "deleted rows should not be queryable");

    let survivor = db
        .query(&query_by_subject("subject-6"), None)
        .expect("query should work");
    assert_eq!(survivor.len(), 1, "unmatched rows should remain");

    let deleted_none = db
        .delete_where(&no_match_query())
        .expect("delete should succeed for non-matching query");
    assert_eq!(deleted_none, 0, "non-matching delete should return zero");

    std::fs::remove_dir_all(db_dir).ok();
}

#[test]
fn filter_logic_matches_reference_implementation_with_500_plus_rows() {
    let db_dir = fresh_db_dir("filter_logic_matches_reference_implementation_with_500_plus_rows");

    let db = SpaceLoggerDb::open(
        &db_dir,
        DbOptions {
            memtable_flush_rows: 64,
        },
    )
    .expect("open should succeed");

    let dataset = (0..700).map(varied_row).collect::<Vec<_>>();
    let mut version = db.current_version();
    for chunk in dataset.chunks(50) {
        version = db
            .insert_batch_with_version(chunk.to_vec(), version)
            .expect("batch insert should succeed");
    }
    db.flush().expect("flush should succeed");

    let queries = vec![
        Query {
            subject: Some("subject-3".to_string()),
            ..Query::default()
        },
        Query {
            object: Some("object-7".to_string()),
            verb: Some("click".to_string()),
            ..Query::default()
        },
        Query {
            x: Some(IntPredicate {
                gte: Some(-250),
                lte: Some(250),
                ..IntPredicate::default()
            }),
            y: Some(IntPredicate {
                gt: Some(-400),
                lt: Some(600),
                ..IntPredicate::default()
            }),
            z: Some(IntPredicate {
                gte: Some(-100),
                lte: Some(900),
                ..IntPredicate::default()
            }),
            ..Query::default()
        },
        Query {
            time_ms: Some(LongPredicate {
                gte: Some(1_700_000_001_000),
                lte: Some(1_700_000_004_000),
                ..LongPredicate::default()
            }),
            verb: Some("view".to_string()),
            ..Query::default()
        },
        Query {
            x: Some(IntPredicate {
                eq: Some(999_999),
                ..IntPredicate::default()
            }),
            ..Query::default()
        },
    ];

    for query in &queries {
        let mut expected = dataset
            .iter()
            .filter(|row| row_matches_query(row, query))
            .cloned()
            .collect::<Vec<_>>();
        expected.reverse();
        let actual = db.query(query, None).expect("query should work");
        assert_eq!(
            actual, expected,
            "database filter result should match reference implementation"
        );
    }

    assert!(
        !db.query(&queries[0], None)
            .expect("query should work")
            .is_empty(),
        "first query should have success case"
    );
    assert_eq!(
        db.query(queries.last().expect("query exists"), None)
            .expect("query should work")
            .len(),
        0,
        "last query should be failure case with no matched rows"
    );

    std::fs::remove_dir_all(db_dir).ok();
}
