use std::fs;
use std::path::PathBuf;
use std::ptr;
use std::sync::Mutex;

use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jlong, jobjectArray};
use once_cell::sync::Lazy;
use space_logger::{DbError, DbOptions, IntPredicate, LongPredicate, Query, Row, SpaceLoggerDb};

#[derive(Default)]
struct NativeState {
    db: Option<SpaceLoggerDb>,
    db_dir: Option<PathBuf>,
}

static STATE: Lazy<Mutex<NativeState>> = Lazy::new(|| Mutex::new(NativeState::default()));

fn throw_runtime(env: &mut JNIEnv, message: impl AsRef<str>) {
    let _ = env.throw_new("java/lang/RuntimeException", message.as_ref());
}

fn jstring_to_string(env: &mut JNIEnv, value: JString) -> Result<String, String> {
    env.get_string(&value)
        .map(|s| s.into())
        .map_err(|e| format!("invalid java string: {e}"))
}

fn open_db(path: &str, flush_rows: usize) -> Result<SpaceLoggerDb, String> {
    SpaceLoggerDb::open(
        path,
        DbOptions {
            memtable_flush_rows: flush_rows,
        },
    )
    .map_err(|e| format!("open db failed: {e}"))
}

fn non_empty(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn query_row_to_wire(row: Row) -> String {
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        row.time_ms,
        row.x,
        row.y,
        row.z,
        row.subject,
        row.verb,
        row.object,
        row.subject_extra,
        row.data.len()
    )
}

fn to_java_string_array(env: &mut JNIEnv, lines: &[String]) -> Result<jobjectArray, String> {
    let string_class = env
        .find_class("java/lang/String")
        .map_err(|e| format!("find String class failed: {e}"))?;
    let array: JObjectArray = env
        .new_object_array(lines.len() as jint, &string_class, JObject::null())
        .map_err(|e| format!("create String[] failed: {e}"))?;

    for (idx, line) in lines.iter().enumerate() {
        let jline = env
            .new_string(line)
            .map_err(|e| format!("create String element failed: {e}"))?;
        env.set_object_array_element(&array, idx as jint, JObject::from(jline))
            .map_err(|e| format!("set String[] element failed: {e}"))?;
    }

    Ok(array.into_raw())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_github_zly2006_sl_jni_NativeSpaceLoggerBridge_nativeInit(
    mut env: JNIEnv,
    _class: JClass,
    db_dir: JString,
    memtable_flush_rows: jint,
) {
    let db_dir = match jstring_to_string(&mut env, db_dir) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, e);
            return;
        }
    };

    let flush_rows = if memtable_flush_rows <= 0 {
        4096usize
    } else {
        memtable_flush_rows as usize
    };

    let db = match open_db(&db_dir, flush_rows) {
        Ok(db) => db,
        Err(e) => {
            throw_runtime(&mut env, e);
            return;
        }
    };

    let mut state = match STATE.lock() {
        Ok(lock) => lock,
        Err(_) => {
            throw_runtime(&mut env, "native state lock poisoned");
            return;
        }
    };

    state.db = Some(db);
    state.db_dir = Some(PathBuf::from(db_dir));
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_github_zly2006_sl_jni_NativeSpaceLoggerBridge_nativeAppend(
    mut env: JNIEnv,
    _class: JClass,
    x: jint,
    y: jint,
    z: jint,
    subject: JString,
    verb: JString,
    object: JString,
    time_ms: jlong,
    subject_extra: JString,
    data: JByteArray,
) -> jboolean {
    let subject = match jstring_to_string(&mut env, subject) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, e);
            return JNI_FALSE;
        }
    };
    let verb = match jstring_to_string(&mut env, verb) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, e);
            return JNI_FALSE;
        }
    };
    let object = match jstring_to_string(&mut env, object) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, e);
            return JNI_FALSE;
        }
    };
    let subject_extra = match jstring_to_string(&mut env, subject_extra) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, e);
            return JNI_FALSE;
        }
    };
    let data = match env.convert_byte_array(&data) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, format!("invalid byte[] data: {e}"));
            return JNI_FALSE;
        }
    };

    let row = Row {
        x,
        y,
        z,
        subject,
        object,
        verb,
        time_ms,
        subject_extra,
        data,
    };

    let state = match STATE.lock() {
        Ok(lock) => lock,
        Err(_) => {
            throw_runtime(&mut env, "native state lock poisoned");
            return JNI_FALSE;
        }
    };

    let Some(db) = state.db.as_ref() else {
        throw_runtime(&mut env, "native db is not initialized");
        return JNI_FALSE;
    };

    for _ in 0..16 {
        let expected = db.current_version();
        match db.insert_with_version(row.clone(), expected) {
            Ok(_) => return JNI_TRUE,
            Err(DbError::VersionConflict { .. }) => continue,
            Err(e) => {
                throw_runtime(&mut env, format!("append failed: {e}"));
                return JNI_FALSE;
            }
        }
    }

    throw_runtime(
        &mut env,
        "append failed after retries due to version conflicts",
    );
    JNI_FALSE
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_github_zly2006_sl_jni_NativeSpaceLoggerBridge_nativeCountAll(
    mut env: JNIEnv,
    _class: JClass,
) -> jint {
    let state = match STATE.lock() {
        Ok(lock) => lock,
        Err(_) => {
            throw_runtime(&mut env, "native state lock poisoned");
            return 0;
        }
    };

    let Some(db) = state.db.as_ref() else {
        throw_runtime(&mut env, "native db is not initialized");
        return 0;
    };

    match db.query(&Query::default(), None) {
        Ok(rows) => rows.len().min(i32::MAX as usize) as jint,
        Err(e) => {
            throw_runtime(&mut env, format!("countAll failed: {e}"));
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_github_zly2006_sl_jni_NativeSpaceLoggerBridge_nativeCountByVerb(
    mut env: JNIEnv,
    _class: JClass,
    verb: JString,
) -> jint {
    let verb = match jstring_to_string(&mut env, verb) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, e);
            return 0;
        }
    };

    let state = match STATE.lock() {
        Ok(lock) => lock,
        Err(_) => {
            throw_runtime(&mut env, "native state lock poisoned");
            return 0;
        }
    };

    let Some(db) = state.db.as_ref() else {
        throw_runtime(&mut env, "native db is not initialized");
        return 0;
    };

    let query = Query {
        verb: Some(verb),
        ..Query::default()
    };

    match db.query(&query, None) {
        Ok(rows) => rows.len().min(i32::MAX as usize) as jint,
        Err(e) => {
            throw_runtime(&mut env, format!("countByVerb failed: {e}"));
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_github_zly2006_sl_jni_NativeSpaceLoggerBridge_nativeQuery(
    mut env: JNIEnv,
    _class: JClass,
    subject: JString,
    object: JString,
    verb: JString,
    min_x: jint,
    max_x: jint,
    min_y: jint,
    max_y: jint,
    min_z: jint,
    max_z: jint,
    after_time_ms: jlong,
    before_time_ms: jlong,
    limit: jint,
) -> jobjectArray {
    let subject = match jstring_to_string(&mut env, subject) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, e);
            return ptr::null_mut();
        }
    };
    let object = match jstring_to_string(&mut env, object) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, e);
            return ptr::null_mut();
        }
    };
    let verb = match jstring_to_string(&mut env, verb) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, e);
            return ptr::null_mut();
        }
    };

    let mut query = Query {
        subject: non_empty(subject),
        object: non_empty(object),
        verb: non_empty(verb),
        ..Query::default()
    };

    if min_x != i32::MIN || max_x != i32::MAX {
        query.x = Some(IntPredicate {
            gte: (min_x != i32::MIN).then_some(min_x),
            lte: (max_x != i32::MAX).then_some(max_x),
            ..IntPredicate::default()
        });
    }
    if min_y != i32::MIN || max_y != i32::MAX {
        query.y = Some(IntPredicate {
            gte: (min_y != i32::MIN).then_some(min_y),
            lte: (max_y != i32::MAX).then_some(max_y),
            ..IntPredicate::default()
        });
    }
    if min_z != i32::MIN || max_z != i32::MAX {
        query.z = Some(IntPredicate {
            gte: (min_z != i32::MIN).then_some(min_z),
            lte: (max_z != i32::MAX).then_some(max_z),
            ..IntPredicate::default()
        });
    }
    if after_time_ms != i64::MIN || before_time_ms != i64::MAX {
        query.time_ms = Some(LongPredicate {
            gte: (after_time_ms != i64::MIN).then_some(after_time_ms),
            lte: (before_time_ms != i64::MAX).then_some(before_time_ms),
            ..LongPredicate::default()
        });
    }

    let safe_limit = if limit <= 0 { 20usize } else { limit as usize };

    let state = match STATE.lock() {
        Ok(lock) => lock,
        Err(_) => {
            throw_runtime(&mut env, "native state lock poisoned");
            return ptr::null_mut();
        }
    };

    let Some(db) = state.db.as_ref() else {
        throw_runtime(&mut env, "native db is not initialized");
        return ptr::null_mut();
    };

    let rows = match db.query(&query, Some(safe_limit)) {
        Ok(rows) => rows,
        Err(e) => {
            throw_runtime(&mut env, format!("query failed: {e}"));
            return ptr::null_mut();
        }
    };
    drop(state);

    let wire_rows = rows.into_iter().map(query_row_to_wire).collect::<Vec<_>>();
    match to_java_string_array(&mut env, &wire_rows) {
        Ok(array) => array,
        Err(e) => {
            throw_runtime(&mut env, e);
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_github_zly2006_sl_jni_NativeSpaceLoggerBridge_nativeReset(
    mut env: JNIEnv,
    _class: JClass,
    db_dir: JString,
    memtable_flush_rows: jint,
) {
    let db_dir = match jstring_to_string(&mut env, db_dir) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, e);
            return;
        }
    };
    let flush_rows = if memtable_flush_rows <= 0 {
        4096usize
    } else {
        memtable_flush_rows as usize
    };

    {
        let mut state = match STATE.lock() {
            Ok(lock) => lock,
            Err(_) => {
                throw_runtime(&mut env, "native state lock poisoned");
                return;
            }
        };
        state.db = None;
        state.db_dir = None;
    }

    let db_dir_path = PathBuf::from(&db_dir);
    if db_dir_path.exists() {
        if let Err(e) = fs::remove_dir_all(&db_dir_path) {
            throw_runtime(&mut env, format!("failed to remove db dir: {e}"));
            return;
        }
    }

    let db = match open_db(&db_dir, flush_rows) {
        Ok(db) => db,
        Err(e) => {
            throw_runtime(&mut env, e);
            return;
        }
    };

    let mut state = match STATE.lock() {
        Ok(lock) => lock,
        Err(_) => {
            throw_runtime(&mut env, "native state lock poisoned");
            return;
        }
    };
    state.db = Some(db);
    state.db_dir = Some(db_dir_path);
}
