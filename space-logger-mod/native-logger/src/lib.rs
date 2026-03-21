use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jlong};
use once_cell::sync::Lazy;
use space_logger::{DbError, DbOptions, Query, Row, SpaceLoggerDb};

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
