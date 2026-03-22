use std::fs;
use std::path::PathBuf;
use std::ptr;
use std::sync::RwLock;

use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString, JValue};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jlong, jobjectArray};
use space_logger::{DbError, DbOptions, IntPredicate, LongPredicate, Query, Row, SpaceLoggerDb};

struct NativeDbHandle {
    db: RwLock<Option<SpaceLoggerDb>>,
    db_dir: PathBuf,
    memtable_flush_rows: usize,
}

fn throw_runtime(env: &mut JNIEnv, message: impl AsRef<str>) {
    let _ = env.throw_new("java/lang/RuntimeException", message.as_ref());
}

fn jstring_to_string(env: &mut JNIEnv, value: JString) -> Result<String, String> {
    env.get_string(&value)
        .map(|s| s.into())
        .map_err(|e| format!("invalid java string: {e}"))
}

fn flush_rows_from_jint(memtable_flush_rows: jint) -> usize {
    if memtable_flush_rows <= 0 {
        4096usize
    } else {
        memtable_flush_rows as usize
    }
}

fn open_db(path: &PathBuf, flush_rows: usize) -> Result<SpaceLoggerDb, String> {
    SpaceLoggerDb::open(
        path,
        DbOptions {
            memtable_flush_rows: flush_rows,
            ..DbOptions::default()
        },
    )
    .map_err(|e| format!("open db failed ({}): {e}", path.display()))
}

fn handle_from_ptr<'a>(native_ptr: jlong) -> Result<&'a NativeDbHandle, String> {
    if native_ptr == 0 {
        return Err("native bridge pointer is null (bridge already closed?)".to_string());
    }
    let raw = native_ptr as *const NativeDbHandle;
    // SAFETY: pointer originates from Box::into_raw in nativeCreate, and Java passes it back unchanged.
    unsafe {
        raw.as_ref()
            .ok_or_else(|| "invalid native bridge pointer".to_string())
    }
}

fn non_empty(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn data_head_12(data: &[u8]) -> &[u8] {
    let head_len = data.len().min(12);
    &data[..head_len]
}

fn to_java_query_row_array(env: &mut JNIEnv, rows: &[Row]) -> Result<jobjectArray, String> {
    let row_class = env
        .find_class("com/github/zly2006/sl/jni/NativeSpaceLoggerBridge$QueryRow")
        .map_err(|e| format!("find QueryRow class failed: {e}"))?;
    let array: JObjectArray = env
        .new_object_array(rows.len() as jint, &row_class, JObject::null())
        .map_err(|e| format!("create QueryRow[] failed: {e}"))?;

    for (idx, row) in rows.iter().enumerate() {
        let jsubject = env
            .new_string(&row.subject)
            .map_err(|e| format!("create subject string failed: {e}"))?;
        let jverb = env
            .new_string(&row.verb)
            .map_err(|e| format!("create verb string failed: {e}"))?;
        let jobject = env
            .new_string(&row.object)
            .map_err(|e| format!("create object string failed: {e}"))?;
        let jsubject_extra = env
            .new_string(&row.subject_extra)
            .map_err(|e| format!("create subjectExtra string failed: {e}"))?;
        let jdata_head = env
            .byte_array_from_slice(data_head_12(&row.data))
            .map_err(|e| format!("create dataHead byte[] failed: {e}"))?;

        let jsubject_obj = JObject::from(jsubject);
        let jverb_obj = JObject::from(jverb);
        let jobject_obj = JObject::from(jobject);
        let jsubject_extra_obj = JObject::from(jsubject_extra);
        let jdata_head_obj = JObject::from(jdata_head);

        let jrow = env
            .new_object(
                &row_class,
                "(JIIILjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;I[B)V",
                &[
                    JValue::Long(row.time_ms),
                    JValue::Int(row.x),
                    JValue::Int(row.y),
                    JValue::Int(row.z),
                    JValue::Object(&jsubject_obj),
                    JValue::Object(&jverb_obj),
                    JValue::Object(&jobject_obj),
                    JValue::Object(&jsubject_extra_obj),
                    JValue::Int(row.data.len().min(i32::MAX as usize) as jint),
                    JValue::Object(&jdata_head_obj),
                ],
            )
            .map_err(|e| format!("create QueryRow object failed: {e}"))?;

        env.set_object_array_element(&array, idx as jint, jrow)
            .map_err(|e| format!("set QueryRow[] element failed: {e}"))?;
    }

    Ok(array.into_raw())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_github_zly2006_sl_jni_NativeSpaceLoggerBridge_nativeCreate(
    mut env: JNIEnv,
    _class: JClass,
    db_dir: JString,
    memtable_flush_rows: jint,
) -> jlong {
    let db_dir = match jstring_to_string(&mut env, db_dir) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, e);
            return 0;
        }
    };

    let flush_rows = flush_rows_from_jint(memtable_flush_rows);
    let db_dir_path = PathBuf::from(db_dir);
    let db = match open_db(&db_dir_path, flush_rows) {
        Ok(db) => db,
        Err(e) => {
            throw_runtime(&mut env, e);
            return 0;
        }
    };

    let handle = NativeDbHandle {
        db: RwLock::new(Some(db)),
        db_dir: db_dir_path,
        memtable_flush_rows: flush_rows,
    };

    Box::into_raw(Box::new(handle)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_github_zly2006_sl_jni_NativeSpaceLoggerBridge_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    native_ptr: jlong,
) {
    if native_ptr == 0 {
        return;
    }

    let raw = native_ptr as *mut NativeDbHandle;
    // SAFETY: pointer must come from Box::into_raw in nativeCreate and is consumed exactly once.
    unsafe {
        drop(Box::from_raw(raw));
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_github_zly2006_sl_jni_NativeSpaceLoggerBridge_nativeAppend(
    mut env: JNIEnv,
    _class: JClass,
    native_ptr: jlong,
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

    let native = match handle_from_ptr(native_ptr) {
        Ok(handle) => handle,
        Err(e) => {
            throw_runtime(&mut env, e);
            return JNI_FALSE;
        }
    };

    let guard = match native.db.read() {
        Ok(lock) => lock,
        Err(_) => {
            throw_runtime(&mut env, "native db lock poisoned");
            return JNI_FALSE;
        }
    };
    let Some(db) = guard.as_ref() else {
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
    native_ptr: jlong,
) -> jint {
    let native = match handle_from_ptr(native_ptr) {
        Ok(handle) => handle,
        Err(e) => {
            throw_runtime(&mut env, e);
            return 0;
        }
    };

    let guard = match native.db.read() {
        Ok(lock) => lock,
        Err(_) => {
            throw_runtime(&mut env, "native db lock poisoned");
            return 0;
        }
    };
    let Some(db) = guard.as_ref() else {
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
    native_ptr: jlong,
    verb: JString,
) -> jint {
    let verb = match jstring_to_string(&mut env, verb) {
        Ok(v) => v,
        Err(e) => {
            throw_runtime(&mut env, e);
            return 0;
        }
    };

    let native = match handle_from_ptr(native_ptr) {
        Ok(handle) => handle,
        Err(e) => {
            throw_runtime(&mut env, e);
            return 0;
        }
    };

    let guard = match native.db.read() {
        Ok(lock) => lock,
        Err(_) => {
            throw_runtime(&mut env, "native db lock poisoned");
            return 0;
        }
    };
    let Some(db) = guard.as_ref() else {
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
    native_ptr: jlong,
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

    let native = match handle_from_ptr(native_ptr) {
        Ok(handle) => handle,
        Err(e) => {
            throw_runtime(&mut env, e);
            return ptr::null_mut();
        }
    };

    let guard = match native.db.read() {
        Ok(lock) => lock,
        Err(_) => {
            throw_runtime(&mut env, "native db lock poisoned");
            return ptr::null_mut();
        }
    };
    let Some(db) = guard.as_ref() else {
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
    drop(guard);

    match to_java_query_row_array(&mut env, &rows) {
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
    native_ptr: jlong,
) {
    let native = match handle_from_ptr(native_ptr) {
        Ok(handle) => handle,
        Err(e) => {
            throw_runtime(&mut env, e);
            return;
        }
    };

    let db_dir = native.db_dir.clone();
    let flush_rows = native.memtable_flush_rows;

    {
        let mut guard = match native.db.write() {
            Ok(lock) => lock,
            Err(_) => {
                throw_runtime(&mut env, "native db lock poisoned");
                return;
            }
        };
        *guard = None;
    }

    if db_dir.exists() {
        if let Err(e) = fs::remove_dir_all(&db_dir) {
            throw_runtime(
                &mut env,
                format!("failed to remove db dir {}: {e}", db_dir.display()),
            );
            return;
        }
    }

    let reopened = match open_db(&db_dir, flush_rows) {
        Ok(db) => db,
        Err(e) => {
            throw_runtime(&mut env, e);
            return;
        }
    };

    let mut guard = match native.db.write() {
        Ok(lock) => lock,
        Err(_) => {
            throw_runtime(&mut env, "native db lock poisoned");
            return;
        }
    };
    *guard = Some(reopened);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_github_zly2006_sl_jni_NativeSpaceLoggerBridge_nativeFlush(
    mut env: JNIEnv,
    _class: JClass,
    native_ptr: jlong,
) -> jboolean {
    let native = match handle_from_ptr(native_ptr) {
        Ok(handle) => handle,
        Err(e) => {
            throw_runtime(&mut env, e);
            return JNI_FALSE;
        }
    };

    let guard = match native.db.read() {
        Ok(lock) => lock,
        Err(_) => {
            throw_runtime(&mut env, "native db lock poisoned");
            return JNI_FALSE;
        }
    };
    let Some(db) = guard.as_ref() else {
        throw_runtime(&mut env, "native db is not initialized");
        return JNI_FALSE;
    };

    match db.flush() {
        Ok(()) => JNI_TRUE,
        Err(e) => {
            throw_runtime(&mut env, format!("flush failed: {e}"));
            JNI_FALSE
        }
    }
}
