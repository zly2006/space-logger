use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Row {
    pub x: i32,
    pub y: i32,
    pub z: i32,
    pub subject: String,
    pub object: String,
    pub verb: String,
    pub time_ms: i64,
    pub subject_extra: String,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct IntPredicate {
    pub eq: Option<i32>,
    pub gt: Option<i32>,
    pub gte: Option<i32>,
    pub lt: Option<i32>,
    pub lte: Option<i32>,
}

impl IntPredicate {
    fn matches(&self, value: i32) -> bool {
        if let Some(eq) = self.eq {
            if value != eq {
                return false;
            }
        }
        if let Some(gt) = self.gt {
            if value <= gt {
                return false;
            }
        }
        if let Some(gte) = self.gte {
            if value < gte {
                return false;
            }
        }
        if let Some(lt) = self.lt {
            if value >= lt {
                return false;
            }
        }
        if let Some(lte) = self.lte {
            if value > lte {
                return false;
            }
        }
        true
    }

    fn is_effective(&self) -> bool {
        self.eq.is_some()
            || self.gt.is_some()
            || self.gte.is_some()
            || self.lt.is_some()
            || self.lte.is_some()
    }
}

#[derive(Clone, Debug, Default)]
pub struct LongPredicate {
    pub eq: Option<i64>,
    pub gt: Option<i64>,
    pub gte: Option<i64>,
    pub lt: Option<i64>,
    pub lte: Option<i64>,
}

impl LongPredicate {
    fn matches(&self, value: i64) -> bool {
        if let Some(eq) = self.eq {
            if value != eq {
                return false;
            }
        }
        if let Some(gt) = self.gt {
            if value <= gt {
                return false;
            }
        }
        if let Some(gte) = self.gte {
            if value < gte {
                return false;
            }
        }
        if let Some(lt) = self.lt {
            if value >= lt {
                return false;
            }
        }
        if let Some(lte) = self.lte {
            if value > lte {
                return false;
            }
        }
        true
    }

    fn is_effective(&self) -> bool {
        self.eq.is_some()
            || self.gt.is_some()
            || self.gte.is_some()
            || self.lt.is_some()
            || self.lte.is_some()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Query {
    pub x: Option<IntPredicate>,
    pub y: Option<IntPredicate>,
    pub z: Option<IntPredicate>,
    pub subject: Option<String>,
    pub object: Option<String>,
    pub verb: Option<String>,
    pub time_ms: Option<LongPredicate>,
}

#[derive(Clone, Debug)]
pub struct DbOptions {
    pub memtable_flush_rows: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            memtable_flush_rows: 4096,
        }
    }
}

#[derive(Debug)]
pub enum DbError {
    Io(std::io::Error),
    Encode(bincode::Error),
    VersionConflict { expected: u64, actual: u64 },
    PoisonedLock,
    CorruptedWal(String),
}

impl Display for DbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Encode(err) => write!(f, "serialization error: {err}"),
            Self::VersionConflict { expected, actual } => {
                write!(
                    f,
                    "optimistic lock conflict: expected version {expected}, actual version {actual}"
                )
            }
            Self::PoisonedLock => write!(f, "lock poisoned"),
            Self::CorruptedWal(message) => write!(f, "corrupted wal: {message}"),
        }
    }
}

impl std::error::Error for DbError {}

impl From<std::io::Error> for DbError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<bincode::Error> for DbError {
    fn from(value: bincode::Error) -> Self {
        Self::Encode(value)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct ColumnStore {
    seq: Vec<u64>,
    x: Vec<i32>,
    y: Vec<i32>,
    z: Vec<i32>,
    subject: Vec<String>,
    object: Vec<String>,
    verb: Vec<String>,
    time_ms: Vec<i64>,
    subject_extra: Vec<String>,
    data: Vec<Vec<u8>>,
}

impl ColumnStore {
    fn len(&self) -> usize {
        self.seq.len()
    }

    fn is_empty(&self) -> bool {
        self.seq.is_empty()
    }

    fn push(&mut self, seq: u64, row: &Row) {
        self.seq.push(seq);
        self.x.push(row.x);
        self.y.push(row.y);
        self.z.push(row.z);
        self.subject.push(row.subject.clone());
        self.object.push(row.object.clone());
        self.verb.push(row.verb.clone());
        self.time_ms.push(row.time_ms);
        self.subject_extra.push(row.subject_extra.clone());
        self.data.push(row.data.clone());
    }

    fn row_at(&self, row_id: usize) -> Row {
        Row {
            x: self.x[row_id],
            y: self.y[row_id],
            z: self.z[row_id],
            subject: self.subject[row_id].clone(),
            object: self.object[row_id].clone(),
            verb: self.verb[row_id].clone(),
            time_ms: self.time_ms[row_id],
            subject_extra: self.subject_extra[row_id].clone(),
            data: self.data[row_id].clone(),
        }
    }

    fn max_seq(&self) -> Option<u64> {
        self.seq.iter().copied().max()
    }
}

#[derive(Clone, Debug, Default)]
struct MemTable {
    columns: ColumnStore,
    subject_index: HashMap<String, Vec<usize>>,
    object_index: HashMap<String, Vec<usize>>,
    verb_index: HashMap<String, Vec<usize>>,
}

impl MemTable {
    fn insert(&mut self, seq: u64, row: &Row) {
        let row_id = self.columns.len();
        self.subject_index
            .entry(row.subject.clone())
            .or_default()
            .push(row_id);
        self.object_index
            .entry(row.object.clone())
            .or_default()
            .push(row_id);
        self.verb_index
            .entry(row.verb.clone())
            .or_default()
            .push(row_id);
        self.columns.push(seq, row);
    }

    fn len(&self) -> usize {
        self.columns.len()
    }

    fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    fn clear(&mut self) {
        *self = Self::default();
    }

    fn to_column_store(&self) -> ColumnStore {
        self.columns.clone()
    }

    fn query(&self, query: &Query) -> Vec<(u64, Row)> {
        let mut candidate = initial_candidates_from_string_filters(
            self.columns.len(),
            query,
            &self.subject_index,
            &self.object_index,
            &self.verb_index,
        );

        if has_xyz_filter(query) {
            let filtered = candidate
                .take()
                .unwrap_or_else(|| (0..self.columns.len()).collect())
                .into_iter()
                .filter(|row_id| {
                    let row_id = *row_id;
                    matches_int(self.columns.x[row_id], query.x.as_ref())
                        && matches_int(self.columns.y[row_id], query.y.as_ref())
                        && matches_int(self.columns.z[row_id], query.z.as_ref())
                })
                .collect::<Vec<_>>();
            candidate = Some(filtered);
        }

        if query
            .time_ms
            .as_ref()
            .is_some_and(LongPredicate::is_effective)
        {
            let filtered = candidate
                .take()
                .unwrap_or_else(|| (0..self.columns.len()).collect())
                .into_iter()
                .filter(|row_id| {
                    let row_id = *row_id;
                    matches_long(self.columns.time_ms[row_id], query.time_ms.as_ref())
                })
                .collect::<Vec<_>>();
            candidate = Some(filtered);
        }

        let row_ids = candidate.unwrap_or_else(|| (0..self.columns.len()).collect());
        materialize_matches(&self.columns, query, row_ids)
    }
}

#[derive(Clone, Debug)]
struct Segment {
    id: u64,
    path: PathBuf,
    columns: ColumnStore,
    index: SegmentIndex,
}

impl Segment {
    fn from_columns(id: u64, path: PathBuf, columns: ColumnStore) -> Self {
        let index = SegmentIndex::build(&columns);
        Self {
            id,
            path,
            columns,
            index,
        }
    }

    fn load(id: u64, path: PathBuf) -> Result<Self, DbError> {
        let bytes = fs::read(&path)?;
        let columns: ColumnStore = bincode::deserialize(&bytes)?;
        Ok(Self::from_columns(id, path, columns))
    }

    fn max_seq(&self) -> Option<u64> {
        self.columns.max_seq()
    }

    fn query(&self, query: &Query) -> Vec<(u64, Row)> {
        let mut candidate = initial_candidates_from_string_filters(
            self.columns.len(),
            query,
            &self.index.subject_index,
            &self.index.object_index,
            &self.index.verb_index,
        );

        if has_xyz_filter(query) {
            let morton_ids = self.index.morton_candidates(query, &self.columns);
            candidate = Some(intersect_sorted_vecs(
                candidate.unwrap_or_else(|| (0..self.columns.len()).collect()),
                morton_ids,
            ));
        }

        if let Some(time_predicate) = query
            .time_ms
            .as_ref()
            .filter(|predicate| predicate.is_effective())
        {
            let time_ids = self.index.time_candidates(time_predicate);
            candidate = Some(intersect_sorted_vecs(
                candidate.unwrap_or_else(|| (0..self.columns.len()).collect()),
                time_ids,
            ));
        }

        let row_ids = candidate.unwrap_or_else(|| (0..self.columns.len()).collect());
        materialize_matches(&self.columns, query, row_ids)
    }
}

#[derive(Clone, Debug, Default)]
struct SegmentIndex {
    subject_index: HashMap<String, Vec<usize>>,
    object_index: HashMap<String, Vec<usize>>,
    verb_index: HashMap<String, Vec<usize>>,
    morton_sorted: Vec<(u128, usize)>,
    time_sorted: Vec<(i64, usize)>,
}

impl SegmentIndex {
    fn build(columns: &ColumnStore) -> Self {
        let mut subject_index: HashMap<String, Vec<usize>> = HashMap::new();
        let mut object_index: HashMap<String, Vec<usize>> = HashMap::new();
        let mut verb_index: HashMap<String, Vec<usize>> = HashMap::new();
        let mut morton_sorted = Vec::with_capacity(columns.len());
        let mut time_sorted = Vec::with_capacity(columns.len());

        for row_id in 0..columns.len() {
            subject_index
                .entry(columns.subject[row_id].clone())
                .or_default()
                .push(row_id);
            object_index
                .entry(columns.object[row_id].clone())
                .or_default()
                .push(row_id);
            verb_index
                .entry(columns.verb[row_id].clone())
                .or_default()
                .push(row_id);
            let morton = morton_encode_i32(columns.x[row_id], columns.y[row_id], columns.z[row_id]);
            morton_sorted.push((morton, row_id));
            time_sorted.push((columns.time_ms[row_id], row_id));
        }

        morton_sorted.sort_unstable_by_key(|(code, _)| *code);
        time_sorted.sort_unstable_by_key(|(time, _)| *time);

        Self {
            subject_index,
            object_index,
            verb_index,
            morton_sorted,
            time_sorted,
        }
    }

    fn morton_candidates(&self, query: &Query, columns: &ColumnStore) -> Vec<usize> {
        let x_bounds = match int_bounds(query.x.as_ref()) {
            Some(bounds) => bounds,
            None => return vec![],
        };
        let y_bounds = match int_bounds(query.y.as_ref()) {
            Some(bounds) => bounds,
            None => return vec![],
        };
        let z_bounds = match int_bounds(query.z.as_ref()) {
            Some(bounds) => bounds,
            None => return vec![],
        };

        let lower_code = morton_encode_i32(x_bounds.0, y_bounds.0, z_bounds.0);
        let upper_code = morton_encode_i32(x_bounds.1, y_bounds.1, z_bounds.1);

        let start = self
            .morton_sorted
            .partition_point(|(code, _)| *code < lower_code);
        let end = self
            .morton_sorted
            .partition_point(|(code, _)| *code <= upper_code);

        let mut candidates = self.morton_sorted[start..end]
            .iter()
            .map(|(_, row_id)| *row_id)
            .filter(|row_id| {
                let row_id = *row_id;
                matches_int(columns.x[row_id], query.x.as_ref())
                    && matches_int(columns.y[row_id], query.y.as_ref())
                    && matches_int(columns.z[row_id], query.z.as_ref())
            })
            .collect::<Vec<_>>();

        candidates.sort_unstable();
        candidates
    }

    fn time_candidates(&self, time_predicate: &LongPredicate) -> Vec<usize> {
        let bounds = match long_bounds(Some(time_predicate)) {
            Some(bounds) => bounds,
            None => return vec![],
        };

        let start = self
            .time_sorted
            .partition_point(|(time, _)| *time < bounds.0);
        let end = self
            .time_sorted
            .partition_point(|(time, _)| *time <= bounds.1);

        let mut ids = self.time_sorted[start..end]
            .iter()
            .map(|(_, row_id)| *row_id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WalRecord {
    seq: u64,
    row: Row,
}

#[derive(Debug)]
struct Wal {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl Wal {
    fn open(path: PathBuf) -> Result<Self, DbError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
        })
    }

    fn append_batch(&mut self, records: &[WalRecord]) -> Result<(), DbError> {
        if records.is_empty() {
            return Ok(());
        }

        for record in records {
            let payload = bincode::serialize(record)?;
            let len = payload.len() as u32;
            self.writer.write_all(&len.to_le_bytes())?;
            self.writer.write_all(&payload)?;
        }
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    fn replay(path: &Path) -> Result<Vec<WalRecord>, DbError> {
        if !path.exists() {
            return Ok(vec![]);
        }
        let mut file = File::open(path)?;
        let mut records = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(DbError::Io(err)),
            }

            let len = u32::from_le_bytes(len_buf) as usize;
            if len == 0 {
                return Err(DbError::CorruptedWal("zero-length entry".to_string()));
            }
            let mut payload = vec![0u8; len];
            match file.read_exact(&mut payload) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                    return Err(DbError::CorruptedWal(
                        "incomplete wal frame payload".to_string(),
                    ));
                }
                Err(err) => return Err(DbError::Io(err)),
            }

            let record: WalRecord = bincode::deserialize(&payload)?;
            records.push(record);
        }

        Ok(records)
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn truncate(&mut self) -> Result<(), DbError> {
        self.writer.flush()?;
        let file = self.writer.get_mut();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.sync_data()?;
        Ok(())
    }
}

#[derive(Debug)]
struct DbState {
    memtable: MemTable,
    segments: Vec<Segment>,
    wal: Wal,
    next_segment_id: u64,
}

#[derive(Debug)]
pub struct SpaceLoggerDb {
    db_dir: PathBuf,
    segments_dir: PathBuf,
    options: DbOptions,
    version: AtomicU64,
    state: RwLock<DbState>,
}

impl SpaceLoggerDb {
    pub fn open(db_dir: impl AsRef<Path>, options: DbOptions) -> Result<Self, DbError> {
        let db_dir = db_dir.as_ref().to_path_buf();
        fs::create_dir_all(&db_dir)?;

        let segments_dir = db_dir.join("segments");
        fs::create_dir_all(&segments_dir)?;

        let mut segments = load_segments(&segments_dir)?;
        segments.sort_unstable_by_key(|segment| segment.id);

        let max_segment_id = segments.iter().map(|segment| segment.id).max().unwrap_or(0);
        let max_segment_seq = segments
            .iter()
            .filter_map(Segment::max_seq)
            .max()
            .unwrap_or(0);

        let wal_path = db_dir.join("wal.log");
        let wal_records = Wal::replay(&wal_path)?;

        let mut memtable = MemTable::default();
        let mut max_wal_seq = max_segment_seq;
        for record in wal_records {
            max_wal_seq = max_wal_seq.max(record.seq);
            if record.seq > max_segment_seq {
                memtable.insert(record.seq, &record.row);
            }
        }

        let wal = Wal::open(wal_path)?;

        let state = DbState {
            memtable,
            segments,
            wal,
            next_segment_id: max_segment_id + 1,
        };

        Ok(Self {
            db_dir,
            segments_dir,
            options,
            version: AtomicU64::new(max_wal_seq),
            state: RwLock::new(state),
        })
    }

    pub fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    pub fn insert_with_version(&self, row: Row, expected_version: u64) -> Result<u64, DbError> {
        self.insert_batch_with_version(vec![row], expected_version)
    }

    pub fn insert_batch_with_version(
        &self,
        rows: Vec<Row>,
        expected_version: u64,
    ) -> Result<u64, DbError> {
        let precheck = self.current_version();
        if precheck != expected_version {
            return Err(DbError::VersionConflict {
                expected: expected_version,
                actual: precheck,
            });
        }

        let mut state = self.state.write().map_err(|_| DbError::PoisonedLock)?;
        let actual = self.current_version();
        if actual != expected_version {
            return Err(DbError::VersionConflict {
                expected: expected_version,
                actual,
            });
        }

        let mut seq = actual;
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            seq += 1;
            records.push(WalRecord { seq, row });
        }

        state.wal.append_batch(&records)?;
        for record in &records {
            state.memtable.insert(record.seq, &record.row);
        }
        self.version.store(seq, Ordering::SeqCst);

        if state.memtable.len() >= self.options.memtable_flush_rows.max(1) {
            self.flush_locked(&mut state)?;
        }

        Ok(seq)
    }

    pub fn flush(&self) -> Result<(), DbError> {
        let mut state = self.state.write().map_err(|_| DbError::PoisonedLock)?;
        self.flush_locked(&mut state)
    }

    pub fn compact(&self) -> Result<(), DbError> {
        let mut state = self.state.write().map_err(|_| DbError::PoisonedLock)?;

        if !state.memtable.is_empty() {
            self.flush_locked(&mut state)?;
        }
        if state.segments.len() <= 1 {
            return Ok(());
        }

        let old_paths = state
            .segments
            .iter()
            .map(|segment| segment.path.clone())
            .collect::<Vec<_>>();

        let merged_columns = merge_segments_columns(&state.segments);
        let new_segment_id = state.next_segment_id;
        let new_segment_path = self
            .segments_dir
            .join(format!("segment_{new_segment_id}.bin"));

        persist_segment(&new_segment_path, &merged_columns)?;
        for path in &old_paths {
            if let Err(err) = fs::remove_file(path) {
                if err.kind() != ErrorKind::NotFound {
                    return Err(DbError::Io(err));
                }
            }
        }

        let merged_segment =
            Segment::from_columns(new_segment_id, new_segment_path, merged_columns);
        state.segments = vec![merged_segment];
        state.next_segment_id += 1;

        Ok(())
    }

    pub fn query(&self, query: &Query) -> Result<Vec<Row>, DbError> {
        let state = self.state.read().map_err(|_| DbError::PoisonedLock)?;

        let mut rows_with_seq = Vec::new();
        for segment in &state.segments {
            rows_with_seq.extend(segment.query(query));
        }
        rows_with_seq.extend(state.memtable.query(query));

        rows_with_seq.sort_unstable_by_key(|(seq, _)| *seq);
        Ok(rows_with_seq
            .into_iter()
            .map(|(_, row)| row)
            .collect::<Vec<_>>())
    }

    pub fn db_dir(&self) -> &Path {
        &self.db_dir
    }

    pub fn wal_path(&self) -> Result<PathBuf, DbError> {
        let state = self.state.read().map_err(|_| DbError::PoisonedLock)?;
        Ok(state.wal.path().to_path_buf())
    }

    fn flush_locked(&self, state: &mut DbState) -> Result<(), DbError> {
        if state.memtable.is_empty() {
            return Ok(());
        }

        let segment_id = state.next_segment_id;
        let path = self.segments_dir.join(format!("segment_{segment_id}.bin"));
        let columns = state.memtable.to_column_store();

        persist_segment(&path, &columns)?;

        let segment = Segment::from_columns(segment_id, path, columns);
        state.segments.push(segment);
        state.next_segment_id += 1;
        state.memtable.clear();
        state.wal.truncate()?;

        Ok(())
    }
}

fn persist_segment(path: &Path, columns: &ColumnStore) -> Result<(), DbError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    let mut writer = BufWriter::new(file);
    let bytes = bincode::serialize(columns)?;
    writer.write_all(&bytes)?;
    writer.flush()?;
    writer.get_ref().sync_data()?;
    Ok(())
}

fn merge_segments_columns(segments: &[Segment]) -> ColumnStore {
    let mut rows = Vec::new();
    for segment in segments {
        for row_id in 0..segment.columns.len() {
            rows.push((segment.columns.seq[row_id], segment.columns.row_at(row_id)));
        }
    }
    rows.sort_unstable_by_key(|(seq, _)| *seq);

    let mut merged = ColumnStore::default();
    for (seq, row) in rows {
        merged.push(seq, &row);
    }
    merged
}

fn load_segments(segments_dir: &Path) -> Result<Vec<Segment>, DbError> {
    let mut segments = Vec::new();

    for entry in fs::read_dir(segments_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };

        let Some(segment_id) = parse_segment_id(file_name) else {
            continue;
        };

        segments.push(Segment::load(segment_id, path)?);
    }

    Ok(segments)
}

fn parse_segment_id(file_name: &str) -> Option<u64> {
    if !file_name.starts_with("segment_") || !file_name.ends_with(".bin") {
        return None;
    }
    let raw_id = file_name
        .trim_start_matches("segment_")
        .trim_end_matches(".bin");
    raw_id.parse::<u64>().ok()
}

fn initial_candidates_from_string_filters(
    row_count: usize,
    query: &Query,
    subject_index: &HashMap<String, Vec<usize>>,
    object_index: &HashMap<String, Vec<usize>>,
    verb_index: &HashMap<String, Vec<usize>>,
) -> Option<Vec<usize>> {
    let mut candidate: Option<Vec<usize>> = None;

    if let Some(subject) = query.subject.as_ref() {
        let ids = subject_index.get(subject).cloned().unwrap_or_default();
        candidate = Some(intersect_sorted_vecs(
            candidate.unwrap_or_else(|| (0..row_count).collect()),
            ids,
        ));
    }

    if let Some(object) = query.object.as_ref() {
        let ids = object_index.get(object).cloned().unwrap_or_default();
        candidate = Some(intersect_sorted_vecs(
            candidate.unwrap_or_else(|| (0..row_count).collect()),
            ids,
        ));
    }

    if let Some(verb) = query.verb.as_ref() {
        let ids = verb_index.get(verb).cloned().unwrap_or_default();
        candidate = Some(intersect_sorted_vecs(
            candidate.unwrap_or_else(|| (0..row_count).collect()),
            ids,
        ));
    }

    candidate
}

fn materialize_matches(
    columns: &ColumnStore,
    query: &Query,
    row_ids: Vec<usize>,
) -> Vec<(u64, Row)> {
    row_ids
        .into_iter()
        .filter(|row_id| {
            let row_id = *row_id;
            matches_int(columns.x[row_id], query.x.as_ref())
                && matches_int(columns.y[row_id], query.y.as_ref())
                && matches_int(columns.z[row_id], query.z.as_ref())
                && matches_long(columns.time_ms[row_id], query.time_ms.as_ref())
                && query
                    .subject
                    .as_ref()
                    .is_none_or(|subject| columns.subject[row_id] == *subject)
                && query
                    .object
                    .as_ref()
                    .is_none_or(|object| columns.object[row_id] == *object)
                && query
                    .verb
                    .as_ref()
                    .is_none_or(|verb| columns.verb[row_id] == *verb)
        })
        .map(|row_id| (columns.seq[row_id], columns.row_at(row_id)))
        .collect::<Vec<_>>()
}

fn matches_int(value: i32, predicate: Option<&IntPredicate>) -> bool {
    predicate.map_or(true, |pred| pred.matches(value))
}

fn matches_long(value: i64, predicate: Option<&LongPredicate>) -> bool {
    predicate.map_or(true, |pred| pred.matches(value))
}

fn has_xyz_filter(query: &Query) -> bool {
    query.x.as_ref().is_some_and(IntPredicate::is_effective)
        || query.y.as_ref().is_some_and(IntPredicate::is_effective)
        || query.z.as_ref().is_some_and(IntPredicate::is_effective)
}

fn int_bounds(predicate: Option<&IntPredicate>) -> Option<(i32, i32)> {
    let mut min = i32::MIN;
    let mut max = i32::MAX;

    if let Some(pred) = predicate {
        if let Some(eq) = pred.eq {
            min = eq;
            max = eq;
        }
        if let Some(gte) = pred.gte {
            min = min.max(gte);
        }
        if let Some(gt) = pred.gt {
            if gt == i32::MAX {
                return None;
            }
            min = min.max(gt + 1);
        }
        if let Some(lte) = pred.lte {
            max = max.min(lte);
        }
        if let Some(lt) = pred.lt {
            if lt == i32::MIN {
                return None;
            }
            max = max.min(lt - 1);
        }
    }

    if min > max {
        return None;
    }

    Some((min, max))
}

fn long_bounds(predicate: Option<&LongPredicate>) -> Option<(i64, i64)> {
    let mut min = i64::MIN;
    let mut max = i64::MAX;

    if let Some(pred) = predicate {
        if let Some(eq) = pred.eq {
            min = eq;
            max = eq;
        }
        if let Some(gte) = pred.gte {
            min = min.max(gte);
        }
        if let Some(gt) = pred.gt {
            if gt == i64::MAX {
                return None;
            }
            min = min.max(gt + 1);
        }
        if let Some(lte) = pred.lte {
            max = max.min(lte);
        }
        if let Some(lt) = pred.lt {
            if lt == i64::MIN {
                return None;
            }
            max = max.min(lt - 1);
        }
    }

    if min > max {
        return None;
    }

    Some((min, max))
}

fn intersect_sorted_vecs(mut left: Vec<usize>, mut right: Vec<usize>) -> Vec<usize> {
    if left.is_empty() || right.is_empty() {
        return vec![];
    }

    left.sort_unstable();
    right.sort_unstable();

    let mut output = Vec::with_capacity(left.len().min(right.len()));
    let mut i = 0usize;
    let mut j = 0usize;

    while i < left.len() && j < right.len() {
        match left[i].cmp(&right[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                output.push(left[i]);
                i += 1;
                j += 1;
            }
        }
    }

    output
}

fn morton_encode_i32(x: i32, y: i32, z: i32) -> u128 {
    morton_encode_u32(
        signed_to_ordered_u32(x),
        signed_to_ordered_u32(y),
        signed_to_ordered_u32(z),
    )
}

fn signed_to_ordered_u32(value: i32) -> u32 {
    (value as u32) ^ 0x8000_0000
}

fn morton_encode_u32(x: u32, y: u32, z: u32) -> u128 {
    let mut code = 0u128;
    for bit in 0..32usize {
        code |= ((x as u128 >> bit) & 1) << (3 * bit);
        code |= ((y as u128 >> bit) & 1) << (3 * bit + 1);
        code |= ((z as u128 >> bit) & 1) << (3 * bit + 2);
    }
    code
}
