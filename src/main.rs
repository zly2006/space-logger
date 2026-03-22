use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use space_logger::{
    DbError, DbOptions, IntPredicate, LongPredicate, Query, Row, SpaceLoggerDb, VERB_MASK_ALL,
    verb_id_from_name, verb_mask_single, verb_name,
};

#[derive(Parser, Debug)]
#[command(name = "space-logger")]
#[command(about = "OLAP 3D logger database CLI")]
struct Cli {
    #[arg(long, default_value = "./space-logger-data")]
    db: PathBuf,

    #[arg(long, default_value_t = 4096)]
    memtable_flush_rows: usize,

    #[arg(long, default_value_t = 50)]
    same_location_kill_limit: usize,

    #[arg(long, default_value_t = true)]
    enable_background_maintenance: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Add {
        x: i32,
        y: i32,
        z: i32,
        subject: String,
        verb: String,
        object: String,
        time_ms: Option<i64>,

        #[arg(long, default_value = "")]
        subject_extra: String,

        #[arg(long, default_value = "")]
        data_hex: String,
    },
    Query {
        #[arg(long)]
        limit: Option<usize>,

        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        filters: Vec<String>,
    },
    Delete {
        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        filters: Vec<String>,
    },
    Compact,
}

#[derive(Debug)]
enum CliError {
    Db(DbError),
    Usage(String),
}

impl Display for CliError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(err) => write!(f, "{err}"),
            Self::Usage(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for CliError {}

impl From<DbError> for CliError {
    fn from(value: DbError) -> Self {
        Self::Db(value)
    }
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), CliError> {
    let cli = Cli::parse();
    let db = SpaceLoggerDb::open(
        &cli.db,
        DbOptions {
            memtable_flush_rows: cli.memtable_flush_rows,
            same_location_kill_limit: cli.same_location_kill_limit,
            enable_background_maintenance: cli.enable_background_maintenance,
        },
    )?;

    match cli.command {
        Command::Add {
            x,
            y,
            z,
            subject,
            verb,
            object,
            time_ms,
            subject_extra,
            data_hex,
        } => {
            let final_time_ms = time_ms.unwrap_or_else(now_ms);
            let verb = parse_verb_input(&verb)?;
            let row = Row {
                x,
                y,
                z,
                subject,
                object,
                verb,
                time_ms: final_time_ms,
                subject_extra,
                data: parse_hex(&data_hex)?,
            };

            let expected = db.current_version();
            let seq = db.insert_with_version(row, expected)?;
            println!("ok seq={seq}");
        }
        Command::Query { limit, filters } => {
            let query = parse_query_filters(&filters)?;
            let rows = db.query(&query, limit)?;

            println!("matched_rows={}", rows.len());
            for row in rows {
                println!(
                    "x={} y={} z={} subject={} verb={} object={} time_ms={} subject_extra={} data_hex={}",
                    row.x,
                    row.y,
                    row.z,
                    row.subject,
                    verb_name(row.verb),
                    row.object,
                    row.time_ms,
                    row.subject_extra,
                    to_hex(&row.data)
                );
            }
        }
        Command::Delete { filters } => {
            let query = parse_query_filters(&filters)?;
            let deleted = db.delete_where(&query)?;
            println!("deleted_rows={deleted}");
        }
        Command::Compact => {
            db.compact()?;
            println!("ok compacted");
        }
    }

    Ok(())
}

fn now_ms() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch");
    (duration.as_secs() as i64) * 1_000 + i64::from(duration.subsec_millis())
}

fn parse_query_filters(filters: &[String]) -> Result<Query, CliError> {
    if filters.is_empty() {
        return Err(CliError::Usage(
            "query/delete requires at least one filter".to_string(),
        ));
    }

    let mut query = Query::default();
    let mut index = 0usize;

    while index < filters.len() {
        let field = filters[index].to_lowercase();
        index += 1;

        if index >= filters.len() {
            return Err(CliError::Usage(format!(
                "missing operator after field `{field}`"
            )));
        }
        let op = filters[index].to_lowercase();
        index += 1;

        match field.as_str() {
            "x" | "y" | "z" => {
                let predicate = int_predicate_slot(&mut query, field.as_str());
                parse_int_operator(predicate, &op, filters, &mut index, field.as_str())?;
            }
            "time" | "time_ms" => {
                let predicate = query.time_ms.get_or_insert_with(LongPredicate::default);
                parse_long_operator(predicate, &op, filters, &mut index, "time_ms")?;
            }
            "subject" => {
                let value = next_token(filters, &mut index, "subject value")?.to_string();
                if op != "=" && op != "==" {
                    return Err(CliError::Usage(
                        "subject only supports `=` operator".to_string(),
                    ));
                }
                assign_string_field(&mut query.subject, value, "subject")?;
            }
            "object" => {
                let value = next_token(filters, &mut index, "object value")?.to_string();
                if op != "=" && op != "==" {
                    return Err(CliError::Usage(
                        "object only supports `=` operator".to_string(),
                    ));
                }
                assign_string_field(&mut query.object, value, "object")?;
            }
            "verb" => {
                let value = next_token(filters, &mut index, "verb value")?.to_string();
                let new_mask = match op.as_str() {
                    "=" | "==" => verb_mask_single(parse_verb_input(&value)?),
                    "in" => parse_verb_in_mask(&value)?,
                    _ => {
                        return Err(CliError::Usage(
                            "verb only supports `=` or `in` operator".to_string(),
                        ));
                    }
                };
                if query.verb_mask == VERB_MASK_ALL {
                    query.verb_mask = new_mask;
                } else {
                    query.verb_mask &= new_mask;
                }
            }
            _ => {
                return Err(CliError::Usage(format!(
                    "unknown field `{field}`; allowed: x y z time_ms subject object verb"
                )));
            }
        }
    }

    Ok(query)
}

fn int_predicate_slot<'a>(query: &'a mut Query, field: &str) -> &'a mut IntPredicate {
    match field {
        "x" => query.x.get_or_insert_with(IntPredicate::default),
        "y" => query.y.get_or_insert_with(IntPredicate::default),
        "z" => query.z.get_or_insert_with(IntPredicate::default),
        _ => unreachable!("unsupported int field"),
    }
}

fn parse_int_operator(
    predicate: &mut IntPredicate,
    op: &str,
    filters: &[String],
    index: &mut usize,
    field: &str,
) -> Result<(), CliError> {
    match op {
        "=" | "==" => {
            let value = parse_i32(next_token(filters, index, field)?, field)?;
            predicate.eq = Some(value);
        }
        ">" => {
            let value = parse_i32(next_token(filters, index, field)?, field)?;
            predicate.gt = Some(value);
        }
        ">=" => {
            let value = parse_i32(next_token(filters, index, field)?, field)?;
            predicate.gte = Some(value);
        }
        "<" => {
            let value = parse_i32(next_token(filters, index, field)?, field)?;
            predicate.lt = Some(value);
        }
        "<=" => {
            let value = parse_i32(next_token(filters, index, field)?, field)?;
            predicate.lte = Some(value);
        }
        "between" | "bet" => {
            let low = parse_i32(next_token(filters, index, field)?, field)?;
            let high = parse_i32(next_token(filters, index, field)?, field)?;
            if low > high {
                return Err(CliError::Usage(format!(
                    "`{field} between` requires low <= high"
                )));
            }
            predicate.gte = Some(low);
            predicate.lte = Some(high);
        }
        _ => {
            return Err(CliError::Usage(format!(
                "unsupported operator `{op}` for `{field}`"
            )));
        }
    }
    Ok(())
}

fn parse_long_operator(
    predicate: &mut LongPredicate,
    op: &str,
    filters: &[String],
    index: &mut usize,
    field: &str,
) -> Result<(), CliError> {
    match op {
        "=" | "==" => {
            let value = parse_i64(next_token(filters, index, field)?, field)?;
            predicate.eq = Some(value);
        }
        ">" => {
            let value = parse_i64(next_token(filters, index, field)?, field)?;
            predicate.gt = Some(value);
        }
        ">=" => {
            let value = parse_i64(next_token(filters, index, field)?, field)?;
            predicate.gte = Some(value);
        }
        "<" => {
            let value = parse_i64(next_token(filters, index, field)?, field)?;
            predicate.lt = Some(value);
        }
        "<=" => {
            let value = parse_i64(next_token(filters, index, field)?, field)?;
            predicate.lte = Some(value);
        }
        "between" | "bet" => {
            let low = parse_i64(next_token(filters, index, field)?, field)?;
            let high = parse_i64(next_token(filters, index, field)?, field)?;
            if low > high {
                return Err(CliError::Usage(format!(
                    "`{field} between` requires low <= high"
                )));
            }
            predicate.gte = Some(low);
            predicate.lte = Some(high);
        }
        _ => {
            return Err(CliError::Usage(format!(
                "unsupported operator `{op}` for `{field}`"
            )));
        }
    }
    Ok(())
}

fn assign_string_field(
    slot: &mut Option<String>,
    value: String,
    field: &str,
) -> Result<(), CliError> {
    if let Some(existing) = slot {
        if existing != &value {
            return Err(CliError::Usage(format!(
                "conflicting duplicate filter for `{field}`"
            )));
        }
    } else {
        *slot = Some(value);
    }
    Ok(())
}

fn next_token<'a>(
    filters: &'a [String],
    index: &mut usize,
    label: &str,
) -> Result<&'a str, CliError> {
    if *index >= filters.len() {
        return Err(CliError::Usage(format!("missing token for {label}")));
    }
    let token = &filters[*index];
    *index += 1;
    Ok(token)
}

fn parse_i32(value: &str, field: &str) -> Result<i32, CliError> {
    value
        .parse::<i32>()
        .map_err(|_| CliError::Usage(format!("invalid i32 value `{value}` for field `{field}`")))
}

fn parse_i64(value: &str, field: &str) -> Result<i64, CliError> {
    value
        .parse::<i64>()
        .map_err(|_| CliError::Usage(format!("invalid i64 value `{value}` for field `{field}`")))
}

fn parse_verb_input(value: &str) -> Result<u32, CliError> {
    let normalized = value.trim().to_lowercase();
    if normalized.is_empty() {
        return Err(CliError::Usage("verb cannot be empty".to_string()));
    }
    if let Some(verb) = verb_id_from_name(&normalized) {
        return Ok(verb);
    }
    let numeric = normalized.parse::<u32>().map_err(|_| {
        CliError::Usage(format!(
            "invalid verb `{value}`, expected known name or integer in 0..31"
        ))
    })?;
    if numeric >= 32 {
        return Err(CliError::Usage(format!(
            "invalid verb id `{numeric}`, expected integer in 0..31"
        )));
    }
    Ok(numeric)
}

fn parse_verb_in_mask(value: &str) -> Result<u32, CliError> {
    let mut mask = 0u32;
    for raw in value.split(',') {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        mask |= verb_mask_single(parse_verb_input(token)?);
    }
    if mask == 0 {
        return Err(CliError::Usage(
            "verb in requires at least one valid verb".to_string(),
        ));
    }
    Ok(mask)
}

fn parse_hex(hex: &str) -> Result<Vec<u8>, CliError> {
    let trimmed = hex.trim();
    if trimmed.is_empty() {
        return Ok(vec![]);
    }

    let raw = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);

    if raw.len() % 2 != 0 {
        return Err(CliError::Usage(
            "data_hex must have even number of hex characters".to_string(),
        ));
    }

    let mut out = Vec::with_capacity(raw.len() / 2);
    let bytes = raw.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let high = hex_nibble(bytes[i])?;
        let low = hex_nibble(bytes[i + 1])?;
        out.push((high << 4) | low);
        i += 2;
    }

    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8, CliError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(CliError::Usage(format!(
            "invalid hex digit `{}` in data_hex",
            byte as char
        ))),
    }
}

fn to_hex(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 2);
    for byte in data {
        out.push(hex_char((byte >> 4) & 0x0f));
        out.push(hex_char(byte & 0x0f));
    }
    out
}

fn hex_char(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'a' + (nibble - 10)) as char,
        _ => unreachable!("nibble must be in 0..=15"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_query_between_and_equals() {
        let filters = vec![
            "x".to_string(),
            "between".to_string(),
            "0".to_string(),
            "100".to_string(),
            "y".to_string(),
            "=".to_string(),
            "10".to_string(),
            "subject".to_string(),
            "=".to_string(),
            "alice".to_string(),
        ];

        let query = parse_query_filters(&filters).expect("parse should work");
        assert_eq!(query.x.as_ref().and_then(|pred| pred.gte), Some(0));
        assert_eq!(query.x.as_ref().and_then(|pred| pred.lte), Some(100));
        assert_eq!(query.y.as_ref().and_then(|pred| pred.eq), Some(10));
        assert_eq!(query.subject.as_deref(), Some("alice"));
    }

    #[test]
    fn parse_query_rejects_unknown_field() {
        let filters = vec!["foo".to_string(), "=".to_string(), "1".to_string()];
        let err = parse_query_filters(&filters).expect_err("parse should fail");
        assert!(
            err.to_string().contains("unknown field"),
            "error should mention unknown field"
        );
    }

    #[test]
    fn parse_hex_supports_empty_and_prefixed() {
        assert_eq!(parse_hex("").expect("empty should parse"), vec![]);
        assert_eq!(
            parse_hex("0x0a0b").expect("prefixed should parse"),
            vec![0x0a, 0x0b]
        );
    }

    #[test]
    fn parse_hex_rejects_invalid_input() {
        let err = parse_hex("0xz1").expect_err("invalid hex should fail");
        assert!(
            err.to_string().contains("invalid hex"),
            "error should mention invalid hex"
        );
    }
}
