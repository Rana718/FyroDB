use crate::storage::{
    store::Store,
    value::{FyroDB, HashInner, ListInner, SetInner, SmallStr, StoreValue, now_ms},
};
use foldhash::{HashMap, HashMapExt, HashSetExt};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const MAGIC: &[u8; 4] = b"FLDB";
const VERSION: u8 = 2;
const TYPE_STRING: u8 = 0;
const TYPE_HASH: u8 = 1;
const TYPE_LIST: u8 = 2;
const TYPE_SET: u8 = 3;
const TYPE_ZSET: u8 = 4;
const TYPE_JSON: u8 = 5;
const TYPE_STREAM: u8 = 6;
const TYPE_EOF: u8 = 0xFF;

const MAX_LOAD_STRING: u32 = 512 * 1024 * 1024;
const MAX_LOAD_ELEMENTS: u32 = 4 * 1024 * 1024;
const REPL_MAGIC: &[u8; 4] = b"FLRP";
const REPL_VERSION: u8 = 1;
const CLUSTER_MAGIC: &[u8; 4] = b"FLCL";
const CLUSTER_VERSION: u8 = 1;

pub fn save_cluster_metadata(path: &str, node_id: &str, epoch: u64) -> io::Result<()> {
    if node_id.is_empty() || node_id.len() > u16::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid node id",
        ));
    }
    let tmp = format!("{path}.tmp");
    let mut file = File::create(&tmp)?;
    file.write_all(CLUSTER_MAGIC)?;
    file.write_all(&[CLUSTER_VERSION])?;
    file.write_all(&(node_id.len() as u16).to_le_bytes())?;
    file.write_all(node_id.as_bytes())?;
    file.write_all(&epoch.to_le_bytes())?;
    file.flush()?;
    fsync_file(&file)?;
    fs::rename(tmp, path)
}

pub fn load_cluster_metadata(path: &str) -> io::Result<Option<(String, u64)>> {
    if !Path::new(path).exists() {
        return Ok(None);
    }
    let mut file = File::open(path)?;
    let mut header = [0u8; 7];
    file.read_exact(&mut header)?;
    if &header[..4] != CLUSTER_MAGIC || header[4] != CLUSTER_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid cluster metadata",
        ));
    }
    let len = u16::from_le_bytes([header[5], header[6]]) as usize;
    let mut id = vec![0u8; len];
    file.read_exact(&mut id)?;
    let mut epoch = [0u8; 8];
    file.read_exact(&mut epoch)?;
    let id = String::from_utf8(id)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid node id"))?;
    Ok(Some((id, u64::from_le_bytes(epoch))))
}

#[cfg(test)]
mod cluster_metadata_tests {
    use super::*;

    #[test]
    fn cluster_metadata_round_trip_is_strict() {
        let path = std::env::temp_dir().join(format!("fyrodb-cluster-meta-{}", std::process::id()));
        let path = path.to_str().unwrap();
        save_cluster_metadata(path, "node-a", 42).unwrap();
        assert_eq!(
            load_cluster_metadata(path).unwrap(),
            Some(("node-a".into(), 42))
        );
        let _ = std::fs::remove_file(path);
    }
}

pub fn save_replication_metadata(path: &str, epoch: u64, offset: u64) -> io::Result<()> {
    let tmp = format!("{path}.tmp");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(REPL_MAGIC)?;
        file.write_all(&[REPL_VERSION])?;
        file.write_all(&epoch.to_le_bytes())?;
        file.write_all(&offset.to_le_bytes())?;
        file.flush()?;
        fsync_file(&file)?;
    }
    fs::rename(tmp, path)?;
    Ok(())
}

pub fn load_replication_metadata(path: &str) -> io::Result<Option<(u64, u64)>> {
    if !Path::new(path).exists() {
        return Ok(None);
    }
    let mut file = File::open(path)?;
    let mut bytes = [0u8; 21];
    file.read_exact(&mut bytes)?;
    if &bytes[..4] != REPL_MAGIC || bytes[4] != REPL_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid replication metadata",
        ));
    }
    Ok(Some((
        u64::from_le_bytes(bytes[5..13].try_into().unwrap()),
        u64::from_le_bytes(bytes[13..21].try_into().unwrap()),
    )))
}

pub fn save(store: &Store, path: &str) -> io::Result<()> {
    let tmp = format!("{path}.tmp");
    {
        let f = File::create(&tmp)?;
        let mut w = BufWriter::with_capacity(1 << 20, f);

        w.write_all(MAGIC)?;
        w.write_all(&[VERSION])?;

        let shard_count = store.data.shard_count();
        let mut write_result = Ok(());

        for shard_idx in 0..shard_count {
            if write_result.is_err() {
                break;
            }
            let slot_count = store.data.shard_slot_count(shard_idx);
            for slot_idx in 0..slot_count {
                if write_result.is_err() {
                    break;
                }
                let Some((key, val)) = store.data.peek_slot(shard_idx, slot_idx) else {
                    continue;
                };
                if val.is_expired() {
                    continue;
                }

                let ttl_ms = val.expires_ms;

                match &val.value {
                    FyroDB::String(s) => {
                        write_result = (|| {
                            write_u8(&mut w, TYPE_STRING)?;
                            write_u64(&mut w, ttl_ms)?;
                            write_bytes(&mut w, key.as_bytes())?;
                            write_bytes(&mut w, s.as_bytes())
                        })();
                    }
                    FyroDB::Hash(h) => {
                        write_result = (|| {
                            write_u8(&mut w, TYPE_HASH)?;
                            write_u64(&mut w, ttl_ms)?;
                            write_bytes(&mut w, key.as_bytes())?;
                            write_u32(&mut w, h.len() as u32)?;
                            for (f, v) in h.iter() {
                                write_bytes(&mut w, f.as_bytes())?;
                                write_bytes(&mut w, v.as_bytes())?;
                            }
                            Ok(())
                        })();
                    }
                    FyroDB::List(l) => {
                        write_result = (|| {
                            write_u8(&mut w, TYPE_LIST)?;
                            write_u64(&mut w, ttl_ms)?;
                            write_bytes(&mut w, key.as_bytes())?;
                            write_u32(&mut w, l.deque().len() as u32)?;
                            for item in l.deque().iter() {
                                write_bytes(&mut w, item.as_bytes())?;
                            }
                            Ok(())
                        })();
                    }
                    FyroDB::Set(s) => {
                        write_result = (|| {
                            write_u8(&mut w, TYPE_SET)?;
                            write_u64(&mut w, ttl_ms)?;
                            write_bytes(&mut w, key.as_bytes())?;
                            write_u32(&mut w, s.len() as u32)?;
                            for member in s.iter() {
                                write_bytes(&mut w, member.to_string().as_bytes())?;
                            }
                            Ok(())
                        })();
                    }
                    FyroDB::ZSet(z) => {
                        write_result = (|| {
                            write_u8(&mut w, TYPE_ZSET)?;
                            write_u64(&mut w, ttl_ms)?;
                            write_bytes(&mut w, key.as_bytes())?;
                            write_u32(&mut w, z.len() as u32)?;
                            for entry in z.iter() {
                                write_bytes(&mut w, entry.member.as_bytes())?;
                                write_u64(&mut w, entry.score.to_bits())?;
                            }
                            Ok(())
                        })();
                    }
                    FyroDB::Json(j) => {
                        write_result = (|| {
                            write_u8(&mut w, TYPE_JSON)?;
                            write_u64(&mut w, ttl_ms)?;
                            write_bytes(&mut w, key.as_bytes())?;
                            write_bytes(&mut w, j.as_bytes())
                        })();
                    }
                    FyroDB::Stream(s) => {
                        write_result = (|| {
                            write_u8(&mut w, TYPE_STREAM)?;
                            write_u64(&mut w, ttl_ms)?;
                            write_bytes(&mut w, key.as_bytes())?;
                            write_u32(&mut w, s.entries.len() as u32)?;
                            for (id, fields) in s.entries.iter() {
                                write_u64(&mut w, id.ms)?;
                                write_u64(&mut w, id.seq)?;
                                write_u32(&mut w, fields.len() as u32)?;
                                for (f, v) in fields {
                                    write_bytes(&mut w, f.as_bytes())?;
                                    write_bytes(&mut w, v.as_bytes())?;
                                }
                            }
                            Ok(())
                        })();
                    }
                }
            }
        }
        write_result?;

        write_u8(&mut w, TYPE_EOF)?;
        w.flush()?;

        let inner = w.into_inner().map_err(|e| e.into_error())?;
        fsync_file(&inner)?;
    }

    fs::rename(&tmp, path)?;

    if let Some(parent) = Path::new(path).parent()
        && let Ok(dir) = File::open(parent)
    {
        let _ = fsync_file(&dir);
    }

    Ok(())
}

pub fn load(store: &Store, path: &str) -> io::Result<usize> {
    load_with_policy(store, path, false)
}

pub fn load_strict(store: &Store, path: &str) -> io::Result<usize> {
    load_with_policy(store, path, true)
}

fn load_with_policy(store: &Store, path: &str, strict: bool) -> io::Result<usize> {
    if !Path::new(path).exists() {
        return Ok(0);
    }

    let f = File::open(path)?;
    let file_len = f.metadata()?.len();
    if file_len < 6 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RDB file too short",
        ));
    }

    let mut r = BufReader::with_capacity(1 << 20, f);

    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid RDB magic",
        ));
    }
    let version = read_u8(&mut r)?;
    if version > VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported RDB version",
        ));
    }

    let now = now_ms();
    let mut count = 0usize;

    loop {
        let type_byte = match read_u8(&mut r) {
            Ok(b) => b,
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                if strict {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated RDB snapshot",
                    ));
                }
                eprintln!("[rdb] warning: truncated file, loaded {count} keys");
                break;
            }
            Err(e) => return Err(e),
        };

        if type_byte == TYPE_EOF {
            break;
        }

        let ttl_ms = read_u64(&mut r)?;
        let key = read_string_bounded(&mut r)?;

        if ttl_ms != 0 && ttl_ms <= now {
            match type_byte {
                TYPE_STRING => {
                    skip_string(&mut r)?;
                }
                TYPE_HASH => {
                    let n = read_u32(&mut r)? as usize;
                    for _ in 0..n {
                        skip_string(&mut r)?;
                        skip_string(&mut r)?;
                    }
                }
                TYPE_LIST | TYPE_SET => {
                    let n = read_u32(&mut r)? as usize;
                    for _ in 0..n {
                        skip_string(&mut r)?;
                    }
                }
                TYPE_ZSET => {
                    let n = read_u32(&mut r)? as usize;
                    for _ in 0..n {
                        skip_string(&mut r)?;
                        let _ = read_u64(&mut r)?;
                    }
                }
                TYPE_JSON => {
                    skip_string(&mut r)?;
                }
                TYPE_STREAM => {
                    let entry_count = read_u32(&mut r)? as usize;
                    for _ in 0..entry_count {
                        let _ = read_u64(&mut r)?;
                        let _ = read_u64(&mut r)?;
                        let field_count = read_u32(&mut r)? as usize;
                        for _ in 0..field_count {
                            skip_string(&mut r)?;
                            skip_string(&mut r)?;
                        }
                    }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unknown type byte in RDB",
                    ));
                }
            }
            continue;
        }

        let store_value = match type_byte {
            TYPE_STRING => {
                let val = read_string_bounded(&mut r)?;
                StoreValue {
                    value: FyroDB::String(SmallStr::from_string(val)),
                    expires_ms: ttl_ms,
                }
            }
            TYPE_HASH => {
                let n = read_u32(&mut r)? as usize;
                if n > 10_000_000 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "hash field count too large",
                    ));
                }
                let mut h = HashMap::with_capacity(n.min(1024));
                for _ in 0..n {
                    let field = read_string_bounded(&mut r)?;
                    let val = read_string_bounded(&mut r)?;
                    h.insert(field.into(), val.into());
                }
                StoreValue {
                    value: FyroDB::Hash(Box::new(HashInner::Full(Box::new(h)))),
                    expires_ms: ttl_ms,
                }
            }
            TYPE_LIST => {
                let n = read_u32(&mut r)? as usize;
                if n > 10_000_000 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "list length too large",
                    ));
                }
                let mut l = std::collections::VecDeque::<SmallStr>::with_capacity(n.min(1024));
                for _ in 0..n {
                    l.push_back(SmallStr::from_string(read_string_bounded(&mut r)?));
                }
                StoreValue {
                    value: FyroDB::List(Box::new(ListInner::Full(Box::new(l)))),
                    expires_ms: ttl_ms,
                }
            }
            TYPE_SET => {
                let n = read_u32(&mut r)? as usize;
                if n > 10_000_000 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "set size too large",
                    ));
                }
                let mut s = foldhash::HashSet::<String>::with_capacity(n.min(1024));
                for _ in 0..n {
                    s.insert(read_string_bounded(&mut r)?);
                }
                StoreValue {
                    value: FyroDB::Set(Box::new(SetInner::from_strings(s))),
                    expires_ms: ttl_ms,
                }
            }
            TYPE_ZSET => {
                let n = read_u32(&mut r)? as usize;
                if n > 10_000_000 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "zset size too large",
                    ));
                }
                let mut z = crate::storage::value::ZSetData::new();
                for _ in 0..n {
                    let member = read_string_bounded(&mut r)?;
                    let score_bits = read_u64(&mut r)?;
                    let score = f64::from_bits(score_bits);
                    z.insert(score, member.as_str());
                }
                StoreValue {
                    value: FyroDB::ZSet(Box::new(z)),
                    expires_ms: ttl_ms,
                }
            }
            TYPE_JSON => {
                let json_str = read_string_bounded(&mut r)?;
                StoreValue {
                    value: FyroDB::Json(SmallStr::from_string(json_str)),
                    expires_ms: ttl_ms,
                }
            }
            TYPE_STREAM => {
                let entry_count = read_u32(&mut r)? as usize;
                if entry_count > 10_000_000 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "stream too large",
                    ));
                }
                let mut stream = crate::storage::stream::StreamData::new();
                for _ in 0..entry_count {
                    let ms = read_u64(&mut r)?;
                    let seq = read_u64(&mut r)?;
                    let field_count = read_u32(&mut r)? as usize;
                    let mut fields = Vec::with_capacity(field_count);
                    for _ in 0..field_count {
                        let f = read_string_bounded(&mut r)?;
                        let v = read_string_bounded(&mut r)?;
                        fields.push((SmallStr::from_string(f), SmallStr::from_string(v)));
                    }
                    let id = crate::storage::stream::StreamId::new(ms, seq);
                    stream.entries.insert(id, fields);
                    stream.last_id = id;
                }
                StoreValue {
                    value: FyroDB::Stream(Box::new(stream)),
                    expires_ms: ttl_ms,
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unknown type byte",
                ));
            }
        };

        if store_value.expires_ms != 0 {
            store.add_ttl();
        }
        store.data.insert(key, store_value);
        count += 1;
    }

    Ok(count)
}

/// Encode/decode one StoreValue using the same bounded representation as RDB.
/// This is used by replication so all supported data types share one codec.
pub fn encode_single_value(value: &StoreValue) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    write_u64(&mut out, value.expires_ms)?;
    match &value.value {
        FyroDB::String(s) => {
            write_u8(&mut out, TYPE_STRING)?;
            write_bytes(&mut out, s.as_bytes())?;
        }
        FyroDB::Hash(h) => {
            write_u8(&mut out, TYPE_HASH)?;
            write_u32(&mut out, h.len() as u32)?;
            for (field, val) in h.iter() {
                write_bytes(&mut out, field.as_bytes())?;
                write_bytes(&mut out, val.as_bytes())?;
            }
        }
        FyroDB::List(l) => {
            write_u8(&mut out, TYPE_LIST)?;
            write_u32(&mut out, l.deque().len() as u32)?;
            for item in l.deque() {
                write_bytes(&mut out, item.as_bytes())?;
            }
        }
        FyroDB::Set(s) => {
            write_u8(&mut out, TYPE_SET)?;
            write_u32(&mut out, s.len() as u32)?;
            for member in s.iter() {
                write_bytes(&mut out, member.to_string().as_bytes())?;
            }
        }
        FyroDB::ZSet(z) => {
            write_u8(&mut out, TYPE_ZSET)?;
            write_u32(&mut out, z.len() as u32)?;
            for entry in z.iter() {
                write_bytes(&mut out, entry.member.as_bytes())?;
                write_u64(&mut out, entry.score.to_bits())?;
            }
        }
        FyroDB::Json(json) => {
            write_u8(&mut out, TYPE_JSON)?;
            write_bytes(&mut out, json.as_bytes())?;
        }
        FyroDB::Stream(stream) => {
            write_u8(&mut out, TYPE_STREAM)?;
            write_u32(&mut out, stream.entries.len() as u32)?;
            for (id, fields) in &stream.entries {
                write_u64(&mut out, id.ms)?;
                write_u64(&mut out, id.seq)?;
                write_u32(&mut out, fields.len() as u32)?;
                for (field, val) in fields {
                    write_bytes(&mut out, field.as_bytes())?;
                    write_bytes(&mut out, val.as_bytes())?;
                }
            }
        }
    }
    Ok(out)
}

pub fn decode_single_value(bytes: &[u8]) -> io::Result<StoreValue> {
    let mut reader = std::io::Cursor::new(bytes);
    let expires_ms = read_u64(&mut reader)?;
    let kind = read_u8(&mut reader)?;
    let value = match kind {
        TYPE_STRING => FyroDB::String(SmallStr::from_string(read_string_bounded(&mut reader)?)),
        TYPE_HASH => {
            let count = read_count_bounded(&mut reader)? as usize;
            let mut map = HashMap::with_capacity(count.min(1024));
            for _ in 0..count {
                map.insert(
                    read_string_bounded(&mut reader)?.into(),
                    read_string_bounded(&mut reader)?.into(),
                );
            }
            FyroDB::Hash(Box::new(HashInner::Full(Box::new(map))))
        }
        TYPE_LIST => {
            let count = read_count_bounded(&mut reader)? as usize;
            let mut list = std::collections::VecDeque::with_capacity(count.min(1024));
            for _ in 0..count {
                list.push_back(SmallStr::from_string(read_string_bounded(&mut reader)?));
            }
            FyroDB::List(Box::new(ListInner::Full(Box::new(list))))
        }
        TYPE_SET => {
            let count = read_count_bounded(&mut reader)? as usize;
            let mut set = foldhash::HashSet::with_capacity(count.min(1024));
            for _ in 0..count {
                set.insert(read_string_bounded(&mut reader)?);
            }
            FyroDB::Set(Box::new(SetInner::from_strings(set)))
        }
        TYPE_ZSET => {
            let count = read_count_bounded(&mut reader)? as usize;
            let mut zset = crate::storage::value::ZSetData::new();
            for _ in 0..count {
                let member = read_string_bounded(&mut reader)?;
                zset.insert(f64::from_bits(read_u64(&mut reader)?), &member);
            }
            FyroDB::ZSet(Box::new(zset))
        }
        TYPE_JSON => FyroDB::Json(SmallStr::from_string(read_string_bounded(&mut reader)?)),
        TYPE_STREAM => {
            let count = read_count_bounded(&mut reader)? as usize;
            let mut stream = crate::storage::stream::StreamData::new();
            for _ in 0..count {
                let id = crate::storage::stream::StreamId::new(
                    read_u64(&mut reader)?,
                    read_u64(&mut reader)?,
                );
                let fields_count = read_count_bounded(&mut reader)? as usize;
                let mut fields = Vec::with_capacity(fields_count.min(1024));
                for _ in 0..fields_count {
                    fields.push((
                        SmallStr::from_string(read_string_bounded(&mut reader)?),
                        SmallStr::from_string(read_string_bounded(&mut reader)?),
                    ));
                }
                stream.entries.insert(id, fields);
                stream.last_id = id;
            }
            FyroDB::Stream(Box::new(stream))
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown value type",
            ));
        }
    };
    if reader.position() != bytes.len() as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing value bytes",
        ));
    }
    Ok(StoreValue { value, expires_ms })
}

pub fn start_background_save(store: Arc<Store>, path: String, interval: Duration) {
    std::thread::Builder::new()
        .name("fyrodb-rdb-saver".into())
        .stack_size(64 * 1024)
        .spawn(move || {
            loop {
                std::thread::sleep(interval);
                match save(&store, &path) {
                    Ok(()) => {}
                    Err(e) => eprintln!("[rdb] background save error: {e}"),
                }
            }
        })
        .expect("failed to spawn RDB saver thread");
}

#[inline]
fn fsync_file(f: &File) -> io::Result<()> {
    let ret = unsafe { libc::fsync(f.as_raw_fd()) };
    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[inline]
fn write_u8(w: &mut impl Write, v: u8) -> io::Result<()> {
    w.write_all(&[v])
}
#[inline]
fn write_u32(w: &mut impl Write, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}
#[inline]
fn write_u64(w: &mut impl Write, v: u64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}
#[inline]
fn write_bytes(w: &mut impl Write, b: &[u8]) -> io::Result<()> {
    write_u32(w, b.len() as u32)?;
    w.write_all(b)
}
#[inline]
fn read_u8(r: &mut impl Read) -> io::Result<u8> {
    let mut b = [0u8];
    r.read_exact(&mut b)?;
    Ok(b[0])
}
#[inline]
fn read_u32(r: &mut impl Read) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}
#[inline]
fn read_u64(r: &mut impl Read) -> io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn read_string_bounded(r: &mut impl Read) -> io::Result<String> {
    let len = read_u32(r)?;
    if len > MAX_LOAD_STRING {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "string length exceeds maximum in RDB",
        ));
    }
    let len = len as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8"))
}

#[inline]
fn read_count_bounded(r: &mut impl Read) -> io::Result<u32> {
    let count = read_u32(r)?;
    if count > MAX_LOAD_ELEMENTS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "collection element count exceeds maximum in replication value",
        ));
    }
    Ok(count)
}

fn skip_string(r: &mut impl Read) -> io::Result<()> {
    let len = read_u32(r)? as u64;
    let mut remaining = len;
    let mut skip_buf = [0u8; 8192];
    while remaining > 0 {
        let chunk = remaining.min(8192) as usize;
        r.read_exact(&mut skip_buf[..chunk])?;
        remaining -= chunk as u64;
    }
    Ok(())
}
