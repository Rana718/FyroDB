//! Snapshot loader (lenient or strict truncation policy) and decoder.

use super::*;

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

        if type_byte == TYPE_INDEX {
            read_search_index(&mut r, store, version)?;
            continue;
        }
        if type_byte == TYPE_VECTOR_GRAPH {
            read_vector_graph(&mut r, store)?;
            continue;
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
                    read_stream_v1(&mut r, false)?;
                }
                TYPE_STREAM2 => {
                    read_stream(&mut r)?;
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
                let val = read_bytes_bounded(&mut r)?;
                StoreValue {
                    value: FyroDB::String(SmallStr::from_bytes(val)),
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
                    let val = read_bytes_bounded(&mut r)?;
                    h.insert(SmallStr::from_bytes(field.into_bytes()), SmallStr::from_bytes(val));
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
                let mut l = ListInner::new();
                for _ in 0..n {
                    l.push_back(&read_string_bounded(&mut r)?);
                }
                StoreValue {
                    value: FyroDB::List(Box::new(l)),
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
                    value: FyroDB::Json(json_str.into_boxed_str()),
                    expires_ms: ttl_ms,
                }
            }
            TYPE_STREAM | TYPE_STREAM2 => {
                let stream = if type_byte == TYPE_STREAM2 {
                    read_stream(&mut r)?
                } else {
                    read_stream_v1(&mut r, false)?
                };
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


pub fn decode_single_value(bytes: &[u8]) -> io::Result<StoreValue> {
    let mut reader = std::io::Cursor::new(bytes);
    let expires_ms = read_u64(&mut reader)?;
    let kind = read_u8(&mut reader)?;
    let value = match kind {
        TYPE_STRING => FyroDB::String(SmallStr::from_bytes(read_bytes_bounded(&mut reader)?)),
        TYPE_HASH => {
            let count = read_count_bounded(&mut reader)? as usize;
            let mut map = HashMap::with_capacity(count.min(1024));
            for _ in 0..count {
                map.insert(
                    SmallStr::from_string(read_string_bounded(&mut reader)?),
                    SmallStr::from_bytes(read_bytes_bounded(&mut reader)?),
                );
            }
            FyroDB::Hash(Box::new(HashInner::Full(Box::new(map))))
        }
        TYPE_LIST => {
            let count = read_count_bounded(&mut reader)? as usize;
            let mut list = ListInner::new();
            for _ in 0..count {
                list.push_back(&read_string_bounded(&mut reader)?);
            }
            FyroDB::List(Box::new(list))
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
        TYPE_JSON => FyroDB::Json(read_string_bounded(&mut reader)?.into_boxed_str()),
        TYPE_STREAM => FyroDB::Stream(Box::new(read_stream_v1(&mut reader, true)?)),
        TYPE_STREAM2 => FyroDB::Stream(Box::new(read_stream(&mut reader)?)),
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

