//! Snapshot writer, single-value encoder, periodic background save.

use super::*;

fn write_record(w: &mut impl Write, key: &str, val: &StoreValue) -> io::Result<()> {
    let ttl_ms = val.expires_ms;
    let head = |w: &mut dyn Write, ty: u8| -> io::Result<()> {
        w.write_all(&[ty])?;
        w.write_all(&ttl_ms.to_le_bytes())?;
        w.write_all(&(key.len() as u32).to_le_bytes())?;
        w.write_all(key.as_bytes())
    };
    match &val.value {
        FyroDB::String(s) => {
            head(w, TYPE_STRING)?;
            write_bytes(w, s.as_bytes())
        }
        FyroDB::Hash(h) => {
            head(w, TYPE_HASH)?;
            write_u32(w, h.len() as u32)?;
            for (f, v) in h.iter() {
                write_bytes(w, f.as_bytes())?;
                write_bytes(w, v.as_bytes())?;
            }
            Ok(())
        }
        FyroDB::List(l) => {
            head(w, TYPE_LIST)?;
            write_u32(w, l.len() as u32)?;
            for item in l.iter() {
                write_bytes(w, item.as_bytes())?;
            }
            Ok(())
        }
        FyroDB::Set(s) => {
            head(w, TYPE_SET)?;
            write_u32(w, s.len() as u32)?;
            for member in s.iter() {
                write_bytes(w, member.to_string().as_bytes())?;
            }
            Ok(())
        }
        FyroDB::ZSet(z) => {
            head(w, TYPE_ZSET)?;
            write_u32(w, z.len() as u32)?;
            for entry in z.iter() {
                write_bytes(w, entry.member.as_bytes())?;
                write_u64(w, entry.score.to_bits())?;
            }
            Ok(())
        }
        FyroDB::Json(j) => {
            head(w, TYPE_JSON)?;
            write_bytes(w, j.as_bytes())
        }
        FyroDB::Stream(s) => {
            head(w, TYPE_STREAM2)?;
            write_stream(w, s)
        }
    }
}

static SAVE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
static AOF_MARK: parking_lot::Mutex<Option<(u64, u64)>> = parking_lot::Mutex::new(None);

struct SnapshotGuard<'a>(&'a Store);

impl Drop for SnapshotGuard<'_> {
    fn drop(&mut self) {
        self.0.data.end_snapshot();
    }
}

fn write_keyspace(w: &mut impl Write, store: &Store) -> io::Result<()> {
    let side: std::sync::Arc<parking_lot::Mutex<Vec<u8>>> = Default::default();
    let sink_buf = std::sync::Arc::clone(&side);
    let sink: customhash::SnapSink<StoreValue> = std::sync::Arc::new(move |k: &str, v: &StoreValue| {
        if !v.is_expired() {
            let _ = write_record(&mut *sink_buf.lock(), k, v);
        }
    });
    if !store.data.begin_snapshot(sink) {
        return Err(io::Error::other("snapshot already in progress"));
    }
    let guard = SnapshotGuard(store);
    *AOF_MARK.lock() = crate::storage::persistence::aof::get().map(|a| a.rotate());
    for shard in 0..store.data.shard_count() {
        let mut result = Ok(());
        store.data.snapshot_shard(shard, |k, v| {
            if result.is_ok() && !v.is_expired() {
                result = write_record(w, k, v);
            }
        });
        result?;
        let pending = std::mem::take(&mut *side.lock());
        w.write_all(&pending)?;
    }
    drop(guard);
    let pending = std::mem::take(&mut *side.lock());
    w.write_all(&pending)
}

pub fn save(store: &Store, path: &str) -> io::Result<()> {
    let _serial = SAVE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let tmp = format!("{path}.tmp");
    {
        let f = File::create(&tmp)?;
        let mut w = BufWriter::with_capacity(1 << 20, f);
        w.write_all(MAGIC)?;
        w.write_all(&[VERSION])?;
        write_keyspace(&mut w, store)?;
        write_search_indexes(&mut w, store)?;
        write_vector_graphs(&mut w, store)?;
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
    if let (Some(aof), Some((epoch, keep_from))) =
        (crate::storage::persistence::aof::get(), AOF_MARK.lock().take())
        && let Err(e) = aof.compact_before(epoch, keep_from)
    {
        eprintln!("[aof] rewrite after snapshot failed: {e}");
    }
    Ok(())
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
            write_u32(&mut out, l.len() as u32)?;
            for item in l.iter() {
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
            write_u8(&mut out, TYPE_STREAM2)?;
            write_stream(&mut out, stream)?;
        }
    }
    Ok(out)
}

pub fn background_save_loop(store: &Store, path: &str, interval: Duration) {
    loop {
        std::thread::sleep(interval);
        match save(store, path) {
            Ok(()) => {}
            Err(e) => eprintln!("[rdb] background save error: {e}"),
        }
        if let Some(log) = store.replication_coordinator() {
            log.flush_journal();
        }
    }
}

