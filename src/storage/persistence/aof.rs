use crate::storage::rdb::{decode_single_value, encode_single_value};

pub fn encode_live(v: &StoreValue) -> Option<Vec<u8>> {
    if v.is_expired() {
        None
    } else {
        encode_single_value(v).ok()
    }
}
use crate::storage::store::Store;
use crate::storage::value::StoreValue;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

const KIND_SET: u8 = 0;
const KIND_DEL: u8 = 1;
const KIND_FLUSH: u8 = 2;
const FLUSH_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fsync {
    Always,
    EverySec,
    No,
}

struct Writer {
    id: usize,
    epoch: u64,
    file: Option<File>,
    buf: Vec<u8>,
}

pub struct Aof {
    dir: PathBuf,
    fsync: Fsync,
    seq: AtomicU64,
    epoch: AtomicU64,
    next_id: AtomicUsize,
    writers: Mutex<Vec<Arc<Mutex<Writer>>>>,
    failed: AtomicBool,
}

static AOF: OnceLock<Aof> = OnceLock::new();

thread_local! {
    static LOCAL: RefCell<Option<Arc<Mutex<Writer>>>> = const { RefCell::new(None) };
}

#[inline(always)]
pub fn enabled() -> bool {
    AOF.get().is_some()
}

pub fn get() -> Option<&'static Aof> {
    AOF.get()
}

fn file_name(epoch: u64, id: usize) -> String {
    format!("aof-{epoch:08}-{id:04}.log")
}

fn parse_name(name: &str) -> Option<(u64, String)> {
    let rest = name.strip_prefix("aof-")?.strip_suffix(".log")?;
    let (epoch, id) = rest.split_once('-')?;
    Some((epoch.parse().ok()?, id.to_string()))
}

fn encode(buf: &mut Vec<u8>, seq: u64, key: &str, value: Option<&[u8]>) {
    encode_kind(buf, seq, if value.is_some() { KIND_SET } else { KIND_DEL }, key, value);
}

fn encode_kind(buf: &mut Vec<u8>, seq: u64, kind: u8, key: &str, value: Option<&[u8]>) {
    let body = 8 + 1 + 4 + key.len() + value.map_or(0, <[u8]>::len);
    buf.extend_from_slice(&(body as u32).to_le_bytes());
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.push(kind);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key.as_bytes());
    if let Some(v) = value {
        buf.extend_from_slice(v);
    }
}

struct Record {
    seq: u64,
    kind: u8,
    key: String,
    value: Option<Vec<u8>>,
}

fn read_record(r: &mut impl Read) -> io::Result<Option<Record>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(len) as usize;
    if !(13..=(512 << 20)).contains(&len) {
        return Ok(None);
    }
    let mut body = vec![0u8; len];
    if r.read_exact(&mut body).is_err() {
        return Ok(None);
    }
    let seq = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let kind = body[8];
    let klen = u32::from_le_bytes(body[9..13].try_into().unwrap()) as usize;
    if 13 + klen > len {
        return Ok(None);
    }
    let Ok(key) = String::from_utf8(body[13..13 + klen].to_vec()) else {
        return Ok(None);
    };
    let value = match kind {
        KIND_SET => Some(body[13 + klen..].to_vec()),
        KIND_DEL | KIND_FLUSH => None,
        _ => return Ok(None),
    };
    Ok(Some(Record { seq, kind, key, value }))
}

struct Cursor {
    reader: BufReader<File>,
    head: Option<Record>,
}

fn open_all(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    let Ok(rd) = fs::read_dir(dir) else {
        return Ok(out);
    };
    for e in rd.flatten() {
        let name = e.file_name();
        if let Some((epoch, _)) = name.to_str().and_then(parse_name) {
            out.push((epoch, e.path()));
        }
    }
    out.sort();
    Ok(out)
}

fn merged(paths: &[PathBuf], mut f: impl FnMut(Record) -> io::Result<()>) -> io::Result<u64> {
    let mut cursors = Vec::new();
    for p in paths {
        let mut reader = BufReader::with_capacity(1 << 16, File::open(p)?);
        let head = read_record(&mut reader)?;
        cursors.push(Cursor { reader, head });
    }
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> = cursors
        .iter()
        .enumerate()
        .filter_map(|(i, c)| c.head.as_ref().map(|r| Reverse((r.seq, i))))
        .collect();
    let mut max_seq = 0;
    while let Some(Reverse((seq, i))) = heap.pop() {
        let c = &mut cursors[i];
        let rec = c.head.take().expect("head");
        max_seq = max_seq.max(seq);
        f(rec)?;
        c.head = read_record(&mut c.reader)?;
        if let Some(r) = &c.head {
            heap.push(Reverse((r.seq, i)));
        }
    }
    Ok(max_seq)
}

fn read_base(dir: &Path) -> u64 {
    fs::read(dir.join("aof-base"))
        .ok()
        .and_then(|b| b.get(..8).map(|b| u64::from_le_bytes(b.try_into().unwrap())))
        .unwrap_or(0)
}

fn write_base(dir: &Path, base: u64) -> io::Result<()> {
    let tmp = dir.join("aof-base.tmp");
    let mut f = File::create(&tmp)?;
    f.write_all(&base.to_le_bytes())?;
    f.sync_data()?;
    fs::rename(tmp, dir.join("aof-base"))
}

pub fn replay(store: &Store, dir: &Path) -> io::Result<(u64, usize)> {
    let files: Vec<PathBuf> = open_all(dir)?.into_iter().map(|(_, p)| p).collect();
    let base = read_base(dir);
    let mut applied = 0usize;
    let max_seq = merged(&files, |rec| {
        if rec.seq < base {
            return Ok(());
        }
        if rec.kind == KIND_FLUSH {
            store.data.clear();
            applied += 1;
            return Ok(());
        }
        match rec.value {
            Some(bytes) => {
                if let Ok(v) = decode_single_value(&bytes) {
                    store.data.insert(rec.key, v);
                }
            }
            None => {
                store.data.remove(&rec.key);
            }
        }
        applied += 1;
        Ok(())
    })?;
    Ok((max_seq, applied))
}

pub fn open(store: &Store, dir: &Path, fsync: Fsync) -> io::Result<usize> {
    fs::create_dir_all(dir)?;
    let (max_seq, applied) = replay(store, dir)?;
    let max_seq = max_seq.max(read_base(dir));
    let epoch = open_all(dir)?.last().map_or(0, |(g, _)| *g) + 1;
    let aof = Aof {
        dir: dir.to_path_buf(),
        fsync,
        seq: AtomicU64::new(max_seq + 1),
        epoch: AtomicU64::new(epoch),
        next_id: AtomicUsize::new(0),
        writers: Mutex::new(Vec::new()),
        failed: AtomicBool::new(false),
    };
    if AOF.set(aof).is_err() {
        return Err(io::Error::other("AOF already open"));
    }
    if fsync == Fsync::EverySec {
        std::thread::Builder::new()
            .name("fyrodb-aof".into())
            .spawn(|| {
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                    if let Some(a) = get() {
                        a.sync_all();
                    }
                }
            })?;
    }
    Ok(applied)
}

impl Aof {
    fn local(&self) -> Arc<Mutex<Writer>> {
        LOCAL.with(|l| {
            let mut l = l.borrow_mut();
            if let Some(w) = l.as_ref() {
                return Arc::clone(w);
            }
            let w = Arc::new(Mutex::new(Writer {
                id: self.next_id.fetch_add(1, Ordering::Relaxed),
                epoch: 0,
                file: None,
                buf: Vec::with_capacity(FLUSH_BYTES),
            }));
            self.writers.lock().push(Arc::clone(&w));
            *l = Some(Arc::clone(&w));
            w
        })
    }

    fn write_out(&self, w: &mut Writer) {
        if w.buf.is_empty() {
            return;
        }
        let epoch = self.epoch.load(Ordering::Acquire);
        if w.file.is_none() || w.epoch != epoch {
            let path = self.dir.join(file_name(epoch, w.id));
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(f) => {
                    w.file = Some(f);
                    w.epoch = epoch;
                }
                Err(e) => {
                    if !self.failed.swap(true, Ordering::Relaxed) {
                        eprintln!("[aof] open {}: {e}", path.display());
                    }
                    return;
                }
            }
        }
        let f = w.file.as_mut().expect("file");
        if let Err(e) = f.write_all(&w.buf)
            && !self.failed.swap(true, Ordering::Relaxed)
        {
            eprintln!("[aof] write: {e}");
        }
        w.buf.clear();
        if self.fsync == Fsync::Always {
            let _ = f.sync_data();
        }
    }

    pub fn record(&self, key: &str, read: impl FnOnce() -> Option<Vec<u8>>) {
        let seq = self.seq.fetch_add(1, Ordering::AcqRel);
        let encoded = read();
        let w = self.local();
        let mut w = w.lock();
        encode(&mut w.buf, seq, key, encoded.as_deref());
        if w.buf.len() >= FLUSH_BYTES || self.fsync == Fsync::Always {
            self.write_out(&mut w);
        }
    }

    pub fn record_flush(&self) {
        let seq = self.seq.fetch_add(1, Ordering::AcqRel);
        let w = self.local();
        let mut w = w.lock();
        encode_kind(&mut w.buf, seq, KIND_FLUSH, "", None);
        self.write_out(&mut w);
    }

    pub fn flush_local(&self) {
        LOCAL.with(|l| {
            if let Some(w) = l.borrow().as_ref() {
                let mut w = w.lock();
                self.write_out(&mut w);
            }
        });
    }

    pub fn sync_all(&self) {
        let writers: Vec<_> = self.writers.lock().clone();
        for w in writers {
            let mut w = w.lock();
            self.write_out(&mut w);
            if let Some(f) = w.file.as_ref() {
                let _ = f.sync_data();
            }
        }
    }

    pub fn rotate(&self) -> (u64, u64) {
        let old = self.epoch.fetch_add(1, Ordering::AcqRel);
        (old, self.seq.load(Ordering::Acquire))
    }

    pub fn compact_before(&self, epoch: u64, keep_from: u64) -> io::Result<()> {
        write_base(&self.dir, keep_from)?;
        let writers: Vec<_> = self.writers.lock().clone();
        for w in &writers {
            let mut w = w.lock();
            self.write_out(&mut w);
        }
        let old: Vec<PathBuf> = open_all(&self.dir)?
            .into_iter()
            .filter(|(g, _)| *g <= epoch)
            .map(|(_, p)| p)
            .collect();
        if old.is_empty() {
            return Ok(());
        }
        let mut carry = Vec::new();
        merged(&old, |rec| {
            if rec.seq >= keep_from {
                encode_kind(&mut carry, rec.seq, rec.kind, &rec.key, rec.value.as_deref());
            }
            Ok(())
        })?;
        if !carry.is_empty() {
            let path = self.dir.join(format!("aof-{:08}-carry.log", self.epoch.load(Ordering::Acquire)));
            let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
            f.write_all(&carry)?;
            f.sync_data()?;
        }
        for p in old {
            fs::remove_file(p)?;
        }
        Ok(())
    }
}

pub fn flush_local() {
    if let Some(a) = get() {
        a.flush_local();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_and_torn_tail_is_ignored() {
        let mut buf = Vec::new();
        encode(&mut buf, 7, "k", Some(b"abc"));
        encode(&mut buf, 9, "gone", None);
        encode(&mut buf, 11, "t", Some(b"xyz"));
        buf.truncate(buf.len() - 2);
        let mut r = io::Cursor::new(buf);
        let a = read_record(&mut r).unwrap().unwrap();
        assert_eq!((a.seq, a.key.as_str(), a.value.as_deref()), (7, "k", Some(&b"abc"[..])));
        let b = read_record(&mut r).unwrap().unwrap();
        assert_eq!((b.seq, b.key.as_str(), b.value), (9, "gone", None));
        assert!(read_record(&mut r).unwrap().is_none());
    }

    #[test]
    fn merge_orders_by_sequence_across_files() {
        let dir = std::env::temp_dir().join(format!("fyro-aof-merge-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let mut a = Vec::new();
        encode(&mut a, 1, "x", Some(b"1"));
        encode(&mut a, 4, "x", Some(b"4"));
        let mut b = Vec::new();
        encode(&mut b, 2, "x", Some(b"2"));
        encode(&mut b, 3, "y", None);
        fs::write(dir.join(file_name(1, 0)), a).unwrap();
        fs::write(dir.join(file_name(1, 1)), b).unwrap();
        let files: Vec<PathBuf> = open_all(&dir).unwrap().into_iter().map(|(_, p)| p).collect();
        let mut seen = Vec::new();
        let max = merged(&files, |r| {
            seen.push(r.seq);
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![1, 2, 3, 4]);
        assert_eq!(max, 4);
        let _ = fs::remove_dir_all(&dir);
    }
}
