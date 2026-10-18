use super::*;

pub(super) const MAGIC: &[u8; 4] = b"FLDB";
/// v3: index records carry M / EF_CONSTRUCTION / vector type, and
/// `TYPE_VECTOR_GRAPH` records persist HNSW links.
pub(super) const VERSION: u8 = 3;
pub(super) const TYPE_STRING: u8 = 0;
pub(super) const TYPE_HASH: u8 = 1;
pub(super) const TYPE_LIST: u8 = 2;
pub(super) const TYPE_SET: u8 = 3;
pub(super) const TYPE_ZSET: u8 = 4;
pub(super) const TYPE_JSON: u8 = 5;
pub(super) const TYPE_STREAM: u8 = 6;
pub(super) const TYPE_INDEX: u8 = 7;
pub(super) const TYPE_VECTOR_GRAPH: u8 = 8;
pub(super) const TYPE_STREAM2: u8 = 9;
pub(super) const TYPE_EOF: u8 = 0xFF;

pub(super) const MAX_LOAD_STRING: u32 = 512 * 1024 * 1024;
pub(super) const MAX_LOAD_ELEMENTS: u32 = 4 * 1024 * 1024;
pub(super) const REPL_MAGIC: &[u8; 4] = b"FLRP";
pub(super) const REPL_VERSION: u8 = 1;
pub(super) const CLUSTER_MAGIC: &[u8; 4] = b"FLCL";
pub(super) const CLUSTER_VERSION: u8 = 1;

#[inline]
pub(super) fn fsync_file(f: &File) -> io::Result<()> {
   let ret = unsafe { libc::fsync(f.as_raw_fd()) };
   if ret == 0 {
      Ok(())
   } else {
      Err(io::Error::last_os_error())
   }
}

#[inline]
pub(super) fn write_u8(w: &mut impl Write, v: u8) -> io::Result<()> {
   w.write_all(&[v])
}
#[inline]
pub(super) fn write_u32(w: &mut impl Write, v: u32) -> io::Result<()> {
   w.write_all(&v.to_le_bytes())
}
#[inline]
pub(super) fn write_u64(w: &mut impl Write, v: u64) -> io::Result<()> {
   w.write_all(&v.to_le_bytes())
}
#[inline]
pub(super) fn write_bytes(w: &mut impl Write, b: &[u8]) -> io::Result<()> {
   write_u32(w, b.len() as u32)?;
   w.write_all(b)
}
#[inline]
pub(super) fn read_u8(r: &mut impl Read) -> io::Result<u8> {
   let mut b = [0u8];
   r.read_exact(&mut b)?;
   Ok(b[0])
}
#[inline]
pub(super) fn read_u32(r: &mut impl Read) -> io::Result<u32> {
   let mut b = [0u8; 4];
   r.read_exact(&mut b)?;
   Ok(u32::from_le_bytes(b))
}
#[inline]
pub(super) fn read_u64(r: &mut impl Read) -> io::Result<u64> {
   let mut b = [0u8; 8];
   r.read_exact(&mut b)?;
   Ok(u64::from_le_bytes(b))
}

pub(super) fn read_string_bounded(r: &mut impl Read) -> io::Result<String> {
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

pub(super) fn read_bytes_bounded(r: &mut impl Read) -> io::Result<Vec<u8>> {
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
   Ok(buf)
}

#[inline]
pub(super) fn read_count_bounded(r: &mut impl Read) -> io::Result<u32> {
   let count = read_u32(r)?;
   if count > MAX_LOAD_ELEMENTS {
      return Err(io::Error::new(
         io::ErrorKind::InvalidData,
         "collection element count exceeds maximum in replication value",
      ));
   }
   Ok(count)
}

pub(super) fn skip_string(r: &mut impl Read) -> io::Result<()> {
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
