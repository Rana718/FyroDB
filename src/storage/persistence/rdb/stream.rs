use super::*;
use crate::storage::stream::{ConsumerData, ConsumerGroup, PelEntry, StreamData, StreamId};

fn bad(msg: &'static str) -> io::Error {
   io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn write_id(w: &mut impl Write, id: StreamId) -> io::Result<()> {
   write_u64(w, id.ms)?;
   write_u64(w, id.seq)
}

fn read_id(r: &mut impl Read) -> io::Result<StreamId> {
   Ok(StreamId::new(read_u64(r)?, read_u64(r)?))
}

pub(super) fn write_stream(w: &mut impl Write, s: &StreamData) -> io::Result<()> {
   write_id(w, s.last_id)?;
   write_id(w, s.max_deleted)?;
   write_u64(w, s.entries_added)?;
   write_u32(w, s.node_count() as u32)?;
   for (master, buf, count, live) in s.raw_nodes() {
      write_id(w, master)?;
      write_u32(w, count)?;
      write_u32(w, live)?;
      write_bytes(w, buf)?;
   }
   write_u32(w, s.groups.len() as u32)?;
   for (name, g) in &s.groups {
      write_bytes(w, name.as_bytes())?;
      write_id(w, g.last_delivered)?;
      write_u64(w, g.entries_read.unwrap_or(u64::MAX))?;
      write_u32(w, g.consumers.len() as u32)?;
      for (cname, c) in &g.consumers {
         write_bytes(w, cname.as_bytes())?;
         write_u64(w, c.seen_time)?;
         write_u64(w, c.active_time)?;
      }
      write_u32(w, g.pel.len() as u32)?;
      for (id, p) in &g.pel {
         write_id(w, *id)?;
         write_bytes(w, p.consumer.as_bytes())?;
         write_u64(w, p.delivery_time)?;
         write_u64(w, p.delivery_count)?;
      }
   }
   Ok(())
}

pub(super) fn read_stream(r: &mut impl Read) -> io::Result<StreamData> {
   let mut s = StreamData::new();
   let last = read_id(r)?;
   s.max_deleted = read_id(r)?;
   s.entries_added = read_u64(r)?;
   let nodes = read_count_bounded(r)?;
   for _ in 0..nodes {
      let master = read_id(r)?;
      let count = read_u32(r)?;
      let live = read_u32(r)?;
      let buf = read_bytes_bounded(r)?;
      if !s.push_raw_node(master, buf, count, live) {
         return Err(bad("corrupt stream node"));
      }
   }
   if last < s.last_id {
      return Err(bad("stream last id behind entries"));
   }
   s.last_id = last;
   let groups = read_count_bounded(r)?;
   for _ in 0..groups {
      let name = read_string_bounded(r)?;
      let mut g = ConsumerGroup::new(read_id(r)?);
      let er = read_u64(r)?;
      g.entries_read = (er != u64::MAX).then_some(er);
      let consumers = read_count_bounded(r)?;
      for _ in 0..consumers {
         let cname = read_string_bounded(r)?;
         let seen_time = read_u64(r)?;
         let active_time = read_u64(r)?;
         g.consumers.insert(
            SmallStr::from_string(cname),
            ConsumerData {
               pel: Default::default(),
               seen_time,
               active_time,
            },
         );
      }
      let pel = read_count_bounded(r)?;
      for _ in 0..pel {
         let id = read_id(r)?;
         let consumer = read_string_bounded(r)?;
         let delivery_time = read_u64(r)?;
         let delivery_count = read_u64(r)?;
         let c = g
            .consumers
            .get_mut(consumer.as_str())
            .ok_or_else(|| bad("pending entry for unknown consumer"))?;
         c.pel.insert(id);
         g.pel.insert(
            id,
            PelEntry {
               consumer: SmallStr::from_string(consumer),
               delivery_time,
               delivery_count,
            },
         );
      }
      s.groups.insert(SmallStr::from_string(name), g);
   }
   Ok(s)
}

pub(super) fn read_stream_v1(r: &mut impl Read, bounded: bool) -> io::Result<StreamData> {
   let count = if bounded {
      read_count_bounded(r)?
   } else {
      read_u32(r)?
   } as usize;
   if count > 10_000_000 {
      return Err(bad("stream too large"));
   }
   let mut s = StreamData::new();
   for _ in 0..count {
      let id = read_id(r)?;
      let n = if bounded {
         read_count_bounded(r)?
      } else {
         read_u32(r)?
      } as usize;
      let mut fields = Vec::with_capacity(n.min(1024));
      for _ in 0..n {
         fields.push((read_string_bounded(r)?, read_string_bounded(r)?));
      }
      if id <= s.last_id && !s.is_empty() {
         return Err(bad("stream ids out of order"));
      }
      s.append(id, &fields);
   }
   Ok(s)
}

#[cfg(test)]
mod tests {
   use super::*;
   use crate::storage::stream::{ClaimOpts, IdSpec};

   #[test]
   fn stream_with_groups_round_trips() {
      let mut s = StreamData::new();
      for i in 1..=300u64 {
         s.add(IdSpec::Exact(StreamId::new(i, 0)), &[("k", i.to_string())])
            .unwrap();
      }
      s.delete(StreamId::new(10, 0));
      s.group_create("g", Some(StreamId::MIN)).unwrap();
      s.read_group_new("g", "c1", 5, false).unwrap();
      let o = ClaimOpts {
         idle: None,
         time: None,
         retry_count: Some(9),
         force: false,
         justid: true,
         last_id: None,
      };
      s.claim("g", "c2", 0, &[StreamId::new(2, 0)], &o).unwrap();
      let mut buf = Vec::new();
      write_stream(&mut buf, &s).unwrap();
      let t = read_stream(&mut io::Cursor::new(&buf)).unwrap();
      assert_eq!(t.len(), 299);
      assert_eq!(t.last_id, s.last_id);
      assert_eq!(t.max_deleted, StreamId::new(10, 0));
      assert_eq!(
         t.range(StreamId::MIN, StreamId::MAX, usize::MAX),
         s.range(StreamId::MIN, StreamId::MAX, usize::MAX)
      );
      assert_eq!(
         t.pending_summary("g").unwrap(),
         s.pending_summary("g").unwrap()
      );
      assert_eq!(t.groups["g"].pel[&StreamId::new(2, 0)].delivery_count, 9);
      buf.truncate(buf.len() - 3);
      assert!(read_stream(&mut io::Cursor::new(&buf)).is_err());
   }
}
