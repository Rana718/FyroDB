use super::util::expiry_ms;
use crate::protocol;
use crate::row::{ColumnInfo, DbResultSet};

// Wire-ready encoding of a cached result: RowDescription + DataRows + CommandComplete
// stored contiguously in one allocation; split0 and split1 mark the boundaries.
pub struct EncodedResult {
   wire: Box<[u8]>,
   split0: u32,
   split1: u32,
   pub columns: Vec<ColumnInfo>,
   pub row_count: u32,
   pub expires_at_ms: u64,
   pub result: std::sync::Arc<DbResultSet>,
}

impl EncodedResult {
   pub fn from_result_set(result: &DbResultSet, ttl_ms: u64) -> Self {
      let mut wire = if result.columns.is_empty() {
         Vec::new()
      } else {
         protocol::encode_row_description(&result.columns)
      };
      let split0 = wire.len() as u32;
      let row_count = result.rows.len() as u32;
      wire.reserve(result.rows.len().saturating_mul(32));
      for row in &result.rows {
         protocol::append_data_row(&mut wire, &row.values);
      }
      let split1 = wire.len() as u32;
      protocol::put_command_complete(&mut wire, &format!("SELECT {row_count}"));
      Self {
         wire: wire.into_boxed_slice(),
         split0,
         split1,
         columns: result.columns.clone(),
         row_count,
         expires_at_ms: expiry_ms(ttl_ms),
         result: std::sync::Arc::new(result.clone()),
      }
   }

   #[inline(always)]
   pub fn is_expired(&self, now: u64) -> bool {
      self.expires_at_ms != 0 && self.expires_at_ms < now
   }

   #[inline]
   pub(crate) fn row_description(&self) -> &[u8] {
      &self.wire[..self.split0 as usize]
   }
   #[inline]
   pub fn data_rows(&self) -> &[u8] {
      &self.wire[self.split0 as usize..self.split1 as usize]
   }
   #[inline]
   pub fn command_complete(&self) -> &[u8] {
      &self.wire[self.split1 as usize..]
   }

   // Simple-query response: RowDescription, rows, CommandComplete, ReadyForQuery.
   #[inline(always)]
   pub fn write_simple_response(&self, out: &mut Vec<u8>) {
      out.reserve(self.wire.len() + 6);
      out.extend_from_slice(&self.wire);
      out.extend_from_slice(&protocol::READY_IDLE);
   }

   // Extended-protocol Execute response: rows plus CommandComplete.
   #[inline(always)]
   pub fn write_execute_response(&self, out: &mut Vec<u8>) {
      out.extend_from_slice(&self.wire[self.split0 as usize..]);
   }
}

impl std::fmt::Debug for EncodedResult {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      f.debug_struct("EncodedResult")
         .field("columns", &self.columns)
         .field("row_count", &self.row_count)
         .field("bytes", &self.wire.len())
         .finish()
   }
}
