use super::util::expiry_ms;
use crate::protocol;
use crate::row::{ColumnInfo, DbResultSet};

// Wire-ready encoding of a cached result: RowDescription + DataRows + CommandComplete
// stored contiguously in one allocation; split0 and split1 mark the boundaries.
pub struct EncodedResult {
   wire: Vec<u8>,
   split0: u32,
   split1: u32,
   pub columns: Vec<ColumnInfo>,
   pub row_count: u32,
   pub expires_at_ms: u64,
}

impl EncodedResult {
   pub fn from_result_set(result: &DbResultSet, ttl_ms: u64) -> Self {
      let (row_description, data_rows, row_count) =
         protocol::encode_result(&result.columns, &result.rows);
      let cc = protocol::encode_command_complete(&format!("SELECT {row_count}"));
      let split0 = row_description.len() as u32;
      let split1 = split0 + data_rows.len() as u32;
      let mut wire = Vec::with_capacity(split1 as usize + cc.len());
      wire.extend_from_slice(&row_description);
      wire.extend_from_slice(&data_rows);
      wire.extend_from_slice(&cc);
      Self {
         wire,
         split0,
         split1,
         columns: result.columns.clone(),
         row_count,
         expires_at_ms: expiry_ms(ttl_ms),
      }
   }

   #[inline]
   pub(crate) fn row_description(&self) -> &[u8] {
      &self.wire[..self.split0 as usize]
   }
   #[inline]
   pub(crate) fn data_rows(&self) -> &[u8] {
      &self.wire[self.split0 as usize..self.split1 as usize]
   }
   #[inline]
   pub(crate) fn command_complete(&self) -> &[u8] {
      &self.wire[self.split1 as usize..]
   }

   // Simple-query response: RowDescription, rows, CommandComplete, ReadyForQuery.
   pub fn write_simple_response(&self, out: &mut Vec<u8>) {
      out.reserve(self.wire.len() + protocol::READY_IDLE.len());
      out.extend_from_slice(&self.wire);
      out.extend_from_slice(&protocol::READY_IDLE);
   }

   // Extended-protocol Execute response: rows plus CommandComplete.
   pub fn write_execute_response(&self, out: &mut Vec<u8>) {
      let payload = self.data_rows().len() + self.command_complete().len();
      out.reserve(payload);
      out.extend_from_slice(self.data_rows());
      out.extend_from_slice(self.command_complete());
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
