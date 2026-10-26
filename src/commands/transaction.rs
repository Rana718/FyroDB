use crate::storage::store::Store;
use crate::utils::resp;
use std::sync::Arc;

pub fn multi(out: &mut Vec<u8>) {
   resp::write_ok(out);
}

pub fn discard(out: &mut Vec<u8>) {
   resp::write_ok(out);
}

pub fn exec_empty(out: &mut Vec<u8>) {
   resp::write_array_header(out, 0);
}

pub fn exec(queue: &[Vec<String>], store: &Arc<Store>, out: &mut Vec<u8>) {
   resp::write_array_header(out, queue.len());
   const STACK_ARGS: usize = 8;
   for cmd_parts in queue {
      if cmd_parts.len() <= STACK_ARGS {
         let mut stack = [""; STACK_ARGS];
         for (i, s) in cmd_parts.iter().enumerate() {
            stack[i] = s.as_str();
         }
         crate::commands::execute(&stack[..cmd_parts.len()], store, out);
      } else {
         let refs: Vec<&str> = cmd_parts.iter().map(|s| s.as_str()).collect();
         crate::commands::execute(&refs, store, out);
      }
   }
}

pub fn watch(_parts: &[&str], out: &mut Vec<u8>) {
   resp::write_ok(out);
}

pub fn unwatch(out: &mut Vec<u8>) {
   resp::write_ok(out);
}
