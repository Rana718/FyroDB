use crate::storage::store::Store;
use crate::utils::resp;
use std::sync::Arc;

pub fn eval(parts: &[&str], store: &Arc<Store>, out: &mut Vec<u8>) {
   if parts.len() < 3 {
      resp::write_wrong_args(out, "eval");
      return;
   }

   if parts.len() == 3 && parts[2] == "0" {
      crate::scripting::eval_script(parts[1], &[], &[], store, out);
      return;
   }

   let numkeys: usize = match parts[2] {
      "0" => 0,
      "1" => 1,
      "2" => 2,
      "3" => 3,
      "4" => 4,
      "5" => 5,
      other => match other.parse() {
         Ok(n) => n,
         Err(_) => {
            out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
            return;
         }
      },
   };

   if numkeys > parts.len() - 3 {
      out.extend_from_slice(b"-ERR Number of keys can't be greater than number of args\r\n");
      return;
   }

   let script = parts[1];
   let keys = &parts[3..3 + numkeys];
   let argv = &parts[3 + numkeys..];

   crate::scripting::eval_script(script, keys, argv, store, out);
}

pub fn evalsha(parts: &[&str], store: &Arc<Store>, out: &mut Vec<u8>) {
   if parts.len() < 3 {
      resp::write_wrong_args(out, "evalsha");
      return;
   }

   if parts.len() == 3 && parts[2] == "0" {
      if !crate::scripting::eval_sha(parts[1], &[], &[], store, out) {
         out.extend_from_slice(b"-NOSCRIPT No matching script. Please use EVAL.\r\n");
      }
      return;
   }

   let numkeys: usize = match parts[2] {
      "0" => 0,
      "1" => 1,
      "2" => 2,
      "3" => 3,
      "4" => 4,
      "5" => 5,
      other => match other.parse() {
         Ok(n) => n,
         Err(_) => {
            out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
            return;
         }
      },
   };

   if numkeys > parts.len() - 3 {
      out.extend_from_slice(b"-ERR Number of keys can't be greater than number of args\r\n");
      return;
   }

   let sha1 = parts[1];
   let keys = &parts[3..3 + numkeys];
   let argv = &parts[3 + numkeys..];

   if !crate::scripting::eval_sha(sha1, keys, argv, store, out) {
      out.extend_from_slice(b"-NOSCRIPT No matching script. Please use EVAL.\r\n");
   }
}

pub fn script_cmd(parts: &[&str], store: &Arc<Store>, out: &mut Vec<u8>) {
   if parts.len() < 2 {
      resp::write_wrong_args(out, "script");
      return;
   }

   let sub = parts[1];
   if sub.eq_ignore_ascii_case("LOAD") {
      if parts.len() != 3 {
         resp::write_wrong_args(out, "script|load");
         return;
      }
      let sha1 = store.load_script(parts[2]);
      resp::write_bulk(out, &sha1);
   } else if sub.eq_ignore_ascii_case("EXISTS") {
      if parts.len() < 3 {
         resp::write_wrong_args(out, "script|exists");
         return;
      }
      resp::write_array_header(out, parts.len() - 2);
      for sha in &parts[2..] {
         let exists = store.script_exists(sha);
         resp::write_integer(out, if exists { 1 } else { 0 });
      }
   } else if sub.eq_ignore_ascii_case("FLUSH") {
      store.flush_scripts();
      resp::write_ok(out);
   } else if sub.eq_ignore_ascii_case("KILL") {
      out.extend_from_slice(b"-NOTBUSY No scripts in execution.\r\n");
   } else {
      out.extend_from_slice(b"-ERR Unknown SCRIPT subcommand or wrong # of args\r\n");
   }
}
