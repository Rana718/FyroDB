use super::small_str::SmallStr;

#[derive(Clone, Debug)]
pub enum JsonValue {
   Null,
   Bool(bool),
   Number(f64),
   String(SmallStr),
   Array(Vec<JsonValue>),
   Object(Vec<(SmallStr, JsonValue)>),
}

#[inline]
fn write_json_string(out: &mut String, value: &str) {
   out.push('"');
   let mut start = 0;
   for (idx, byte) in value.bytes().enumerate() {
      let escaped = match byte {
         b'\\' | b'"' => Some(byte),
         _ => None,
      };
      if let Some(byte) = escaped {
         out.push_str(&value[start..idx]);
         out.push('\\');
         out.push(byte as char);
         start = idx + 1;
      }
   }
   out.push_str(&value[start..]);
   out.push('"');
}

impl JsonValue {
   pub fn type_name(&self) -> &'static str {
      match self {
         Self::Null => "null",
         Self::Bool(_) => "boolean",
         Self::Number(_) => "number",
         Self::String(_) => "string",
         Self::Array(_) => "array",
         Self::Object(_) => "object",
      }
   }

   pub fn to_resp_string(&self) -> String {
      let mut out = String::new();
      self.write_json(&mut out);
      out
   }

   fn write_json(&self, out: &mut String) {
      match self {
         Self::Null => out.push_str("null"),
         Self::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
         Self::Number(n) => {
            if *n == (*n as i64) as f64 {
               use std::fmt::Write;
               let _ = write!(out, "{}", *n as i64);
            } else {
               use std::fmt::Write;
               let _ = write!(out, "{}", n);
            }
         }
         Self::String(s) => write_json_string(out, s),
         Self::Array(arr) => {
            out.push('[');
            for (i, value) in arr.iter().enumerate() {
               if i != 0 {
                  out.push(',');
               }
               value.write_json(out);
            }
            out.push(']');
         }
         Self::Object(obj) => {
            out.push('{');
            for (i, (key, value)) in obj.iter().enumerate() {
               if i != 0 {
                  out.push(',');
               }
               write_json_string(out, key);
               out.push(':');
               value.write_json(out);
            }
            out.push('}');
         }
      }
   }

   pub fn get_path(&self, path: &str) -> Option<&JsonValue> {
      if path == "." || path == "$" || path.is_empty() {
         return Some(self);
      }
      let normalized = path.strip_prefix("$.").or_else(|| path.strip_prefix('.'))?;
      let mut current = self;
      for part in normalized.split('.') {
         if let Some(idx_str) = part.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            if let Ok(idx) = idx_str.parse::<usize>() {
               match current {
                  Self::Array(arr) => current = arr.get(idx)?,
                  _ => return None,
               }
            } else {
               return None;
            }
         } else {
            match current {
               Self::Object(obj) => {
                  current = obj.iter().find(|(k, _)| k == part).map(|(_, v)| v)?;
               }
               _ => return None,
            }
         }
      }
      Some(current)
   }

   pub fn get_path_mut(&mut self, path: &str) -> Option<&mut JsonValue> {
      if path == "." || path == "$" || path.is_empty() {
         return Some(self);
      }
      let normalized = path.strip_prefix("$.").or_else(|| path.strip_prefix('.'))?;
      let mut current = self;
      for part in normalized.split('.') {
         if let Some(idx_str) = part.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            if let Ok(idx) = idx_str.parse::<usize>() {
               match current {
                  Self::Array(arr) => current = arr.get_mut(idx)?,
                  _ => return None,
               }
            } else {
               return None;
            }
         } else {
            match current {
               Self::Object(obj) => {
                  current = obj.iter_mut().find(|(k, _)| k == part).map(|(_, v)| v)?;
               }
               _ => return None,
            }
         }
      }
      Some(current)
   }

   pub fn set_path(&mut self, path: &str, value: JsonValue) -> bool {
      if path == "." || path == "$" || path.is_empty() {
         *self = value;
         return true;
      }
      let normalized = match path.strip_prefix("$.").or_else(|| path.strip_prefix('.')) {
         Some(n) => n,
         None => return false,
      };
      let parts: Vec<&str> = normalized.split('.').collect();
      if parts.is_empty() {
         return false;
      }
      let (parent_parts, last) = parts.split_at(parts.len() - 1);
      let last = last[0];

      let mut current = self;
      for part in parent_parts {
         if let Some(idx_str) = part.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            if let Ok(idx) = idx_str.parse::<usize>() {
               match current {
                  Self::Array(arr) => match arr.get_mut(idx) {
                     Some(v) => current = v,
                     None => return false,
                  },
                  _ => return false,
               }
            } else {
               return false;
            }
         } else {
            match current {
               Self::Object(obj) => match obj.iter_mut().find(|(k, _)| k == part).map(|(_, v)| v) {
                  Some(v) => current = v,
                  None => return false,
               },
               _ => return false,
            }
         }
      }

      if let Some(idx_str) = last.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
         if let Ok(idx) = idx_str.parse::<usize>() {
            match current {
               Self::Array(arr) if idx < arr.len() => {
                  arr[idx] = value;
                  true
               }
               _ => false,
            }
         } else {
            false
         }
      } else {
         match current {
            Self::Object(obj) => {
               if let Some((_, v)) = obj.iter_mut().find(|(k, _)| k == last) {
                  *v = value;
               } else {
                  obj.push((SmallStr::new(last), value));
               }
               true
            }
            _ => false,
         }
      }
   }

   pub fn del_path(&mut self, path: &str) -> bool {
      if path == "." || path == "$" || path.is_empty() {
         return false;
      }
      let normalized = match path.strip_prefix("$.").or_else(|| path.strip_prefix('.')) {
         Some(n) => n,
         None => return false,
      };
      let parts: Vec<&str> = normalized.split('.').collect();
      if parts.is_empty() {
         return false;
      }
      let (parent_parts, last) = parts.split_at(parts.len() - 1);
      let last = last[0];

      let mut current = self;
      for part in parent_parts {
         match current {
            Self::Object(obj) => match obj.iter_mut().find(|(k, _)| k == part).map(|(_, v)| v) {
               Some(v) => current = v,
               None => return false,
            },
            _ => return false,
         }
      }

      match current {
         Self::Object(obj) => {
            let before = obj.len();
            obj.retain(|(k, _)| k != last);
            obj.len() < before
         }
         Self::Array(arr) => {
            if let Ok(idx) = last.parse::<usize>() {
               if idx < arr.len() {
                  arr.remove(idx);
                  true
               } else {
                  false
               }
            } else {
               false
            }
         }
         _ => false,
      }
   }

   pub fn parse(input: &str) -> Option<JsonValue> {
      let input = input.trim();
      if input.is_empty() {
         return None;
      }
      let (val, rest) = parse_json_value(input.as_bytes())?;
      if rest.iter().all(|b| b.is_ascii_whitespace()) {
         Some(val)
      } else {
         None
      }
   }
}

fn parse_json_value(input: &[u8]) -> Option<(JsonValue, &[u8])> {
   let input = skip_ws(input);
   if input.is_empty() {
      return None;
   }
   match input[0] {
      b'"' => parse_json_string(input),
      b'{' => parse_json_object(input),
      b'[' => parse_json_array(input),
      b't' if input.starts_with(b"true") => Some((JsonValue::Bool(true), &input[4..])),
      b'f' if input.starts_with(b"false") => Some((JsonValue::Bool(false), &input[5..])),
      b'n' if input.starts_with(b"null") => Some((JsonValue::Null, &input[4..])),
      _ => parse_json_number(input),
   }
}

fn skip_ws(input: &[u8]) -> &[u8] {
   let mut i = 0;
   while i < input.len() && input[i].is_ascii_whitespace() {
      i += 1;
   }
   &input[i..]
}

fn parse_json_string(input: &[u8]) -> Option<(JsonValue, &[u8])> {
   if input.first() != Some(&b'"') {
      return None;
   }
   let start = 1;
   let mut i = start;
   let mut has_escape = false;
   while i < input.len() {
      match input[i] {
         b'"' => {
            if !has_escape {
               let s = unsafe { std::str::from_utf8_unchecked(&input[start..i]) };
               return Some((JsonValue::String(SmallStr::new(s)), &input[i + 1..]));
            }
            break;
         }
         b'\\' => {
            has_escape = true;
            i += 2;
         }
         _ => i += 1,
      }
   }
   if !has_escape {
      return None;
   }
   i = start;
   let mut s = String::with_capacity(32);
   while i < input.len() {
      match input[i] {
         b'"' => return Some((JsonValue::String(SmallStr::from_string(s)), &input[i + 1..])),
         b'\\' => {
            i += 1;
            if i >= input.len() {
               return None;
            }
            match input[i] {
               b'"' => s.push('"'),
               b'\\' => s.push('\\'),
               b'/' => s.push('/'),
               b'n' => s.push('\n'),
               b'r' => s.push('\r'),
               b't' => s.push('\t'),
               b'b' => s.push('\x08'),
               b'f' => s.push('\x0c'),
               b'u' => {
                  if i + 4 >= input.len() {
                     return None;
                  }
                  let hex = std::str::from_utf8(&input[i + 1..i + 5]).ok()?;
                  let cp = u32::from_str_radix(hex, 16).ok()?;
                  s.push(char::from_u32(cp).unwrap_or('\u{FFFD}'));
                  i += 4;
               }
               _ => return None,
            }
         }
         b => s.push(b as char),
      }
      i += 1;
   }
   None
}

fn parse_json_number(input: &[u8]) -> Option<(JsonValue, &[u8])> {
   let mut i = 0;
   if i < input.len() && input[i] == b'-' {
      i += 1;
   }
   while i < input.len() && input[i].is_ascii_digit() {
      i += 1;
   }
   if i < input.len() && input[i] == b'.' {
      i += 1;
      while i < input.len() && input[i].is_ascii_digit() {
         i += 1;
      }
   }
   if i < input.len() && (input[i] == b'e' || input[i] == b'E') {
      i += 1;
      if i < input.len() && (input[i] == b'+' || input[i] == b'-') {
         i += 1;
      }
      while i < input.len() && input[i].is_ascii_digit() {
         i += 1;
      }
   }
   if i == 0 {
      return None;
   }
   let s = std::str::from_utf8(&input[..i]).ok()?;
   let n = s.parse::<f64>().ok()?;
   Some((JsonValue::Number(n), &input[i..]))
}

fn parse_json_object(input: &[u8]) -> Option<(JsonValue, &[u8])> {
   let mut rest = skip_ws(&input[1..]);
   let mut obj = Vec::new();
   if rest.first() == Some(&b'}') {
      return Some((JsonValue::Object(obj), &rest[1..]));
   }
   loop {
      rest = skip_ws(rest);
      let (key_val, r) = parse_json_string(rest)?;
      let key = match key_val {
         JsonValue::String(s) => s,
         _ => return None,
      };
      rest = skip_ws(r);
      if rest.first() != Some(&b':') {
         return None;
      }
      rest = skip_ws(&rest[1..]);
      let (val, r) = parse_json_value(rest)?;
      obj.push((key, val));
      rest = skip_ws(r);
      match rest.first() {
         Some(&b',') => rest = &rest[1..],
         Some(&b'}') => return Some((JsonValue::Object(obj), &rest[1..])),
         _ => return None,
      }
   }
}

fn parse_json_array(input: &[u8]) -> Option<(JsonValue, &[u8])> {
   let mut rest = skip_ws(&input[1..]);
   let mut arr = Vec::new();
   if rest.first() == Some(&b']') {
      return Some((JsonValue::Array(arr), &rest[1..]));
   }
   loop {
      rest = skip_ws(rest);
      let (val, r) = parse_json_value(rest)?;
      arr.push(val);
      rest = skip_ws(r);
      match rest.first() {
         Some(&b',') => rest = &rest[1..],
         Some(&b']') => return Some((JsonValue::Array(arr), &rest[1..])),
         _ => return None,
      }
   }
}
