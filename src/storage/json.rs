use crate::storage::store::Store;
use crate::storage::value::{JsonValue, StoreValue};

impl Store {
    pub fn json_set(
        &self,
        key: &str,
        path: &str,
        value: &str,
        nx: bool,
        xx: bool,
    ) -> Result<bool, &'static str> {
        if path == "." || path == "$" || path.is_empty() {
            if nx && self.data.get_ref(key).is_some_and(|e| !e.is_expired()) {
                return Ok(false);
            }
            if xx {
                let exists = self.data.get_ref(key).is_some_and(|e| !e.is_expired());
                if !exists {
                    return Ok(false);
                }
            }
            self.data
                .insert_str(key, StoreValue::json_raw(value.to_owned()));
            return Ok(true);
        }

        let parsed = JsonValue::parse(value).ok_or("ERR invalid JSON")?;

        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                if xx {
                    return Ok(false);
                }
                let mut root = JsonValue::Object(Vec::new());
                if root.set_path(path, parsed.clone()) {
                    val.value.set_json(root);
                    val.expires_ms = 0;
                    Ok(true)
                } else {
                    Err("ERR path does not exist")
                }
            } else {
                if val.value.as_json_str().is_none() {
                    return Err("WRONGTYPE");
                }
                let mut json = val.value.parse_json().unwrap();
                if nx && json.get_path(path).is_some() {
                    return Ok(false);
                }
                if xx && json.get_path(path).is_none() {
                    return Ok(false);
                }
                if json.set_path(path, parsed.clone()) {
                    val.value.set_json(json);
                    Ok(true)
                } else {
                    Err("ERR path does not exist")
                }
            }
        });

        match result {
            Some(r) => r,
            None => {
                if xx {
                    return Ok(false);
                }
                if path == "." || path == "$" || path.is_empty() {
                    self.data.insert_str(key, StoreValue::json(parsed));
                    Ok(true)
                } else {
                    let mut root = JsonValue::Object(Vec::new());
                    if root.set_path(path, parsed) {
                        self.data.insert_str(key, StoreValue::json(root));
                        Ok(true)
                    } else {
                        Err("ERR path does not exist")
                    }
                }
            }
        }
    }

    pub fn json_get(&self, key: &str, paths: &[&str]) -> Result<Option<String>, &'static str> {
        let mut buf = Vec::new();
        match self.json_get_to_buf(key, paths, &mut buf) {
            Ok(true) => {
                // Strip the bulk header and trailing CRLF to return the value.
                let s = String::from_utf8_lossy(&buf).into_owned();
                let start = s.find("\r\n").map(|i| i + 2).unwrap_or(0);
                let end = s.len().saturating_sub(2);
                Ok(Some(s[start..end].to_string()))
            }
            Ok(false) => Ok(None),
            Err(e) => Err(e),
        }
    }

/// Root-path fast case writes the stored document straight to `out`.
/// Ok(true) when a reply was written.
    pub fn json_get_to_buf(
        &self,
        key: &str,
        paths: &[&str],
        out: &mut Vec<u8>,
    ) -> Result<bool, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(false),
            Some(e) if e.is_expired() => Ok(false),
            Some(e) => {
                let raw = match e.value.as_json_str() {
                    Some(s) => s,
                    None => return Err("WRONGTYPE"),
                };
                if paths.is_empty()
                    || (paths.len() == 1
                        && (paths[0] == "." || paths[0] == "$" || paths[0].is_empty()))
                {
                    crate::utils::resp::write_bulk(out, raw);
                    return Ok(true);
                }
                let json = match e.value.parse_json() {
                    Some(j) => j,
                    None => return Ok(false),
                };
                if paths.len() == 1 {
                    match json.get_path(paths[0]) {
                        Some(v) => {
                            crate::utils::resp::write_bulk(out, &v.to_resp_string());
                            Ok(true)
                        }
                        None => Ok(false),
                    }
                } else {
                    let mut result = String::from("{");
                    for (i, &p) in paths.iter().enumerate() {
                        if i > 0 {
                            result.push(',');
                        }
                        result.push('"');
                        result.push_str(p);
                        result.push_str("\":");
                        match json.get_path(p) {
                            Some(v) => result.push_str(&v.to_resp_string()),
                            None => result.push_str("null"),
                        }
                    }
                    result.push('}');
                    crate::utils::resp::write_bulk(out, &result);
                    Ok(true)
                }
            }
        }
    }

    pub fn json_del(&self, key: &str, path: &str) -> Result<usize, &'static str> {
        if path == "." || path == "$" || path.is_empty() {
            return if self.data.remove(key).is_some() {
                Ok(1)
            } else {
                Ok(0)
            };
        }

        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(0);
            }
            if val.value.as_json_str().is_none() {
                return Err("WRONGTYPE");
            }
            let mut json = val.value.parse_json().unwrap();
            if json.del_path(path) {
                val.value.set_json(json);
                Ok(1)
            } else {
                Ok(0)
            }
        });

        match result {
            Some(r) => r,
            None => Ok(0),
        }
    }

    pub fn json_type(&self, key: &str, path: &str) -> Result<Option<&'static str>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(None),
            Some(e) if e.is_expired() => Ok(None),
            Some(e) => {
                if e.value.as_json_str().is_none() {
                    return Err("WRONGTYPE");
                }
                let json = e.value.parse_json().unwrap();
                let target = if path.is_empty() || path == "." || path == "$" {
                    &json
                } else {
                    match json.get_path(path) {
                        Some(v) => v,
                        None => return Ok(None),
                    }
                };
                Ok(Some(target.type_name()))
            }
        }
    }

    pub fn json_numincrby(
        &self,
        key: &str,
        path: &str,
        by: f64,
    ) -> Result<Option<f64>, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Err("ERR no such key");
            }
            if val.value.as_json_str().is_none() {
                return Err("WRONGTYPE");
            }
            let mut json = val.value.parse_json().unwrap();
            let target = json.get_path_mut(path).ok_or("ERR path does not exist")?;
            match target {
                JsonValue::Number(n) => {
                    *n += by;
                    let result = *n;
                    val.value.set_json(json);
                    Ok(Some(result))
                }
                _ => Err("ERR path value is not a number"),
            }
        });

        match result {
            Some(r) => r,
            None => Err("ERR no such key"),
        }
    }

    pub fn json_strappend(
        &self,
        key: &str,
        path: &str,
        append: &str,
    ) -> Result<Option<usize>, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Err("ERR no such key");
            }
            if val.value.as_json_str().is_none() {
                return Err("WRONGTYPE");
            }
            let mut json = val.value.parse_json().unwrap();
            let target = json.get_path_mut(path).ok_or("ERR path does not exist")?;
            match target {
                JsonValue::String(s) => {
                    s.push_str(append);
                    let len = s.len();
                    val.value.set_json(json);
                    Ok(Some(len))
                }
                _ => Err("ERR path value is not a string"),
            }
        });

        match result {
            Some(r) => r,
            None => Err("ERR no such key"),
        }
    }

    pub fn json_strlen(&self, key: &str, path: &str) -> Result<Option<usize>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(None),
            Some(e) if e.is_expired() => Ok(None),
            Some(e) => {
                if e.value.as_json_str().is_none() {
                    return Err("WRONGTYPE");
                }
                let json = e.value.parse_json().unwrap();
                let target = if path.is_empty() || path == "." || path == "$" {
                    &json
                } else {
                    match json.get_path(path) {
                        Some(v) => v,
                        None => return Ok(None),
                    }
                };
                match target {
                    JsonValue::String(s) => Ok(Some(s.len())),
                    _ => Err("ERR path value is not a string"),
                }
            }
        }
    }

    pub fn json_arrappend(
        &self,
        key: &str,
        path: &str,
        values: &[&str],
    ) -> Result<Option<usize>, &'static str> {
        let parsed: Vec<JsonValue> = values
            .iter()
            .map(|v| JsonValue::parse(v).ok_or("ERR invalid JSON"))
            .collect::<Result<Vec<_>, _>>()?;

        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Err("ERR no such key");
            }
            if val.value.as_json_str().is_none() {
                return Err("WRONGTYPE");
            }
            let mut json = val.value.parse_json().unwrap();
            let target = json.get_path_mut(path).ok_or("ERR path does not exist")?;
            match target {
                JsonValue::Array(arr) => {
                    arr.extend(parsed.iter().cloned());
                    let len = arr.len();
                    val.value.set_json(json);
                    Ok(Some(len))
                }
                _ => Err("ERR path value is not an array"),
            }
        });

        match result {
            Some(r) => r,
            None => Err("ERR no such key"),
        }
    }

    pub fn json_arrlen(&self, key: &str, path: &str) -> Result<Option<usize>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(None),
            Some(e) if e.is_expired() => Ok(None),
            Some(e) => {
                if e.value.as_json_str().is_none() {
                    return Err("WRONGTYPE");
                }
                let json = e.value.parse_json().unwrap();
                let target = if path.is_empty() || path == "." || path == "$" {
                    &json
                } else {
                    match json.get_path(path) {
                        Some(v) => v,
                        None => return Ok(None),
                    }
                };
                match target {
                    JsonValue::Array(arr) => Ok(Some(arr.len())),
                    _ => Err("ERR path value is not an array"),
                }
            }
        }
    }

    pub fn json_arrpop(
        &self,
        key: &str,
        path: &str,
        index: i64,
    ) -> Result<Option<String>, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(None);
            }
            if val.value.as_json_str().is_none() {
                return Err("WRONGTYPE");
            }
            let mut json = val.value.parse_json().unwrap();
            let target = json.get_path_mut(path).ok_or("ERR path does not exist")?;
            match target {
                JsonValue::Array(arr) => {
                    if arr.is_empty() {
                        return Ok(None);
                    }
                    let idx = if index < 0 {
                        let adj = arr.len() as i64 + index;
                        if adj < 0 { 0 } else { adj as usize }
                    } else {
                        (index as usize).min(arr.len() - 1)
                    };
                    let removed = arr.remove(idx);
                    let s = removed.to_resp_string();
                    val.value.set_json(json);
                    Ok(Some(s))
                }
                _ => Err("ERR path value is not an array"),
            }
        });

        match result {
            Some(r) => r,
            None => Ok(None),
        }
    }

    pub fn json_objkeys(&self, key: &str, path: &str) -> Result<Option<Vec<String>>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(None),
            Some(e) if e.is_expired() => Ok(None),
            Some(e) => {
                if e.value.as_json_str().is_none() {
                    return Err("WRONGTYPE");
                }
                let json = e.value.parse_json().unwrap();
                let target = if path.is_empty() || path == "." || path == "$" {
                    &json
                } else {
                    match json.get_path(path) {
                        Some(v) => v,
                        None => return Ok(None),
                    }
                };
                match target {
                    JsonValue::Object(obj) => {
                        Ok(Some(obj.iter().map(|(k, _)| k.to_string()).collect()))
                    }
                    _ => Err("ERR path value is not an object"),
                }
            }
        }
    }

    pub fn json_objlen(&self, key: &str, path: &str) -> Result<Option<usize>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(None),
            Some(e) if e.is_expired() => Ok(None),
            Some(e) => {
                if e.value.as_json_str().is_none() {
                    return Err("WRONGTYPE");
                }
                let json = e.value.parse_json().unwrap();
                let target = if path.is_empty() || path == "." || path == "$" {
                    &json
                } else {
                    match json.get_path(path) {
                        Some(v) => v,
                        None => return Ok(None),
                    }
                };
                match target {
                    JsonValue::Object(obj) => Ok(Some(obj.len())),
                    _ => Err("ERR path value is not an object"),
                }
            }
        }
    }

    pub fn json_toggle(&self, key: &str, path: &str) -> Result<Option<bool>, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Err("ERR no such key");
            }
            if val.value.as_json_str().is_none() {
                return Err("WRONGTYPE");
            }
            let mut json = val.value.parse_json().unwrap();
            let target = json.get_path_mut(path).ok_or("ERR path does not exist")?;
            match target {
                JsonValue::Bool(b) => {
                    *b = !*b;
                    let result = *b;
                    val.value.set_json(json);
                    Ok(Some(result))
                }
                _ => Err("ERR path value is not a boolean"),
            }
        });

        match result {
            Some(r) => r,
            None => Err("ERR no such key"),
        }
    }

    pub fn json_clear(&self, key: &str, path: &str) -> Result<usize, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(0);
            }
            if val.value.as_json_str().is_none() {
                return Err("WRONGTYPE");
            }
            let mut json = val.value.parse_json().unwrap();
            let target = if path.is_empty() || path == "." || path == "$" {
                &mut json
            } else {
                match json.get_path_mut(path) {
                    Some(v) => v,
                    None => return Ok(0),
                }
            };
            match target {
                JsonValue::Array(arr) => {
                    arr.clear();
                    val.value.set_json(json);
                    Ok(1)
                }
                JsonValue::Object(obj) => {
                    obj.clear();
                    val.value.set_json(json);
                    Ok(1)
                }
                JsonValue::Number(n) => {
                    *n = 0.0;
                    val.value.set_json(json);
                    Ok(1)
                }
                _ => Ok(0),
            }
        });

        match result {
            Some(r) => r,
            None => Ok(0),
        }
    }

    pub fn json_arrindex(
        &self,
        key: &str,
        path: &str,
        value: &str,
        start: i64,
        stop: i64,
    ) -> Result<i64, &'static str> {
        let search = JsonValue::parse(value).ok_or("ERR invalid JSON")?;

        match self.data.get_ref(key) {
            None => Ok(-1),
            Some(e) if e.is_expired() => Ok(-1),
            Some(e) => {
                if e.value.as_json_str().is_none() {
                    return Err("WRONGTYPE");
                }
                let json = e.value.parse_json().unwrap();
                let target = if path.is_empty() || path == "." || path == "$" {
                    &json
                } else {
                    match json.get_path(path) {
                        Some(v) => v,
                        None => return Ok(-1),
                    }
                };
                match target {
                    JsonValue::Array(arr) => {
                        let len = arr.len() as i64;
                        let s = if start < 0 {
                            (len + start).max(0)
                        } else {
                            start
                        } as usize;
                        let e_idx = if stop == 0 {
                            arr.len()
                        } else if stop < 0 {
                            (len + stop).max(0) as usize
                        } else {
                            (stop as usize).min(arr.len())
                        };
                        if let Some(i) = arr[s..e_idx]
                            .iter()
                            .position(|v| json_values_equal(v, &search))
                        {
                            return Ok((s + i) as i64);
                        }
                        Ok(-1)
                    }
                    _ => Err("ERR path value is not an array"),
                }
            }
        }
    }

    pub fn json_arrinsert(
        &self,
        key: &str,
        path: &str,
        index: i64,
        values: &[&str],
    ) -> Result<Option<usize>, &'static str> {
        let parsed: Vec<JsonValue> = values
            .iter()
            .map(|v| JsonValue::parse(v).ok_or("ERR invalid JSON"))
            .collect::<Result<Vec<_>, _>>()?;

        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Err("ERR no such key");
            }
            if val.value.as_json_str().is_none() {
                return Err("WRONGTYPE");
            }
            let mut json = val.value.parse_json().unwrap();
            let target = json.get_path_mut(path).ok_or("ERR path does not exist")?;
            match target {
                JsonValue::Array(arr) => {
                    let idx = if index < 0 {
                        let adj = arr.len() as i64 + index;
                        if adj < 0 { 0 } else { adj as usize }
                    } else {
                        (index as usize).min(arr.len())
                    };
                    for (i, v) in parsed.iter().cloned().enumerate() {
                        arr.insert(idx + i, v);
                    }
                    let len = arr.len();
                    val.value.set_json(json);
                    Ok(Some(len))
                }
                _ => Err("ERR path value is not an array"),
            }
        });

        match result {
            Some(r) => r,
            None => Err("ERR no such key"),
        }
    }

    pub fn json_arrtrim(
        &self,
        key: &str,
        path: &str,
        start: i64,
        stop: i64,
    ) -> Result<Option<usize>, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Err("ERR no such key");
            }
            if val.value.as_json_str().is_none() {
                return Err("WRONGTYPE");
            }
            let mut json = val.value.parse_json().unwrap();
            let target = json.get_path_mut(path).ok_or("ERR path does not exist")?;
            match target {
                JsonValue::Array(arr) => {
                    let len = arr.len() as i64;
                    let s = if start < 0 {
                        (len + start).max(0)
                    } else {
                        start.min(len)
                    } as usize;
                    let e = if stop < 0 {
                        (len + stop).max(0)
                    } else {
                        stop.min(len - 1)
                    } as usize;
                    if s > e || s >= arr.len() {
                        arr.clear();
                    } else {
                        let keep: Vec<JsonValue> = arr.drain(s..=e.min(arr.len() - 1)).collect();
                        *arr = keep;
                    }
                    let len = arr.len();
                    val.value.set_json(json);
                    Ok(Some(len))
                }
                _ => Err("ERR path value is not an array"),
            }
        });

        match result {
            Some(r) => r,
            None => Err("ERR no such key"),
        }
    }

    pub fn json_nummultby(
        &self,
        key: &str,
        path: &str,
        by: f64,
    ) -> Result<Option<f64>, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Err("ERR no such key");
            }
            if val.value.as_json_str().is_none() {
                return Err("WRONGTYPE");
            }
            let mut json = val.value.parse_json().unwrap();
            let target = json.get_path_mut(path).ok_or("ERR path does not exist")?;
            match target {
                JsonValue::Number(n) => {
                    *n *= by;
                    let result = *n;
                    val.value.set_json(json);
                    Ok(Some(result))
                }
                _ => Err("ERR path value is not a number"),
            }
        });

        match result {
            Some(r) => r,
            None => Err("ERR no such key"),
        }
    }
}

fn json_values_equal(a: &JsonValue, b: &JsonValue) -> bool {
    match (a, b) {
        (JsonValue::Null, JsonValue::Null) => true,
        (JsonValue::Bool(a), JsonValue::Bool(b)) => a == b,
        (JsonValue::Number(a), JsonValue::Number(b)) => (a - b).abs() < f64::EPSILON,
        (JsonValue::String(a), JsonValue::String(b)) => a == b,
        _ => false,
    }
}
