use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnInfo {
   pub name: String,
   pub type_oid: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DbValue {
   Null,
   Bool(bool),
   Int(i64),
   Float(f64),
   Text(String),
}

impl DbValue {
   pub fn as_bytes(&self) -> Option<Vec<u8>> {
      match self {
         Self::Null => None,
         Self::Bool(b) => Some(if *b { vec![b't'] } else { vec![b'f'] }),
         Self::Int(i) => Some(i.to_string().into_bytes()),
         Self::Float(f) => Some(f.to_string().into_bytes()),
         Self::Text(s) => Some(s.as_bytes().to_vec()),
      }
   }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DbRow {
   pub values: Vec<DbValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DbResultSet {
   pub columns: Vec<ColumnInfo>,
   pub rows: Vec<DbRow>,
   pub rows_affected: u64,
}

impl DbResultSet {
   pub fn empty() -> Self {
      Self {
         columns: Vec::new(),
         rows: Vec::new(),
         rows_affected: 0,
      }
   }

   // Serializes rowset to JSON for direct reading by Redis clients on port 8000.
   pub fn to_json(&self) -> String {
      let mut list = Vec::with_capacity(self.rows.len());
      for row in &self.rows {
         let mut map = serde_json::Map::new();
         for (col, val) in self.columns.iter().zip(&row.values) {
            let v = match val {
               DbValue::Null => serde_json::Value::Null,
               DbValue::Bool(b) => serde_json::Value::Bool(*b),
               DbValue::Int(i) => serde_json::Value::Number((*i).into()),
               DbValue::Float(f) => serde_json::Number::from_f64(*f)
                  .map(serde_json::Value::Number)
                  .unwrap_or(serde_json::Value::Null),
               DbValue::Text(t) => serde_json::Value::String(t.clone()),
            };
            map.insert(col.name.clone(), v);
         }
         list.push(serde_json::Value::Object(map));
      }
      serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_string())
   }

   // Deserializes a cached JSON rowset into a result set.
   pub fn from_json(json_str: &str) -> Option<Self> {
      let val: serde_json::Value = serde_json::from_str(json_str).ok()?;
      let arr = val.as_array()?;
      if arr.is_empty() {
         return Some(Self::empty());
      }

      let first = arr[0].as_object()?;
      let columns: Vec<ColumnInfo> = first
         .keys()
         .map(|k| ColumnInfo {
            name: k.clone(),
            type_oid: 25, // default text oid
         })
         .collect();

      let mut rows = Vec::with_capacity(arr.len());
      for item in arr {
         let obj = item.as_object()?;
         let mut values = Vec::with_capacity(columns.len());
         for col in &columns {
            let v = match obj.get(&col.name) {
               None | Some(serde_json::Value::Null) => DbValue::Null,
               Some(serde_json::Value::Bool(b)) => DbValue::Bool(*b),
               Some(serde_json::Value::Number(n)) => {
                  if let Some(i) = n.as_i64() {
                     DbValue::Int(i)
                  } else if let Some(f) = n.as_f64() {
                     DbValue::Float(f)
                  } else {
                     DbValue::Null
                  }
               }
               Some(serde_json::Value::String(s)) => DbValue::Text(s.clone()),
               Some(other) => DbValue::Text(other.to_string()),
            };
            values.push(v);
         }
         rows.push(DbRow { values });
      }

      Some(Self {
         columns,
         rows,
         rows_affected: arr.len() as u64,
      })
   }
}
