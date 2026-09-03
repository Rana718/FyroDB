use crate::storage::store::Store;
use crate::storage::value::{FyroDB, SmallStr, StoreValue};
use foldhash::{HashMap, HashMapExt};
use std::collections::BTreeMap;

pub type StreamEntry = (String, Vec<(String, String)>);

#[derive(Clone)]
pub struct StreamData {
    pub entries: BTreeMap<StreamId, Vec<(SmallStr, SmallStr)>>,
    pub last_id: StreamId,
    pub groups: HashMap<SmallStr, ConsumerGroup>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl StreamId {
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.splitn(2, '-').collect();
        match parts.as_slice() {
            [ms, seq] => Some(Self {
                ms: ms.parse().ok()?,
                seq: seq.parse().ok()?,
            }),
            [ms] => Some(Self {
                ms: ms.parse().ok()?,
                seq: 0,
            }),
            _ => None,
        }
    }

    pub fn min() -> Self {
        Self { ms: 0, seq: 0 }
    }

    pub fn max() -> Self {
        Self {
            ms: u64::MAX,
            seq: u64::MAX,
        }
    }
}

#[derive(Clone)]
pub struct ConsumerGroup {
    pub last_delivered: StreamId,
    pub pel: BTreeMap<StreamId, PelEntry>,
    pub consumers: HashMap<SmallStr, ConsumerData>,
}

#[derive(Clone)]
pub struct PelEntry {
    pub consumer: SmallStr,
    pub delivery_time: u64,
    pub delivery_count: u32,
}

#[derive(Clone)]
pub struct ConsumerData {
    pub pel: Vec<StreamId>,
}

impl Default for StreamData {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamData {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_id: StreamId::new(0, 0),
            groups: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn add(&mut self, id: StreamId, fields: Vec<(String, String)>) -> StreamId {
        let fields = fields
            .into_iter()
            .map(|(k, v)| (SmallStr::from_string(k), SmallStr::from_string(v)))
            .collect();
        let actual_id = if id.ms == 0 && id.seq == 0 {
            let ms = crate::storage::value::now_ms();
            let seq = if ms == self.last_id.ms {
                self.last_id.seq + 1
            } else {
                0
            };
            StreamId::new(ms, seq)
        } else if id.seq == 0 && id.ms > 0 {
            if id.ms == self.last_id.ms {
                StreamId::new(id.ms, self.last_id.seq + 1)
            } else {
                id
            }
        } else {
            id
        };
        self.entries.insert(actual_id, fields);
        self.last_id = actual_id;
        actual_id
    }

    pub fn trim_maxlen(&mut self, maxlen: usize, _approx: bool) {
        while self.entries.len() > maxlen {
            self.entries.pop_first();
        }
    }
}

impl Store {
    pub fn xadd(
        &self,
        key: &str,
        id_str: &str,
        fields: Vec<(String, String)>,
        maxlen: Option<usize>,
        nomkstream: bool,
    ) -> Result<Option<String>, &'static str> {
        let id = if id_str == "*" {
            StreamId::new(0, 0)
        } else {
            StreamId::parse(id_str)
                .ok_or("Invalid stream ID specified as stream command argument")?
        };

        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                if nomkstream {
                    return Ok(None);
                }
                let mut stream = StreamData::new();
                let actual = stream.add(id, fields.clone());
                if let Some(ml) = maxlen {
                    stream.trim_maxlen(ml, false);
                }
                val.value = FyroDB::Stream(Box::new(stream));
                val.expires_ms = 0;
                return Ok(Some(actual.to_string()));
            }
            match val.value.as_stream_mut() {
                Some(stream) => {
                    let actual = stream.add(id, fields.clone());
                    if let Some(ml) = maxlen {
                        stream.trim_maxlen(ml, false);
                    }
                    Ok(Some(actual.to_string()))
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => {
                if nomkstream {
                    return Ok(None);
                }
                let mut stream = StreamData::new();
                let actual = stream.add(id, fields);
                if let Some(ml) = maxlen {
                    stream.trim_maxlen(ml, false);
                }
                let id_s = actual.to_string();
                self.data
                    .insert_str(key, StoreValue::stream(stream));
                Ok(Some(id_s))
            }
        }
    }

    pub fn xlen(&self, key: &str) -> Result<usize, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(0),
            Some(e) if e.is_expired() => Ok(0),
            Some(e) => match e.value.as_stream() {
                Some(s) => Ok(s.len()),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: usize,
    ) -> Result<Vec<StreamEntry>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![]),
            Some(e) if e.is_expired() => Ok(vec![]),
            Some(e) => match e.value.as_stream() {
                Some(s) => {
                    let start_id = if start == "-" {
                        StreamId::min()
                    } else {
                        StreamId::parse(start).unwrap_or(StreamId::min())
                    };
                    let end_id = if end == "+" {
                        StreamId::max()
                    } else {
                        StreamId::parse(end).unwrap_or(StreamId::max())
                    };
                    let limit = if count == 0 { usize::MAX } else { count };
                    let items: Vec<(String, Vec<(String, String)>)> = s
                        .entries
                        .range(start_id..=end_id)
                        .take(limit)
                        .map(|(id, fields)| {
                            (
                                id.to_string(),
                                fields
                                    .iter()
                                    .map(|(k, v)| (k.to_string(), v.to_string()))
                                    .collect(),
                            )
                        })
                        .collect();
                    Ok(items)
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn xrevrange(
        &self,
        key: &str,
        end: &str,
        start: &str,
        count: usize,
    ) -> Result<Vec<StreamEntry>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![]),
            Some(e) if e.is_expired() => Ok(vec![]),
            Some(e) => match e.value.as_stream() {
                Some(s) => {
                    let start_id = if start == "-" {
                        StreamId::min()
                    } else {
                        StreamId::parse(start).unwrap_or(StreamId::min())
                    };
                    let end_id = if end == "+" {
                        StreamId::max()
                    } else {
                        StreamId::parse(end).unwrap_or(StreamId::max())
                    };
                    let limit = if count == 0 { usize::MAX } else { count };
                    let items: Vec<(String, Vec<(String, String)>)> = s
                        .entries
                        .range(start_id..=end_id)
                        .rev()
                        .take(limit)
                        .map(|(id, fields)| {
                            (
                                id.to_string(),
                                fields
                                    .iter()
                                    .map(|(k, v)| (k.to_string(), v.to_string()))
                                    .collect(),
                            )
                        })
                        .collect();
                    Ok(items)
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn xtrim(&self, key: &str, maxlen: usize) -> Result<usize, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(0);
            }
            match val.value.as_stream_mut() {
                Some(s) => {
                    let before = s.len();
                    s.trim_maxlen(maxlen, false);
                    Ok(before - s.len())
                }
                None => Err("WRONGTYPE"),
            }
        });
        match result {
            Some(r) => r,
            None => Ok(0),
        }
    }

    pub fn xdel(&self, key: &str, ids: &[&str]) -> Result<usize, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(0);
            }
            match val.value.as_stream_mut() {
                Some(s) => {
                    let mut removed = 0;
                    for id_str in ids {
                        if let Some(id) = StreamId::parse(id_str)
                            && s.entries.remove(&id).is_some()
                        {
                            removed += 1;
                        }
                    }
                    Ok(removed)
                }
                None => Err("WRONGTYPE"),
            }
        });
        match result {
            Some(r) => r,
            None => Ok(0),
        }
    }

    pub fn xgroup_create(
        &self,
        key: &str,
        group: &str,
        id: &str,
        mkstream: bool,
    ) -> Result<bool, &'static str> {
        let start_id = if id == "$" {
            None
        } else {
            Some(StreamId::parse(id).unwrap_or(StreamId::min()))
        };

        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                if !mkstream {
                    return Err("ERR The XGROUP subcommand requires the key to exist");
                }
                let mut stream = StreamData::new();
                let last = start_id.unwrap_or(stream.last_id);
                stream.groups.insert(
                    SmallStr::new(group),
                    ConsumerGroup {
                        last_delivered: last,
                        pel: BTreeMap::new(),
                        consumers: HashMap::new(),
                    },
                );
                val.value = FyroDB::Stream(Box::new(stream));
                val.expires_ms = 0;
                return Ok(true);
            }
            match val.value.as_stream_mut() {
                Some(s) => {
                    let last = start_id.unwrap_or(s.last_id);
                    s.groups.insert(
                        SmallStr::new(group),
                        ConsumerGroup {
                            last_delivered: last,
                            pel: BTreeMap::new(),
                            consumers: HashMap::new(),
                        },
                    );
                    Ok(true)
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => {
                if !mkstream {
                    return Err("ERR The XGROUP subcommand requires the key to exist");
                }
                let mut stream = StreamData::new();
                let last = start_id.unwrap_or(StreamId::min());
                stream.groups.insert(
                    SmallStr::new(group),
                    ConsumerGroup {
                        last_delivered: last,
                        pel: BTreeMap::new(),
                        consumers: HashMap::new(),
                    },
                );
                self.data
                    .insert_str(key, StoreValue::stream(stream));
                Ok(true)
            }
        }
    }

    pub fn xgroup_destroy(&self, key: &str, group: &str) -> Result<bool, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(false);
            }
            match val.value.as_stream_mut() {
                Some(s) => Ok(s.groups.remove(group).is_some()),
                None => Err("WRONGTYPE"),
            }
        });
        match result {
            Some(r) => r,
            None => Ok(false),
        }
    }

    pub fn xack(&self, key: &str, group: &str, ids: &[&str]) -> Result<usize, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(0);
            }
            match val.value.as_stream_mut() {
                Some(s) => {
                    let g = match s.groups.get_mut(group) {
                        Some(g) => g,
                        None => return Ok(0),
                    };
                    let mut acked = 0;
                    for id_str in ids {
                        if let Some(id) = StreamId::parse(id_str)
                            && g.pel.remove(&id).is_some()
                        {
                            acked += 1;
                        }
                    }
                    Ok(acked)
                }
                None => Err("WRONGTYPE"),
            }
        });
        match result {
            Some(r) => r,
            None => Ok(0),
        }
    }
}
