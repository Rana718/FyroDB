use std::sync::atomic::AtomicU64;

/// Hard caps enforced by the schema parser.
const MAX_FIELDS: usize = 512;
const MAX_DIMS: usize = 32768;

#[derive(Clone, Copy, PartialEq, Eq)]
#[derive(Debug)]
pub enum FieldType {
    Numeric,
    Tag,
    Text,
    Vector,
}

impl FieldType {
    pub fn as_str(self) -> &'static str {
        match self {
            FieldType::Numeric => "NUMERIC",
            FieldType::Tag => "TAG",
            FieldType::Text => "TEXT",
            FieldType::Vector => "VECTOR",
        }
    }
}

pub use fyro_vector::DistanceMetric;

#[derive(Clone)]
pub struct VectorAttrs {
    pub dims: usize,
    pub metric: DistanceMetric,
    pub algorithm: &'static str,
    pub dtype: &'static str,
    /// HNSW connectivity: outbound edges per node (level 0 keeps 2×).
    pub m: usize,
    /// HNSW candidate-search width during linking.
    pub ef_construction: usize,
    /// Traversal encoding of an HNSW graph (FLAT is always exact f32).
    pub quant: fyro_vector::Quant,
}

impl VectorAttrs {
    /// Element type of document and query blobs.
    pub fn elem(&self) -> fyro_vector::ElemType {
        fyro_vector::ElemType::parse(self.dtype).unwrap_or(fyro_vector::ElemType::Float32)
    }

    /// Byte length of one vector blob.
    pub fn blob_len(&self) -> usize {
        self.dims * self.elem().width()
    }

    /// Decode a document or query blob to f32 (None on a size mismatch).
    pub fn decode(&self, bytes: &[u8]) -> Option<Vec<f32>> {
        self.elem().decode(bytes, self.dims)
    }
}

/// Safe bounds for user-supplied HNSW parameters preventing unreasonable
/// memory preallocation or stalled builds.
pub const MAX_M: usize = 256;
pub const MAX_EF_CONSTRUCTION: usize = 10_000;

#[derive(Clone)]
pub struct FieldDef {
    pub name: String,
    pub alias: Option<String>,
    pub field_type: FieldType,
    pub sortable: bool,
    pub no_index: bool,
    pub separator: char,
    pub vector: Option<VectorAttrs>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    Hash,
    Json,
}

impl StorageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            StorageKind::Hash => "HASH",
            StorageKind::Json => "JSON",
        }
    }
}

pub struct IndexDef {
    pub name: String,
    pub prefixes: Vec<String>,
    pub storage: StorageKind,
    pub fields: Vec<FieldDef>,
    pub created_at_ms: u64,
    pub num_docs: AtomicU64,
}

impl IndexDef {
    pub fn matches_key(&self, key: &str) -> bool {
        self.prefixes.is_empty() || self.prefixes.iter().any(|p| key.starts_with(p.as_str()))
    }

    pub fn field(&self, name: &str) -> Option<&FieldDef> {
        self.fields
            .iter()
            .find(|f| f.name == name || f.alias.as_deref() == Some(name))
    }
}

pub fn parse_field_defs(
    schema: &[&str],
) -> Result<Vec<FieldDef>, (&'static str, Option<usize>)> {
    let mut fields = Vec::new();
    let mut i = 0;
    while i < schema.len() {
        if fields.len() >= MAX_FIELDS {
            return Err(("ERR too many fields", None));
        }
        let name = schema[i];
        i += 1;
        let Some(&ty) = schema.get(i) else {
            return Err(("ERR wrong number of args", Some(i)));
        };
        i += 1;
        let field_type = match ty.to_ascii_uppercase().as_str() {
            "NUMERIC" => FieldType::Numeric,
            "TAG" => FieldType::Tag,
            "TEXT" => FieldType::Text,
            "VECTOR" => FieldType::Vector,
            _ => return Err(("ERR unknown field type", Some(i))),
        };
        let mut def = FieldDef {
            name: name.to_string(),
            alias: None,
            field_type,
            sortable: false,
            no_index: false,
            separator: ',',
            vector: None,
        };
        let mut vector_attrs: Vec<&str> = Vec::new();
        while i < schema.len() {
            let tok = schema[i].to_ascii_uppercase();
            match tok.as_str() {
                "SORTABLE" => {
                    def.sortable = true;
                    i += 1;
                }
                "UNF" => i += 1,
                "NOINDEX" => {
                    def.no_index = true;
                    i += 1;
                }
                "AS" => {
                    def.alias = schema.get(i + 1).map(|s| s.to_string());
                    i += 2;
                }
                "SEPARATOR" => {
                    def.separator = schema.get(i + 1).and_then(|s| s.chars().next()).unwrap_or(',');
                    i += 2;
                }
                "CASESENSITIVE" | "NOSTEM" | "WITHSUFFIXTRIE" | "INDEXMISSING" | "INDEXEMPTY" => {
                    i += 1;
                }
                "WEIGHT" | "PHONETIC" => i += 2,
                _ => break,
            }
        }
        if field_type == FieldType::Vector {
            let Some(algorithm) = schema.get(i).map(|s| s.to_ascii_uppercase()) else {
                return Err(("ERR vector field requires algorithm", Some(i)));
            };
            i += 1;
            let Some(&count_tok) = schema.get(i) else {
                return Err(("ERR vector field requires arg count", Some(i)));
            };
            i += 1;
            let count: usize = count_tok.parse().map_err(|_| ("ERR bad vector arg count", Some(i)))?;
            let mut attrs: Vec<(&str, &str)> = Vec::with_capacity(8);
            for _ in 0..count / 2 {
                let Some(k) = schema.get(i) else {
                    return Err(("ERR wrong vector arg count", Some(i)));
                };
                let Some(v) = schema.get(i + 1) else {
                    return Err(("ERR wrong vector arg count", Some(i)));
                };
                attrs.push((k, v));
                i += 2;
            }
            let mut dims = 0usize;
            let mut metric = DistanceMetric::Cosine;
            let mut dtype = "FLOAT32";
            let mut m = fyro_vector::hnsw::DEFAULT_M;
            let mut ef_construction = fyro_vector::hnsw::DEFAULT_EF_CONSTRUCTION;
            let mut quant = None;
            for (k, v) in attrs {
                match k.to_ascii_uppercase().as_str() {
                    "DIM" | "DIMS" => {
                        dims = v.parse().map_err(|_| ("ERR bad vector DIM", Some(i)))?;
                    }
                    "DISTANCE_METRIC" => {
                        metric = match v.to_ascii_uppercase().as_str() {
                            "L2" => DistanceMetric::L2,
                            "COSINE" => DistanceMetric::Cosine,
                            "IP" => DistanceMetric::Ip,
                            _ => return Err(("ERR bad distance metric", Some(i))),
                        };
                    }
                    "TYPE" => {
                        if v.eq_ignore_ascii_case("FLOAT64") {
                            return Err(("ERR FLOAT64 vectors unsupported", Some(i)));
                        }
                        dtype = canonical_dtype(v).ok_or(("ERR bad vector TYPE", Some(i)))?;
                    }
                    "M" => {
                        m = v
                            .parse::<usize>()
                            .ok()
                            .filter(|m| (2..=MAX_M).contains(m))
                            .ok_or(("ERR bad M", Some(i)))?;
                    }
                    "QUANTIZATION" | "COMPRESSION" => {
                        quant = Some(fyro_vector::Quant::parse(v).ok_or(("ERR bad QUANTIZATION (NONE | INT8 | RABITQ)", Some(i)))?);
                    }
                    "EF_CONSTRUCTION" => {
                        ef_construction = v
                            .parse::<usize>()
                            .ok()
                            .filter(|ef| (1..=MAX_EF_CONSTRUCTION).contains(ef))
                            .ok_or(("ERR bad EF_CONSTRUCTION", Some(i)))?;
                    }
                    _ => {}
                }
            }
            if dims == 0 || dims > MAX_DIMS {
                return Err(("ERR bad vector DIM", Some(i)));
            }
            def.vector = Some(VectorAttrs {
                dims,
                metric,
                algorithm: if algorithm == "HNSW" { "HNSW" } else { "FLAT" },
                dtype,
                m,
                ef_construction,
                // Int8 is ~2.7× faster and 35% smaller at 768d; below 256 dims f32 is fast enough.
                quant: if algorithm == "HNSW" {
                    quant.unwrap_or(if dims >= 256 { fyro_vector::Quant::Int8 } else { fyro_vector::Quant::None })
                } else {
                    fyro_vector::Quant::None
                },
            });
            vector_attrs.clear();
            let _ = vector_attrs;
        }
        fields.push(def);
    }
    Ok(fields)
}

/// Supported vector element types, canonical spelling.
pub fn canonical_dtype(s: &str) -> Option<&'static str> {
    fyro_vector::ElemType::parse(s).map(|t| t.as_str())
}
