use super::*;

pub(super) fn write_search_indexes(w: &mut BufWriter<File>, store: &Store) -> io::Result<()> {
   let names = store.search_state().list_names();
   for name in names {
      let Some(def) = store.search_state().def_arc(&name) else {
         continue;
      };
      write_u8(w, TYPE_INDEX)?;
      write_bytes(w, def.name.as_bytes())?;
      write_u32(w, def.prefixes.len() as u32)?;
      for p in &def.prefixes {
         write_bytes(w, p.as_bytes())?;
      }
      write_u8(
         w,
         match def.storage {
            crate::search::StorageKind::Hash => 0,
            crate::search::StorageKind::Json => 1,
         },
      )?;
      write_u32(w, def.fields.len() as u32)?;
      for f in &def.fields {
         write_bytes(w, f.name.as_bytes())?;
         write_u8(
            w,
            match f.field_type {
               crate::search::FieldType::Numeric => 0,
               crate::search::FieldType::Tag => 1,
               crate::search::FieldType::Text => 2,
               crate::search::FieldType::Vector => 3,
            },
         )?;
         let flags = u8::from(f.sortable) | (u8::from(f.no_index) << 1);
         write_u8(w, flags)?;
         write_u8(w, f.separator as u8)?;
         if let Some(v) = &f.vector {
            write_u32(w, v.dims as u32)?;
            write_u8(
               w,
               match v.metric {
                  crate::search::DistanceMetric::L2 => 0,
                  crate::search::DistanceMetric::Cosine => 1,
                  crate::search::DistanceMetric::Ip => 2,
               },
            )?;
            write_u8(w, u8::from(v.algorithm == "HNSW"))?;
            write_u32(w, v.m as u32)?;
            write_u32(w, v.ef_construction as u32)?;
            write_bytes(w, v.dtype.as_bytes())?;
            write_bytes(w, v.quant.as_str().as_bytes())?;
         }
      }
   }
   Ok(())
}

/// Persists HNSW links and FLAT id maps to restore search immediately on restart.
/// Vectors are re-read from loaded hash fields.
pub(super) fn write_vector_graphs(w: &mut BufWriter<File>, store: &Store) -> io::Result<()> {
   for (index, field, keys, dump) in store.search_state().graphs_for_snapshot() {
      let key_of: HashMap<usize, std::sync::Arc<str>> = keys.into_iter().collect();
      write_u8(w, TYPE_VECTOR_GRAPH)?;
      write_bytes(w, index.as_bytes())?;
      write_bytes(w, field.as_bytes())?;
      match dump {
         Some(d) => {
            let nodes: Vec<_> = d
               .nodes
               .iter()
               .filter(|n| key_of.contains_key(&(n.id as usize)))
               .collect();
            write_u8(w, 1)?;
            write_u32(w, d.entry.unwrap_or(u32::MAX))?;
            write_u32(w, d.centroids.len() as u32)?;
            write_u32(w, d.centroids.first().map_or(0, |c| c.len()) as u32)?;
            for c in &d.centroids {
               for &x in c {
                  write_u32(w, x.to_bits())?;
               }
            }
            write_u32(w, nodes.len() as u32)?;
            for n in nodes {
               write_bytes(w, key_of[&(n.id as usize)].as_bytes())?;
               write_u32(w, n.id)?;
               write_u8(w, n.level)?;
               for list in &n.links {
                  write_u32(w, list.len() as u32)?;
                  for &e in list {
                     write_u32(w, e)?;
                  }
               }
            }
         }
         None => {
            write_u8(w, 0)?;
            write_u32(w, u32::MAX)?;
            write_u32(w, 0)?;
            write_u32(w, 0)?;
            write_u32(w, key_of.len() as u32)?;
            for (id, key) in &key_of {
               write_bytes(w, key.as_bytes())?;
               write_u32(w, *id as u32)?;
               write_u8(w, 0)?;
               write_u32(w, 0)?;
            }
         }
      }
   }
   Ok(())
}

pub(super) fn read_vector_graph<R: Read>(r: &mut R, store: &Store) -> io::Result<()> {
   let bad = |m: &'static str| io::Error::new(io::ErrorKind::InvalidData, m);
   let index = read_string_bounded(r)?;
   let field = read_string_bounded(r)?;
   let _linked = read_u8(r)? == 1;
   let entry = read_u32(r)?;
   let ncent = read_u32(r)? as usize;
   let cdims = read_u32(r)? as usize;
   if ncent > 256 || cdims > 65_536 {
      return Err(bad("vector graph codebook out of range"));
   }
   let mut centroids = Vec::with_capacity(ncent);
   for _ in 0..ncent {
      let mut c = Vec::with_capacity(cdims);
      for _ in 0..cdims {
         c.push(f32::from_bits(read_u32(r)?));
      }
      centroids.push(c);
   }
   let count = read_count_bounded(r)? as usize;
   let mut nodes = Vec::with_capacity(count.min(1 << 20));
   for _ in 0..count {
      let key = read_string_bounded(r)?;
      let id = read_u32(r)?;
      let level = read_u8(r)?;
      if level as usize > 32 {
         return Err(bad("vector graph level out of range"));
      }
      let mut links = Vec::with_capacity(level as usize + 1);
      for _ in 0..=level {
         let n = read_u32(r)? as usize;
         if n > 1024 {
            return Err(bad("vector graph degree out of range"));
         }
         let mut list = Vec::with_capacity(n);
         for _ in 0..n {
            list.push(read_u32(r)?);
         }
         links.push(list);
      }
      nodes.push((key, id, level, links));
   }
   let entry = (entry != u32::MAX).then_some(entry);
   store
      .search_state()
      .restore_graph(store, &index, &field, nodes, entry, centroids);
   Ok(())
}

pub(super) fn read_search_index<R: Read>(r: &mut R, store: &Store, version: u8) -> io::Result<()> {
   let name = read_string_bounded(r)?;
   let prefix_count = read_u32(r)? as usize;
   if prefix_count > 4096 {
      return Err(io::Error::new(
         io::ErrorKind::InvalidData,
         "index prefix count too large",
      ));
   }
   let mut prefixes = Vec::with_capacity(prefix_count.min(16));
   for _ in 0..prefix_count {
      prefixes.push(read_string_bounded(r)?);
   }
   let storage = match read_u8(r)? {
      0 => crate::search::StorageKind::Hash,
      1 => crate::search::StorageKind::Json,
      _ => {
         return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad index storage kind",
         ));
      }
   };
   let field_count = read_u32(r)? as usize;
   if field_count > 1024 {
      return Err(io::Error::new(
         io::ErrorKind::InvalidData,
         "index field count too large",
      ));
   }
   let mut fields = Vec::with_capacity(field_count.min(64));
   for _ in 0..field_count {
      let field_name = read_string_bounded(r)?;
      let field_type = match read_u8(r)? {
         0 => crate::search::FieldType::Numeric,
         1 => crate::search::FieldType::Tag,
         2 => crate::search::FieldType::Text,
         3 => crate::search::FieldType::Vector,
         _ => {
            return Err(io::Error::new(
               io::ErrorKind::InvalidData,
               "bad index field type",
            ));
         }
      };
      let flags = read_u8(r)?;
      let separator = read_u8(r)? as char;
      let mut field = crate::search::FieldDef {
         name: field_name,
         alias: None,
         field_type,
         sortable: flags & 1 != 0,
         no_index: flags & 2 != 0,
         separator,
         vector: None,
      };
      if field_type == crate::search::FieldType::Vector {
         let dims = read_u32(r)? as usize;
         let metric = match read_u8(r)? {
            0 => crate::search::DistanceMetric::L2,
            1 => crate::search::DistanceMetric::Cosine,
            2 => crate::search::DistanceMetric::Ip,
            _ => {
               return Err(io::Error::new(
                  io::ErrorKind::InvalidData,
                  "bad index metric",
               ));
            }
         };
         let algorithm = if read_u8(r)? == 1 { "HNSW" } else { "FLAT" };
         // v2 snapshots predate persisted M / EF_CONSTRUCTION / type.
         let (m, ef_construction, dtype, quant) = if version >= 3 {
            let m = read_u32(r)? as usize;
            let ef = read_u32(r)? as usize;
            let dtype = crate::search::canonical_dtype(&read_string_bounded(r)?)
               .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad vector type"))?;
            let quant = fyro_vector::Quant::parse(&read_string_bounded(r)?).ok_or_else(|| {
               io::Error::new(io::ErrorKind::InvalidData, "bad vector quantization")
            })?;
            (m, ef, dtype, quant)
         } else {
            // v2 graphs were exact f32; keep them that way.
            (
               fyro_vector::hnsw::DEFAULT_M,
               100,
               "FLOAT32",
               fyro_vector::Quant::None,
            )
         };
         field.vector = Some(crate::search::VectorAttrs {
            dims,
            metric,
            algorithm,
            dtype,
            m,
            ef_construction,
            quant: if algorithm == "HNSW" {
               quant
            } else {
               fyro_vector::Quant::None
            },
         });
      }
      fields.push(field);
   }
   let def = crate::search::IndexDef {
      name,
      prefixes,
      storage,
      fields,
      created_at_ms: now_ms(),
      num_docs: std::sync::atomic::AtomicU64::new(0),
   };
   let _ = store.search_state().create(def);
   Ok(())
}
