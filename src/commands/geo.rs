use crate::storage::geo::{GeoCenter, GeoShape, GeoUnit};
use crate::storage::store::Store;
use crate::utils::resp;
use crate::{parse_float, store_ok, wt};

pub fn geoadd(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "geoadd");
   };
   if rest.is_empty() {
      return resp::write_wrong_args(out, "geoadd");
   }

   let mut nx = false;
   let mut xx = false;
   let mut ch = false;
   let mut i = 0;

   loop {
      if i >= rest.len() {
         return resp::write_wrong_args(out, "geoadd");
      }
      if rest[i].eq_ignore_ascii_case("NX") {
         nx = true;
      } else if rest[i].eq_ignore_ascii_case("XX") {
         xx = true;
      } else if rest[i].eq_ignore_ascii_case("CH") {
         ch = true;
      } else {
         break;
      }
      i += 1;
   }

   let coords = &rest[i..];
   if coords.is_empty() || coords.len() % 3 != 0 {
      return resp::write_wrong_args(out, "geoadd");
   }

   const STACK_ITEMS: usize = 16;
   let n_items = coords.len() / 3;
   let mut stack_items = [(0.0f64, 0.0f64, ""); STACK_ITEMS];
   let mut items_vec;
   let items: &[(f64, f64, &str)] = if n_items <= STACK_ITEMS {
      for (idx, &[lon_str, lat_str, member]) in coords.as_chunks::<3>().0.iter().enumerate() {
         let lon = parse_float!(out, lon_str);
         let lat = parse_float!(out, lat_str);
         if !(-180.0..=180.0).contains(&lon) || !(-85.05112878..=85.05112878).contains(&lat) {
            return resp::write_err(out, "invalid longitude,latitude pair");
         }
         stack_items[idx] = (lon, lat, member);
      }
      &stack_items[..n_items]
   } else {
      items_vec = Vec::with_capacity(n_items);
      for &[lon_str, lat_str, member] in coords.as_chunks::<3>().0 {
         let lon = parse_float!(out, lon_str);
         let lat = parse_float!(out, lat_str);
         if !(-180.0..=180.0).contains(&lon) || !(-85.05112878..=85.05112878).contains(&lat) {
            return resp::write_err(out, "invalid longitude,latitude pair");
         }
         items_vec.push((lon, lat, member));
      }
      &items_vec
   };
   resp::write_integer(
      out,
      store_ok!(out, store.geoadd(key, items, nx, xx, ch)) as i64,
   );
}

pub fn geopos(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, key, members @ ..] if !members.is_empty() => {
         let positions = wt!(out, store.geopos(key, members));
         resp::write_array_header(out, positions.len());
         for pos in positions {
            match pos {
               Some((lon, lat)) => {
                  resp::write_array_header(out, 2);
                  resp::write_float(out, lon);
                  resp::write_float(out, lat);
               }
               None => resp::write_nil(out),
            }
         }
      }
      _ => resp::write_wrong_args(out, "geopos"),
   }
}

pub fn geodist(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let (key, m1, m2, unit) = match parts {
      [_, key, m1, m2] => (*key, *m1, *m2, GeoUnit::M),
      [_, key, m1, m2, u] => {
         let unit = match GeoUnit::parse_str(u) {
            Some(u) => u,
            None => return resp::write_err(out, "unsupported unit"),
         };
         (*key, *m1, *m2, unit)
      }
      _ => return resp::write_wrong_args(out, "geodist"),
   };
   match store.geodist(key, m1, m2, unit) {
      Ok(Some(d)) => resp::write_float(out, d),
      Ok(None) => resp::write_nil(out),
      Err(e) => resp::write_store_err(out, e),
   }
}

pub fn geohash(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, key, members @ ..] if !members.is_empty() => {
         let hashes = wt!(out, store.geohash(key, members));
         resp::write_array_header(out, hashes.len());
         for h in hashes {
            match h {
               Some(s) => resp::write_bulk(out, &s),
               None => resp::write_nil(out),
            }
         }
      }
      _ => resp::write_wrong_args(out, "geohash"),
   }
}

pub fn geosearch(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "geosearch");
   };

   let Some(p) = parse_geosearch_params(rest, out) else {
      return;
   };

   let results = store_ok!(out, store.geosearch(key, p.query));

   resp::write_array_header(out, results.len());
   for r in &results {
      if p.withcoord || p.withdist {
         let mut fields = 1;
         if p.withdist {
            fields += 1;
         }
         if p.withcoord {
            fields += 1;
         }
         resp::write_array_header(out, fields);
         resp::write_bulk(out, &r.member);
         if p.withdist {
            resp::write_float(out, r.dist);
         }
         if p.withcoord {
            resp::write_array_header(out, 2);
            resp::write_float(out, r.lon);
            resp::write_float(out, r.lat);
         }
      } else {
         resp::write_bulk(out, &r.member);
      }
   }
}

pub fn geosearchstore(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, dst, src, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "geosearchstore");
   };
   let Some(p) = parse_geosearch_params(rest, out) else {
      return;
   };
   resp::write_integer(
      out,
      store_ok!(out, store.geosearchstore(dst, src, p.query, p.storedist)) as i64,
   );
}

struct GeoSearchParams {
   query: crate::storage::geo::GeoSearchQuery,
   withcoord: bool,
   withdist: bool,
   storedist: bool,
}

fn parse_geosearch_params(rest: &[&str], out: &mut Vec<u8>) -> Option<GeoSearchParams> {
   let mut center: Option<GeoCenter> = None;
   let mut shape: Option<GeoShape> = None;
   let mut asc = true;
   let mut count = 0usize;
   let mut withcoord = false;
   let mut withdist = false;
   let mut storedist = false;
   let mut i = 0;

   while i < rest.len() {
      if rest[i].eq_ignore_ascii_case("FROMMEMBER") {
         i += 1;
         if i >= rest.len() {
            resp::write_err(out, "syntax error");
            return None;
         }
         center = Some(GeoCenter::Member(rest[i].to_string()));
      } else if rest[i].eq_ignore_ascii_case("FROMLONLAT") {
         if i + 2 >= rest.len() {
            resp::write_err(out, "syntax error");
            return None;
         }
         let lon = match rest[i + 1].parse::<f64>() {
            Ok(n) => n,
            Err(_) => {
               resp::write_err(out, "value is not a float");
               return None;
            }
         };
         let lat = match rest[i + 2].parse::<f64>() {
            Ok(n) => n,
            Err(_) => {
               resp::write_err(out, "value is not a float");
               return None;
            }
         };
         center = Some(GeoCenter::LonLat(lon, lat));
         i += 2;
      } else if rest[i].eq_ignore_ascii_case("BYRADIUS") {
         if i + 2 >= rest.len() {
            resp::write_err(out, "syntax error");
            return None;
         }
         let radius = match rest[i + 1].parse::<f64>() {
            Ok(n) => n,
            Err(_) => {
               resp::write_err(out, "value is not a float");
               return None;
            }
         };
         let unit = match GeoUnit::parse_str(rest[i + 2]) {
            Some(u) => u,
            None => {
               resp::write_err(out, "unsupported unit");
               return None;
            }
         };
         shape = Some(GeoShape::Radius(radius, unit));
         i += 2;
      } else if rest[i].eq_ignore_ascii_case("BYBOX") {
         if i + 3 >= rest.len() {
            resp::write_err(out, "syntax error");
            return None;
         }
         let w = match rest[i + 1].parse::<f64>() {
            Ok(n) => n,
            Err(_) => {
               resp::write_err(out, "value is not a float");
               return None;
            }
         };
         let h = match rest[i + 2].parse::<f64>() {
            Ok(n) => n,
            Err(_) => {
               resp::write_err(out, "value is not a float");
               return None;
            }
         };
         let unit = match GeoUnit::parse_str(rest[i + 3]) {
            Some(u) => u,
            None => {
               resp::write_err(out, "unsupported unit");
               return None;
            }
         };
         shape = Some(GeoShape::Box(w, h, unit));
         i += 3;
      } else if rest[i].eq_ignore_ascii_case("ASC") {
         asc = true;
      } else if rest[i].eq_ignore_ascii_case("DESC") {
         asc = false;
      } else if rest[i].eq_ignore_ascii_case("COUNT") {
         i += 1;
         if i >= rest.len() {
            resp::write_err(out, "syntax error");
            return None;
         }
         count = match rest[i].parse::<usize>() {
            Ok(n) => n,
            Err(_) => {
               resp::write_err(out, "value is not an integer");
               return None;
            }
         };
         if i + 1 < rest.len() && rest[i + 1].eq_ignore_ascii_case("ANY") {
            i += 1;
         }
      } else if rest[i].eq_ignore_ascii_case("WITHCOORD") {
         withcoord = true;
      } else if rest[i].eq_ignore_ascii_case("WITHDIST") {
         withdist = true;
      } else if rest[i].eq_ignore_ascii_case("WITHHASH") {
      } else if rest[i].eq_ignore_ascii_case("STOREDIST") {
         storedist = true;
      }
      i += 1;
   }

   let center = match center {
      Some(c) => c,
      None => {
         resp::write_err(out, "syntax error");
         return None;
      }
   };
   let shape = match shape {
      Some(s) => s,
      None => {
         resp::write_err(out, "syntax error");
         return None;
      }
   };

   Some(GeoSearchParams {
      query: crate::storage::geo::GeoSearchQuery {
         center,
         shape,
         asc,
         count,
      },
      withcoord,
      withdist,
      storedist,
   })
}
