use crate::storage::geo::{GeoCenter, GeoShape, GeoUnit};
use crate::storage::store::Store;
use crate::utils::resp;
use crate::utils::util::format_float;
use crate::{parse_float, parse_int, store_ok, wt};

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

    let mut items = Vec::with_capacity(coords.len() / 3);
    for chunk in coords.chunks(3) {
        let lon = parse_float!(out, chunk[0]);
        let lat = parse_float!(out, chunk[1]);
        if !(-180.0..=180.0).contains(&lon) || !(-85.05112878..=85.05112878).contains(&lat) {
            return resp::write_err(out, "invalid longitude,latitude pair");
        }
        items.push((lon, lat, chunk[2].to_string()));
    }

    resp::write_integer(
        out,
        store_ok!(out, store.geoadd(key, &items, nx, xx, ch)) as i64,
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
                        resp::write_bulk(out, &format_float(lon));
                        resp::write_bulk(out, &format_float(lat));
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
        Ok(Some(d)) => resp::write_bulk(out, &format_float(d)),
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

    let mut center: Option<GeoCenter> = None;
    let mut shape: Option<GeoShape> = None;
    let mut asc = true;
    let mut count = 0usize;
    let mut withcoord = false;
    let mut withdist = false;
    let mut i = 0;

    while i < rest.len() {
        if rest[i].eq_ignore_ascii_case("FROMMEMBER") {
            i += 1;
            if i >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            center = Some(GeoCenter::Member(rest[i].to_string()));
        } else if rest[i].eq_ignore_ascii_case("FROMLONLAT") {
            if i + 2 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            let lon = parse_float!(out, rest[i + 1]);
            let lat = parse_float!(out, rest[i + 2]);
            center = Some(GeoCenter::LonLat(lon, lat));
            i += 2;
        } else if rest[i].eq_ignore_ascii_case("BYRADIUS") {
            if i + 2 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            let radius = parse_float!(out, rest[i + 1]);
            let unit = match GeoUnit::parse_str(rest[i + 2]) {
                Some(u) => u,
                None => return resp::write_err(out, "unsupported unit"),
            };
            shape = Some(GeoShape::Radius(radius, unit));
            i += 2;
        } else if rest[i].eq_ignore_ascii_case("BYBOX") {
            if i + 3 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            let w = parse_float!(out, rest[i + 1]);
            let h = parse_float!(out, rest[i + 2]);
            let unit = match GeoUnit::parse_str(rest[i + 3]) {
                Some(u) => u,
                None => return resp::write_err(out, "unsupported unit"),
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
                return resp::write_err(out, "syntax error");
            }
            count = parse_int!(out, rest[i], usize);
            if i + 1 < rest.len() && rest[i + 1].eq_ignore_ascii_case("ANY") {
                i += 1;
            }
        } else if rest[i].eq_ignore_ascii_case("WITHCOORD") {
            withcoord = true;
        } else if rest[i].eq_ignore_ascii_case("WITHDIST") {
            withdist = true;
        } else if rest[i].eq_ignore_ascii_case("WITHHASH") {
        }
        i += 1;
    }

    let c = match center {
        Some(c) => c,
        None => return resp::write_err(out, "syntax error"),
    };
    let s = match shape {
        Some(s) => s,
        None => return resp::write_err(out, "syntax error"),
    };

    let results = store_ok!(
        out,
        store.geosearch(
            key,
            crate::storage::geo::GeoSearchQuery {
                center: c,
                shape: s,
                asc,
                count,
            },
        )
    );

    resp::write_array_header(out, results.len());
    for r in &results {
        if withcoord || withdist {
            let mut fields = 1;
            if withdist {
                fields += 1;
            }
            if withcoord {
                fields += 1;
            }
            resp::write_array_header(out, fields);
            resp::write_bulk(out, &r.member);
            if withdist {
                resp::write_bulk(out, &format_float(r.dist));
            }
            if withcoord {
                resp::write_array_header(out, 2);
                resp::write_bulk(out, &format_float(r.lon));
                resp::write_bulk(out, &format_float(r.lat));
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

    let mut center: Option<GeoCenter> = None;
    let mut shape: Option<GeoShape> = None;
    let mut asc = true;
    let mut count = 0usize;
    let mut storedist = false;
    let mut i = 0;

    while i < rest.len() {
        if rest[i].eq_ignore_ascii_case("FROMMEMBER") {
            i += 1;
            if i >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            center = Some(GeoCenter::Member(rest[i].to_string()));
        } else if rest[i].eq_ignore_ascii_case("FROMLONLAT") {
            if i + 2 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            let lon = parse_float!(out, rest[i + 1]);
            let lat = parse_float!(out, rest[i + 2]);
            center = Some(GeoCenter::LonLat(lon, lat));
            i += 2;
        } else if rest[i].eq_ignore_ascii_case("BYRADIUS") {
            if i + 2 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            let radius = parse_float!(out, rest[i + 1]);
            let unit = match GeoUnit::parse_str(rest[i + 2]) {
                Some(u) => u,
                None => return resp::write_err(out, "unsupported unit"),
            };
            shape = Some(GeoShape::Radius(radius, unit));
            i += 2;
        } else if rest[i].eq_ignore_ascii_case("BYBOX") {
            if i + 3 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            let w = parse_float!(out, rest[i + 1]);
            let h = parse_float!(out, rest[i + 2]);
            let unit = match GeoUnit::parse_str(rest[i + 3]) {
                Some(u) => u,
                None => return resp::write_err(out, "unsupported unit"),
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
                return resp::write_err(out, "syntax error");
            }
            count = parse_int!(out, rest[i], usize);
            if i + 1 < rest.len() && rest[i + 1].eq_ignore_ascii_case("ANY") {
                i += 1;
            }
        } else if rest[i].eq_ignore_ascii_case("STOREDIST") {
            storedist = true;
        }
        i += 1;
    }

    let c = match center {
        Some(c) => c,
        None => return resp::write_err(out, "syntax error"),
    };
    let s = match shape {
        Some(s) => s,
        None => return resp::write_err(out, "syntax error"),
    };

    resp::write_integer(
        out,
        store_ok!(
            out,
            store.geosearchstore(
                dst,
                src,
                crate::storage::geo::GeoSearchQuery {
                    center: c,
                    shape: s,
                    asc,
                    count,
                },
                storedist,
            )
        ) as i64,
    );
}
