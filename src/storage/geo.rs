use crate::storage::store::Store;
use crate::storage::value::{StoreValue, ZSetData};
use std::f64::consts::PI;

impl Store {
    pub fn geoadd(
        &self,
        key: &str,
        items: &[(f64, f64, String)],
        nx: bool,
        xx: bool,
        ch: bool,
    ) -> Result<usize, &'static str> {
        // Geohash interning: distinct members may hash to the same score, so
        // members are owned by this Vec while zadd borrows them.
        let owned: Vec<(f64, String)> = items
            .iter()
            .map(|(lon, lat, member)| {
                let hash = geohash_encode(*lon, *lat);
                (f64::from_bits(hash), member.clone())
            })
            .collect();
        let members: Vec<(f64, &str)> = owned
            .iter()
            .map(|(score, member)| (*score, member.as_str()))
            .collect();

        self.zadd(
            key,
            &members,
            crate::storage::zset::ZAddOptions {
                nx,
                xx,
                ch,
                ..Default::default()
            },
        )
    }

    pub fn geopos(
        &self,
        key: &str,
        members: &[&str],
    ) -> Result<Vec<Option<(f64, f64)>>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![None; members.len()]),
            Some(e) if e.is_expired() => Ok(vec![None; members.len()]),
            Some(e) => match e.value.as_zset() {
                Some(z) => Ok(members
                    .iter()
                    .map(|m| {
                        z.get_score(m).map(|score| {
                            let bits = score.to_bits();
                            geohash_decode(bits)
                        })
                    })
                    .collect()),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn geodist(
        &self,
        key: &str,
        member1: &str,
        member2: &str,
        unit: GeoUnit,
    ) -> Result<Option<f64>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(None),
            Some(e) if e.is_expired() => Ok(None),
            Some(e) => match e.value.as_zset() {
                Some(z) => {
                    let s1 = match z.get_score(member1) {
                        Some(s) => s,
                        None => return Ok(None),
                    };
                    let s2 = match z.get_score(member2) {
                        Some(s) => s,
                        None => return Ok(None),
                    };
                    let (lon1, lat1) = geohash_decode(s1.to_bits());
                    let (lon2, lat2) = geohash_decode(s2.to_bits());
                    let dist = haversine(lat1, lon1, lat2, lon2);
                    Ok(Some(unit.from_meters(dist)))
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn geohash(
        &self,
        key: &str,
        members: &[&str],
    ) -> Result<Vec<Option<String>>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![None; members.len()]),
            Some(e) if e.is_expired() => Ok(vec![None; members.len()]),
            Some(e) => match e.value.as_zset() {
                Some(z) => Ok(members
                    .iter()
                    .map(|m| {
                        z.get_score(m).map(|score| {
                            let bits = score.to_bits();
                            encode_geohash_string(bits)
                        })
                    })
                    .collect()),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn geosearch(
        &self,
        key: &str,
        query: GeoSearchQuery,
    ) -> Result<Vec<GeoResult>, &'static str> {
        let (clon, clat) = match &query.center {
            GeoCenter::LonLat(lon, lat) => (*lon, *lat),
            GeoCenter::Member(m) => {
                let positions = self.geopos(key, &[m])?;
                match positions.into_iter().next().flatten() {
                    Some(pos) => pos,
                    None => return Ok(vec![]),
                }
            }
        };

        match self.data.get_ref(key) {
            None => Ok(vec![]),
            Some(e) if e.is_expired() => Ok(vec![]),
            Some(e) => match e.value.as_zset() {
                Some(z) => {
                    let mut results: Vec<GeoResult> = Vec::new();
                    for entry in z.iter() {
                        let (lon, lat) = geohash_decode(entry.score.to_bits());
                        let dist = haversine(clat, clon, lat, lon);
                        let in_range = match &query.shape {
                            GeoShape::Radius(r, unit) => dist <= unit.to_meters(*r),
                            GeoShape::Box(w, h, unit) => {
                                let half_w = unit.to_meters(*w) / 2.0;
                                let half_h = unit.to_meters(*h) / 2.0;
                                let dx = haversine(clat, clon, clat, lon);
                                let dy = haversine(clat, clon, lat, clon);
                                dx <= half_w && dy <= half_h
                            }
                        };
                        if in_range {
                            results.push(GeoResult {
                                member: entry.member.to_string(),
                                dist,
                                lon,
                                lat,
                                hash: entry.score.to_bits(),
                            });
                        }
                    }
                    if query.asc {
                        results.sort_by(|a, b| {
                            a.dist
                                .partial_cmp(&b.dist)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });
                    } else {
                        results.sort_by(|a, b| {
                            b.dist
                                .partial_cmp(&a.dist)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });
                    }
                    if query.count > 0 && results.len() > query.count {
                        results.truncate(query.count);
                    }
                    Ok(results)
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn geosearchstore(
        &self,
        dst: &str,
        src: &str,
        query: GeoSearchQuery,
        storedist: bool,
    ) -> Result<usize, &'static str> {
        let results = self.geosearch(src, query)?;
        let mut z = ZSetData::new();
        for r in &results {
            let score = if storedist {
                r.dist
            } else {
                f64::from_bits(r.hash)
            };
            z.insert(score, r.member.as_str());
        }
        let n = z.len();
        self.data.insert(dst.to_string(), StoreValue::zset(z));
        Ok(n)
    }
}

#[derive(Clone)]
pub enum GeoCenter {
    LonLat(f64, f64),
    Member(String),
}

#[derive(Clone)]
pub enum GeoShape {
    Radius(f64, GeoUnit),
    Box(f64, f64, GeoUnit),
}

/// Center + shape + order/limit; reply flags belong to the command layer.
#[derive(Clone)]
pub struct GeoSearchQuery {
    pub center: GeoCenter,
    pub shape: GeoShape,
    pub asc: bool,
    pub count: usize,
}

#[derive(Clone, Copy)]
pub enum GeoUnit {
    M,
    Km,
    Ft,
    Mi,
}

impl GeoUnit {
    pub fn parse_str(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("m") {
            Some(Self::M)
        } else if s.eq_ignore_ascii_case("km") {
            Some(Self::Km)
        } else if s.eq_ignore_ascii_case("ft") {
            Some(Self::Ft)
        } else if s.eq_ignore_ascii_case("mi") {
            Some(Self::Mi)
        } else {
            None
        }
    }

    pub fn to_meters(self, val: f64) -> f64 {
        match self {
            Self::M => val,
            Self::Km => val * 1000.0,
            Self::Ft => val * 0.3048,
            Self::Mi => val * 1609.344,
        }
    }

    pub fn from_meters(self, meters: f64) -> f64 {
        match self {
            Self::M => meters,
            Self::Km => meters / 1000.0,
            Self::Ft => meters / 0.3048,
            Self::Mi => meters / 1609.344,
        }
    }
}

pub struct GeoResult {
    pub member: String,
    pub dist: f64,
    pub lon: f64,
    pub lat: f64,
    pub hash: u64,
}

fn geohash_encode(lon: f64, lat: f64) -> u64 {
    let mut lon_range = (-180.0f64, 180.0f64);
    let mut lat_range = (-85.05112878f64, 85.05112878f64);
    let mut bits: u64 = 0;

    for i in 0..52 {
        if i % 2 == 0 {
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon >= mid {
                bits |= 1 << (51 - i);
                lon_range.0 = mid;
            } else {
                lon_range.1 = mid;
            }
        } else {
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat >= mid {
                bits |= 1 << (51 - i);
                lat_range.0 = mid;
            } else {
                lat_range.1 = mid;
            }
        }
    }
    bits
}

fn geohash_decode(bits: u64) -> (f64, f64) {
    let mut lon_range = (-180.0f64, 180.0f64);
    let mut lat_range = (-85.05112878f64, 85.05112878f64);

    for i in 0..52 {
        let bit = (bits >> (51 - i)) & 1;
        if i % 2 == 0 {
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if bit == 1 {
                lon_range.0 = mid;
            } else {
                lon_range.1 = mid;
            }
        } else {
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if bit == 1 {
                lat_range.0 = mid;
            } else {
                lat_range.1 = mid;
            }
        }
    }
    let lon = (lon_range.0 + lon_range.1) / 2.0;
    let lat = (lat_range.0 + lat_range.1) / 2.0;
    (lon, lat)
}

fn encode_geohash_string(bits: u64) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let mut result = String::with_capacity(11);
    let mut hash = bits;
    for _ in 0..11 {
        let idx = ((hash >> 47) & 0x1F) as usize;
        result.push(ALPHABET[idx] as char);
        hash <<= 5;
    }
    result
}

fn haversine(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS: f64 = 6372797.560856;
    let lat1_r = lat1 * PI / 180.0;
    let lat2_r = lat2 * PI / 180.0;
    let dlat = (lat2 - lat1) * PI / 180.0;
    let dlon = (lon2 - lon1) * PI / 180.0;

    let a = (dlat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS * c
}
