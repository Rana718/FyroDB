mod common;
use common::*;
use fyro_db::storage::geo::{GeoCenter, GeoSearchQuery, GeoShape, GeoUnit};

#[test]
fn geoadd_creates_key() {
    let s = store();
    let items = vec![
        (13.361389, 38.115556, "Palermo".to_string()),
        (15.087269, 37.502669, "Catania".to_string()),
    ];
    assert_eq!(s.geoadd("k", &items, false, false, false), Ok(2));
}

#[test]
fn geoadd_updates_existing() {
    let s = store();
    let items = vec![(13.361389, 38.115556, "Palermo".to_string())];
    s.geoadd("k", &items, false, false, false).unwrap();
    let items2 = vec![(13.5, 38.2, "Palermo".to_string())];
    assert_eq!(s.geoadd("k", &items2, false, false, false), Ok(0));
}

#[test]
fn geopos_existing_members() {
    let s = store();
    let items = vec![
        (13.361389, 38.115556, "Palermo".to_string()),
        (15.087269, 37.502669, "Catania".to_string()),
    ];
    s.geoadd("k", &items, false, false, false).unwrap();
    let positions = s.geopos("k", &["Palermo", "Catania"]).unwrap();
    assert!(positions[0].is_some());
    assert!(positions[1].is_some());
    let (lon, lat) = positions[0].unwrap();
    assert!((lon - 13.361389).abs() < 0.01);
    assert!((lat - 38.115556).abs() < 0.01);
}

#[test]
fn geopos_missing_member() {
    let s = store();
    let items = vec![(13.361389, 38.115556, "Palermo".to_string())];
    s.geoadd("k", &items, false, false, false).unwrap();
    let positions = s.geopos("k", &["missing"]).unwrap();
    assert_eq!(positions[0], None);
}

#[test]
fn geopos_missing_key() {
    let s = store();
    let positions = s.geopos("k", &["any"]).unwrap();
    assert_eq!(positions[0], None);
}

#[test]
fn geodist_between_members() {
    let s = store();
    let items = vec![
        (13.361389, 38.115556, "Palermo".to_string()),
        (15.087269, 37.502669, "Catania".to_string()),
    ];
    s.geoadd("k", &items, false, false, false).unwrap();
    let dist = s
        .geodist("k", "Palermo", "Catania", GeoUnit::Km)
        .unwrap()
        .unwrap();
    assert!(dist > 150.0 && dist < 170.0);
}

#[test]
fn geodist_missing_member() {
    let s = store();
    let items = vec![(13.361389, 38.115556, "Palermo".to_string())];
    s.geoadd("k", &items, false, false, false).unwrap();
    assert_eq!(s.geodist("k", "Palermo", "missing", GeoUnit::M), Ok(None));
}

#[test]
fn geodist_units() {
    let s = store();
    let items = vec![(0.0, 0.0, "a".to_string()), (1.0, 0.0, "b".to_string())];
    s.geoadd("k", &items, false, false, false).unwrap();
    let dist_m = s.geodist("k", "a", "b", GeoUnit::M).unwrap().unwrap();
    let dist_km = s.geodist("k", "a", "b", GeoUnit::Km).unwrap().unwrap();
    assert!((dist_m / 1000.0 - dist_km).abs() < 1.0);
}

#[test]
fn geohash_returns_string() {
    let s = store();
    let items = vec![(13.361389, 38.115556, "Palermo".to_string())];
    s.geoadd("k", &items, false, false, false).unwrap();
    let hashes = s.geohash("k", &["Palermo"]).unwrap();
    assert!(hashes[0].is_some());
    assert_eq!(hashes[0].as_ref().unwrap().len(), 11);
}

#[test]
fn geohash_missing_member() {
    let s = store();
    let items = vec![(13.361389, 38.115556, "Palermo".to_string())];
    s.geoadd("k", &items, false, false, false).unwrap();
    let hashes = s.geohash("k", &["missing"]).unwrap();
    assert_eq!(hashes[0], None);
}

#[test]
fn geosearch_by_radius() {
    let s = store();
    let items = vec![
        (13.361389, 38.115556, "Palermo".to_string()),
        (15.087269, 37.502669, "Catania".to_string()),
        (2.349014, 48.864716, "Paris".to_string()),
    ];
    s.geoadd("k", &items, false, false, false).unwrap();
    let results = s
        .geosearch(
            "k",
            GeoSearchQuery {
                center: GeoCenter::LonLat(15.0, 37.0),
                shape: GeoShape::Radius(200.0, GeoUnit::Km),
                asc: true,
                count: 0,
            },
        )
        .unwrap();
    let members: Vec<&str> = results.iter().map(|r| r.member.as_str()).collect();
    assert!(members.contains(&"Palermo"));
    assert!(members.contains(&"Catania"));
    assert!(!members.contains(&"Paris"));
}

#[test]
fn geosearch_by_radius_with_count() {
    let s = store();
    let items = vec![
        (13.361389, 38.115556, "Palermo".to_string()),
        (15.087269, 37.502669, "Catania".to_string()),
        (14.0, 37.8, "Middle".to_string()),
    ];
    s.geoadd("k", &items, false, false, false).unwrap();
    let results = s
        .geosearch(
            "k",
            GeoSearchQuery {
                center: GeoCenter::LonLat(14.0, 37.8),
                shape: GeoShape::Radius(500.0, GeoUnit::Km),
                asc: true,
                count: 1,
            },
        )
        .unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn geosearch_from_member() {
    let s = store();
    let items = vec![
        (13.361389, 38.115556, "Palermo".to_string()),
        (15.087269, 37.502669, "Catania".to_string()),
    ];
    s.geoadd("k", &items, false, false, false).unwrap();
    let results = s
        .geosearch(
            "k",
            GeoSearchQuery {
                center: GeoCenter::Member("Palermo".to_string()),
                shape: GeoShape::Radius(200.0, GeoUnit::Km),
                asc: true,
                count: 0,
            },
        )
        .unwrap();
    assert!(!results.is_empty());
}

#[test]
fn geosearchstore_stores_results() {
    let s = store();
    let items = vec![
        (13.361389, 38.115556, "Palermo".to_string()),
        (15.087269, 37.502669, "Catania".to_string()),
    ];
    s.geoadd("k", &items, false, false, false).unwrap();
    let count = s
        .geosearchstore(
            "dst",
            "k",
            GeoSearchQuery {
                center: GeoCenter::LonLat(15.0, 37.5),
                shape: GeoShape::Radius(200.0, GeoUnit::Km),
                asc: true,
                count: 0,
            },
            false,
        )
        .unwrap();
    assert!(count >= 1);
    assert_eq!(s.zcard("dst"), Ok(count));
}

#[test]
fn geoadd_wrong_type() {
    let s = store();
    set_str(&s, "k", "val");
    let items = vec![(1.0, 1.0, "a".to_string())];
    assert_eq!(s.geoadd("k", &items, false, false, false), Err("WRONGTYPE"));
}
