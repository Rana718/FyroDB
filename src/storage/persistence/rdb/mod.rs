use crate::storage::{
    store::Store,
    value::{FyroDB, HashInner, ListInner, SetInner, SmallStr, StoreValue, now_ms},
};
use foldhash::{HashMap, HashMapExt, HashSetExt};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::time::Duration;

mod format;
mod index;
mod load;
mod meta;
mod save;
mod stream;

use format::*;
use index::*;
use stream::*;
pub use load::{decode_single_value, load, load_strict};
pub use meta::*;
pub use save::{background_save_loop, encode_single_value, save};

#[cfg(test)]
mod cluster_metadata_tests {
    use super::*;

    #[test]
    fn cluster_metadata_round_trip_is_strict() {
        let path = std::env::temp_dir().join(format!("fyrodb-cluster-meta-{}", std::process::id()));

        let path = path.to_str().unwrap();
        save_cluster_metadata(path, "node-a", 42).unwrap();
        assert_eq!(
            load_cluster_metadata(path).unwrap(),
            Some(("node-a".into(), 42))
        );
        let _ = std::fs::remove_file(path);
    }
}
