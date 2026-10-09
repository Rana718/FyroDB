use super::*;

pub fn save_cluster_metadata(path: &str, node_id: &str, epoch: u64) -> io::Result<()> {
    if node_id.is_empty() || node_id.len() > u16::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid node id",
        ));
    }
    let tmp = format!("{path}.tmp");
    let mut file = File::create(&tmp)?;
    file.write_all(CLUSTER_MAGIC)?;
    file.write_all(&[CLUSTER_VERSION])?;
    file.write_all(&(node_id.len() as u16).to_le_bytes())?;
    file.write_all(node_id.as_bytes())?;
    file.write_all(&epoch.to_le_bytes())?;
    file.flush()?;
    fsync_file(&file)?;
    fs::rename(tmp, path)
}

pub fn load_cluster_metadata(path: &str) -> io::Result<Option<(String, u64)>> {
    if !Path::new(path).exists() {
        return Ok(None);
    }
    let mut file = File::open(path)?;
    let mut header = [0u8; 7];
    file.read_exact(&mut header)?;
    if &header[..4] != CLUSTER_MAGIC || header[4] != CLUSTER_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid cluster metadata",
        ));
    }
    let len = u16::from_le_bytes([header[5], header[6]]) as usize;
    let mut id = vec![0u8; len];
    file.read_exact(&mut id)?;
    let mut epoch = [0u8; 8];
    file.read_exact(&mut epoch)?;
    let id = String::from_utf8(id)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid node id"))?;
    Ok(Some((id, u64::from_le_bytes(epoch))))
}



pub fn save_replication_metadata(path: &str, epoch: u64, offset: u64) -> io::Result<()> {
    let tmp = format!("{path}.tmp");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(REPL_MAGIC)?;
        file.write_all(&[REPL_VERSION])?;
        file.write_all(&epoch.to_le_bytes())?;
        file.write_all(&offset.to_le_bytes())?;
        file.flush()?;
        fsync_file(&file)?;
    }
    fs::rename(tmp, path)?;
    Ok(())
}

pub fn load_replication_metadata(path: &str) -> io::Result<Option<(u64, u64)>> {
    if !Path::new(path).exists() {
        return Ok(None);
    }
    let mut file = File::open(path)?;
    let mut bytes = [0u8; 21];
    file.read_exact(&mut bytes)?;
    if &bytes[..4] != REPL_MAGIC || bytes[4] != REPL_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid replication metadata",
        ));
    }
    Ok(Some((
        u64::from_le_bytes(bytes[5..13].try_into().unwrap()),
        u64::from_le_bytes(bytes[13..21].try_into().unwrap()),
    )))
}
