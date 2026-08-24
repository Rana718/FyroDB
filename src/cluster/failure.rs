use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureReport {
    pub target_id: String,
    pub reporter_id: String,
    pub epoch: u64,
}

struct Evidence {
    reporters: HashSet<String>,
    updated_at: Instant,
}

/// Collects bounded, epoch-scoped failure evidence. It never mutates topology;
/// failover code must separately apply fencing and slot-ownership rules.
pub struct FailureTracker {
    quorum: usize,
    retention: Duration,
    evidence: HashMap<(String, u64), Evidence>,
}

impl FailureTracker {
    pub fn new(quorum: usize, retention: Duration) -> Self {
        Self {
            quorum: quorum.max(1),
            retention,
            evidence: HashMap::new(),
        }
    }

    pub fn report(&mut self, report: FailureReport) -> bool {
        self.expire();
        let evidence = self
            .evidence
            .entry((report.target_id, report.epoch))
            .or_insert_with(|| Evidence {
                reporters: HashSet::new(),
                updated_at: Instant::now(),
            });
        evidence.updated_at = Instant::now();
        evidence.reporters.insert(report.reporter_id);
        evidence.reporters.len() >= self.quorum
    }

    pub fn report_count(&self, target_id: &str, epoch: u64) -> usize {
        self.evidence
            .get(&(target_id.to_owned(), epoch))
            .map_or(0, |value| value.reporters.len())
    }

    pub fn quorum(&self) -> usize {
        self.quorum
    }

    pub fn expire(&mut self) {
        let retention = self.retention;
        self.evidence
            .retain(|_, value| value.updated_at.elapsed() <= retention);
    }
}

pub fn encode_failure_report(report: &FailureReport) -> Option<Vec<u8>> {
    if report.target_id.len() > u16::MAX as usize || report.reporter_id.len() > u16::MAX as usize {
        return None;
    }
    let mut out = Vec::with_capacity(12 + report.target_id.len() + report.reporter_id.len());
    out.extend_from_slice(&report.epoch.to_be_bytes());
    out.extend_from_slice(&(report.target_id.len() as u16).to_be_bytes());
    out.extend_from_slice(report.target_id.as_bytes());
    out.extend_from_slice(&(report.reporter_id.len() as u16).to_be_bytes());
    out.extend_from_slice(report.reporter_id.as_bytes());
    Some(out)
}

pub fn decode_failure_report(mut input: &[u8]) -> Option<FailureReport> {
    let epoch = u64::from_be_bytes(take(&mut input, 8)?.try_into().ok()?);
    let target_len = u16::from_be_bytes(take(&mut input, 2)?.try_into().ok()?) as usize;
    let target_id = String::from_utf8(take(&mut input, target_len)?.to_vec()).ok()?;
    let reporter_len = u16::from_be_bytes(take(&mut input, 2)?.try_into().ok()?) as usize;
    let reporter_id = String::from_utf8(take(&mut input, reporter_len)?.to_vec()).ok()?;
    if !input.is_empty() || target_id.is_empty() || reporter_id.is_empty() {
        return None;
    }
    Some(FailureReport {
        target_id,
        reporter_id,
        epoch,
    })
}

fn take<'a>(input: &mut &'a [u8], count: usize) -> Option<&'a [u8]> {
    if input.len() < count {
        return None;
    }
    let (value, rest) = input.split_at(count);
    *input = rest;
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_unique_reporters_for_quorum() {
        let mut tracker = FailureTracker::new(2, Duration::from_secs(10));
        let report = |reporter: &str| FailureReport {
            target_id: "b".into(),
            reporter_id: reporter.into(),
            epoch: 4,
        };
        assert!(!tracker.report(report("a")));
        assert!(!tracker.report(report("a")));
        assert!(tracker.report(report("c")));
        assert_eq!(tracker.report_count("b", 4), 2);
    }

    #[test]
    fn report_binary_round_trip_is_strict() {
        let report = FailureReport {
            target_id: "b".into(),
            reporter_id: "a".into(),
            epoch: 4,
        };
        let bytes = encode_failure_report(&report).unwrap();
        assert_eq!(decode_failure_report(&bytes), Some(report));
        assert!(decode_failure_report(&bytes[..bytes.len() - 1]).is_none());
    }
}
