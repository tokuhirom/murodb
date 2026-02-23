use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct CheckpointPolicy {
    pub(super) tx_threshold: u64,
    pub(super) wal_bytes_threshold: u64,
    pub(super) interval_ms: u64,
}

impl CheckpointPolicy {
    pub(super) fn from_env() -> Self {
        Self {
            tx_threshold: parse_checkpoint_env_u64(
                "MURODB_CHECKPOINT_TX_THRESHOLD",
                DEFAULT_CHECKPOINT_TX_THRESHOLD,
                0,
            ),
            wal_bytes_threshold: parse_checkpoint_env_u64(
                "MURODB_CHECKPOINT_WAL_BYTES_THRESHOLD",
                DEFAULT_CHECKPOINT_WAL_BYTES_THRESHOLD,
                0,
            ),
            interval_ms: parse_checkpoint_env_u64(
                "MURODB_CHECKPOINT_INTERVAL_MS",
                DEFAULT_CHECKPOINT_INTERVAL_MS,
                0,
            ),
        }
    }
}

impl From<CheckpointPolicy> for RuntimeConfig {
    fn from(policy: CheckpointPolicy) -> Self {
        Self {
            checkpoint_tx_threshold: policy.tx_threshold,
            checkpoint_wal_bytes_threshold: policy.wal_bytes_threshold,
            checkpoint_interval_ms: policy.interval_ms,
        }
    }
}

impl RuntimeConfig {
    pub fn defaults() -> Self {
        Self {
            checkpoint_tx_threshold: DEFAULT_CHECKPOINT_TX_THRESHOLD,
            checkpoint_wal_bytes_threshold: DEFAULT_CHECKPOINT_WAL_BYTES_THRESHOLD,
            checkpoint_interval_ms: DEFAULT_CHECKPOINT_INTERVAL_MS,
        }
    }
}

fn parse_checkpoint_env_u64(name: &str, default: u64, min: u64) -> u64 {
    let Ok(raw) = std::env::var(name) else {
        return default;
    };
    match raw.parse::<u64>() {
        Ok(v) if v >= min => v,
        Ok(_) => {
            eprintln!(
                "WARNING: {} must be >= {}, using default {}",
                name, min, default
            );
            default
        }
        Err(_) => {
            eprintln!(
                "WARNING: {} must be an integer, using default {}",
                name, default
            );
            default
        }
    }
}

impl Session {
    pub fn runtime_config(&self) -> RuntimeConfig {
        self.checkpoint_policy.into()
    }

    pub fn set_runtime_config(&mut self, config: RuntimeConfig) -> Result<()> {
        self.check_poisoned()?;
        self.refresh_from_disk_if_needed()?;
        if self.active_tx.is_some() {
            return Err(MuroError::Execution(
                "SET runtime option cannot be used inside a transaction".into(),
            ));
        }
        self.checkpoint_policy = CheckpointPolicy {
            tx_threshold: config.checkpoint_tx_threshold,
            wal_bytes_threshold: config.checkpoint_wal_bytes_threshold,
            interval_ms: config.checkpoint_interval_ms,
        };
        Ok(())
    }

    pub(super) fn handle_set_runtime_option(
        &mut self,
        stmt: &crate::sql::ast::SetRuntimeOption,
    ) -> Result<ExecResult> {
        let mut cfg = self.runtime_config();
        match stmt.option {
            crate::sql::ast::RuntimeOption::CheckpointTxThreshold => {
                cfg.checkpoint_tx_threshold = stmt.value
            }
            crate::sql::ast::RuntimeOption::CheckpointWalBytesThreshold => {
                cfg.checkpoint_wal_bytes_threshold = stmt.value
            }
            crate::sql::ast::RuntimeOption::CheckpointIntervalMs => {
                cfg.checkpoint_interval_ms = stmt.value
            }
        }
        self.set_runtime_config(cfg)?;
        Ok(ExecResult::Ok)
    }

    pub(super) fn post_commit_checkpoint(&mut self) {
        self.post_checkpoint("post-commit");
    }

    pub(super) fn post_rollback_checkpoint(&mut self) {
        self.post_checkpoint("post-rollback");
    }

    // FIXME: Replace string phase labels with an enum to prevent typos.
    pub(super) fn post_checkpoint(&mut self, phase: &str) {
        self.pending_checkpoint_ops = self.pending_checkpoint_ops.saturating_add(1);
        if !self.should_checkpoint_now() {
            self.stats.deferred_checkpoints += 1;
            return;
        }
        self.stats.total_checkpoints += 1;
        // Best-effort: rollback leaves no committed changes to preserve in WAL.
        if let Err((attempts, e)) = self.try_checkpoint_truncate_with_retry() {
            self.stats.failed_checkpoints += 1;
            self.stats.last_failure_error = Some(format!("{}", e));
            self.stats.last_failure_timestamp_ms = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            );
            self.emit_checkpoint_warning(phase, attempts, &e);
            return;
        }
        self.pending_checkpoint_ops = 0;
        self.last_checkpoint_at = std::time::Instant::now();
    }

    pub(super) fn should_checkpoint_now(&self) -> bool {
        if self.pending_checkpoint_ops == 0 {
            return false;
        }
        if self.checkpoint_policy.tx_threshold > 0 {
            if self.checkpoint_policy.tx_threshold <= 1 {
                return true;
            }
            if self.pending_checkpoint_ops >= self.checkpoint_policy.tx_threshold {
                return true;
            }
        }
        if self.checkpoint_policy.wal_bytes_threshold > 0 {
            if let Ok(size) = self.wal.file_size_bytes() {
                if size >= self.checkpoint_policy.wal_bytes_threshold {
                    return true;
                }
            }
        }
        if self.checkpoint_policy.interval_ms > 0
            && self.last_checkpoint_at.elapsed().as_millis() as u64
                >= self.checkpoint_policy.interval_ms
        {
            return true;
        }
        false
    }

    pub(super) fn handle_show_checkpoint_stats(&self) -> Result<ExecResult> {
        let stats = &self.stats;
        let rows = vec![
            Row {
                values: vec![
                    (
                        "stat".to_string(),
                        Value::Varchar("total_checkpoints".to_string()),
                    ),
                    (
                        "value".to_string(),
                        Value::Varchar(stats.total_checkpoints.to_string()),
                    ),
                ],
            },
            Row {
                values: vec![
                    (
                        "stat".to_string(),
                        Value::Varchar("failed_checkpoints".to_string()),
                    ),
                    (
                        "value".to_string(),
                        Value::Varchar(stats.failed_checkpoints.to_string()),
                    ),
                ],
            },
            Row {
                values: vec![
                    (
                        "stat".to_string(),
                        Value::Varchar("last_failure_error".to_string()),
                    ),
                    (
                        "value".to_string(),
                        Value::Varchar(stats.last_failure_error.clone().unwrap_or_default()),
                    ),
                ],
            },
            Row {
                values: vec![
                    (
                        "stat".to_string(),
                        Value::Varchar("last_failure_timestamp_ms".to_string()),
                    ),
                    (
                        "value".to_string(),
                        Value::Varchar(
                            stats
                                .last_failure_timestamp_ms
                                .map(|v| v.to_string())
                                .unwrap_or_default(),
                        ),
                    ),
                ],
            },
        ];
        Ok(ExecResult::Rows(rows))
    }

    pub fn checkpoint_stats(&self) -> &CheckpointStats {
        &self.stats
    }

    pub fn database_stats(&self) -> &DatabaseStats {
        &self.stats
    }

    pub(super) fn record_commit_in_doubt(&mut self, error: &MuroError) {
        self.stats.commit_in_doubt_count += 1;
        self.stats.last_commit_in_doubt_error = Some(error.to_string());
        self.stats.last_commit_in_doubt_timestamp_ms = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );
        eprintln!("WARNING: commit_in_doubt error=\"{}\"", error);
    }

    pub(super) fn handle_show_database_stats(&self) -> Result<ExecResult> {
        let stats = &self.stats;
        let cache_hits = self.pager.cache_hits();
        let cache_misses = self.pager.cache_misses();
        let cache_total = cache_hits.saturating_add(cache_misses);
        let wal_file_size_bytes = match self.wal.file_size_bytes() {
            Ok(size) => size,
            Err(err) => {
                eprintln!(
                    "WARNING: failed to read WAL file size for SHOW DATABASE STATS: path={} error={}",
                    self.wal.wal_path().display(),
                    err
                );
                0
            }
        };
        let cache_hit_rate_pct = if cache_total == 0 {
            0.0
        } else {
            (cache_hits as f64 * 100.0) / (cache_total as f64)
        };
        fn stat_row(name: &str, value: String) -> Row {
            Row {
                values: vec![
                    ("stat".to_string(), Value::Varchar(name.to_string())),
                    ("value".to_string(), Value::Varchar(value)),
                ],
            }
        }
        let rows = vec![
            stat_row("total_checkpoints", stats.total_checkpoints.to_string()),
            stat_row("failed_checkpoints", stats.failed_checkpoints.to_string()),
            stat_row(
                "last_failure_error",
                stats.last_failure_error.clone().unwrap_or_default(),
            ),
            stat_row(
                "last_failure_timestamp_ms",
                stats
                    .last_failure_timestamp_ms
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
            ),
            stat_row(
                "commit_in_doubt_count",
                stats.commit_in_doubt_count.to_string(),
            ),
            stat_row(
                "last_commit_in_doubt_error",
                stats.last_commit_in_doubt_error.clone().unwrap_or_default(),
            ),
            stat_row(
                "last_commit_in_doubt_timestamp_ms",
                stats
                    .last_commit_in_doubt_timestamp_ms
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
            ),
            stat_row(
                "freelist_sanitize_count",
                stats.freelist_sanitize_count.to_string(),
            ),
            stat_row(
                "freelist_out_of_range_total",
                stats.freelist_out_of_range_total.to_string(),
            ),
            stat_row(
                "freelist_duplicates_total",
                stats.freelist_duplicates_total.to_string(),
            ),
            stat_row(
                "deferred_checkpoints",
                stats.deferred_checkpoints.to_string(),
            ),
            stat_row(
                "checkpoint_pending_ops",
                self.pending_checkpoint_ops.to_string(),
            ),
            stat_row(
                "checkpoint_policy_tx_threshold",
                self.checkpoint_policy.tx_threshold.to_string(),
            ),
            stat_row(
                "checkpoint_policy_wal_bytes_threshold",
                self.checkpoint_policy.wal_bytes_threshold.to_string(),
            ),
            stat_row(
                "checkpoint_policy_interval_ms",
                self.checkpoint_policy.interval_ms.to_string(),
            ),
            stat_row("pager_cache_hits", cache_hits.to_string()),
            stat_row("pager_cache_misses", cache_misses.to_string()),
            stat_row(
                "pager_cache_hit_rate_pct",
                format!("{:.2}", cache_hit_rate_pct),
            ),
            stat_row("wal_file_size_bytes", wal_file_size_bytes.to_string()),
        ];
        Ok(ExecResult::Rows(rows))
    }

    pub(super) fn emit_checkpoint_warning(&self, phase: &str, attempts: usize, error: &MuroError) {
        let wal_path = self.wal.wal_path().display();
        let wal_size = self
            .wal
            .file_size_bytes()
            .ok()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        eprintln!(
            "WARNING: checkpoint_failed phase={} attempts={} error=\"{}\" wal_path={} wal_size_bytes={}",
            phase, attempts, error, wal_path, wal_size
        );
    }

    pub(crate) fn try_checkpoint_truncate_once(&mut self) -> Result<()> {
        #[cfg(test)]
        if self.inject_checkpoint_failures_remaining > 0 {
            self.inject_checkpoint_failures_remaining -= 1;
            return Err(MuroError::Io(std::io::Error::other(
                "injected checkpoint failure",
            )));
        }
        self.wal.checkpoint_truncate()
    }

    pub(super) fn try_checkpoint_truncate_with_retry(
        &mut self,
    ) -> std::result::Result<usize, (usize, MuroError)> {
        let mut last_err = None;
        for attempt in 1..=CHECKPOINT_MAX_ATTEMPTS {
            if attempt > 1 {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            match self.try_checkpoint_truncate_once() {
                Ok(()) => return Ok(attempt),
                Err(e) => last_err = Some(e),
            }
        }
        Err((
            CHECKPOINT_MAX_ATTEMPTS,
            last_err.unwrap_or_else(|| {
                MuroError::Io(std::io::Error::other(
                    "checkpoint truncate failed without error detail",
                ))
            }),
        ))
    }

    #[cfg(test)]
    pub(super) fn inject_checkpoint_failure_once_for_test(&mut self) {
        self.inject_checkpoint_failures_remaining = 1;
    }

    #[cfg(test)]
    pub(super) fn inject_checkpoint_failures_for_test(&mut self, count: usize) {
        self.inject_checkpoint_failures_remaining = count;
    }

    #[cfg(test)]
    pub(super) fn inject_wal_recreate_failure_once_for_test(&mut self) {
        self.inject_wal_recreate_fail_once = true;
    }

    #[cfg(test)]
    pub(super) fn set_checkpoint_policy_for_test(
        &mut self,
        tx_threshold: u64,
        wal_threshold: u64,
        interval_ms: u64,
    ) {
        self.checkpoint_policy = CheckpointPolicy {
            tx_threshold,
            wal_bytes_threshold: wal_threshold,
            interval_ms,
        };
    }
}
