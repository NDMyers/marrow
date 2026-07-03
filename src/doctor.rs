//! Post-ingest index self-check.
//!
//! Proves that what ingest just wrote is actually reachable through the same
//! resolution path agents use, so "ingest succeeded but queries can't see it"
//! regressions (e.g. the Windows path-separator mismatch fixed in bf4e77c)
//! fail loudly at ingest time instead of silently degrading every later query.

use anyhow::Result;
use rusqlite::Connection;

use crate::retrieval;

pub struct SelfCheckReport {
    pub repo_id: String,
    pub checked: usize,
    pub failures: Vec<String>,
}

impl SelfCheckReport {
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }

    /// One-line (multi-line on failure) summary for humans and agents.
    pub fn summary_line(&self) -> String {
        if self.checked == 0 {
            format!(
                "Self-check ({}): no indexed symbols to sample.",
                self.repo_id
            )
        } else if self.passed() {
            format!(
                "Self-check ({}): {}/{} sampled checks resolvable through the \
                 agent query path (both path-separator styles).",
                self.repo_id, self.checked, self.checked
            )
        } else {
            format!(
                "Self-check ({}) FAILED — {} of {} sampled checks unresolvable:\n  - {}",
                self.repo_id,
                self.failures.len(),
                self.checked,
                self.failures.join("\n  - ")
            )
        }
    }
}

/// Sample up to `sample_limit` indexed symbols (one per file) and resolve each
/// through `resolve_symbol_or_disambiguate` — the exact entry point every
/// agent-facing query tool uses — passing the stored file path in both
/// separator styles. Also smoke-tests one full capsule build. Failures name
/// the symbol, the path form that failed, and the underlying error.
pub fn run_index_self_check(
    conn: &Connection,
    repo_id: &str,
    sample_limit: usize,
) -> Result<SelfCheckReport> {
    let mut stmt = conn.prepare(
        "SELECT MIN(symbol_name), file_path FROM nodes
         WHERE repo_id = ?1
           AND symbol_type IN ('function', 'method', 'class', 'struct')
         GROUP BY file_path
         ORDER BY file_path
         LIMIT ?2",
    )?;
    let samples: Vec<(String, String)> = stmt
        .query_map(rusqlite::params![repo_id, sample_limit as i64], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .flatten()
        .collect();

    let mut report = SelfCheckReport {
        repo_id: repo_id.to_string(),
        checked: 0,
        failures: Vec::new(),
    };

    for (symbol, stored_path) in &samples {
        let posix = stored_path.replace('\\', "/");
        let windows = stored_path.replace('/', "\\");
        let mut styles = vec![posix];
        if windows != styles[0] {
            styles.push(windows);
        }
        for path_style in styles {
            report.checked += 1;
            if let Err(err) =
                retrieval::resolve_symbol_or_disambiguate(conn, symbol, repo_id, Some(&path_style))
            {
                report
                    .failures
                    .push(format!("'{symbol}' with filepath '{path_style}': {err}"));
            }
        }
    }

    // Smoke-test one full capsule build end to end.
    if let Some((symbol, stored_path)) = samples.first() {
        report.checked += 1;
        if let Err(err) = retrieval::get_context_capsule(conn, symbol, repo_id, Some(stored_path)) {
            report
                .failures
                .push(format!("capsule build for '{symbol}': {err}"));
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ingested_fixture() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("alpha.py"),
            "def alpha():\n    return 1\n\n\ndef beta():\n    return alpha()\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("nested")).unwrap();
        std::fs::write(
            dir.path().join("nested").join("gamma.py"),
            "def gamma():\n    return 2\n",
        )
        .unwrap();
        let conn = crate::db::init_db(":memory:").unwrap();
        crate::ingestion::ingest_repo(&conn, "fixture", dir.path()).unwrap();
        (dir, conn)
    }

    #[test]
    fn self_check_passes_on_healthy_index_with_both_separator_styles() {
        let (_dir, conn) = ingested_fixture();
        let report = run_index_self_check(&conn, "fixture", 8).unwrap();
        assert!(report.checked > 0, "should sample symbols");
        assert!(
            report.passed(),
            "healthy index should pass: {}",
            report.summary_line()
        );
        // The nested file guarantees at least one stored path containing a
        // separator, so both POSIX and Windows styles were exercised.
        assert!(
            report.checked >= 3,
            "expected both-style checks, got {}",
            report.checked
        );
    }

    #[test]
    fn self_check_reports_empty_repo_without_failing() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let report = run_index_self_check(&conn, "ghost", 8).unwrap();
        assert_eq!(report.checked, 0);
        assert!(report.passed());
        assert!(report.summary_line().contains("no indexed symbols"));
    }

    #[test]
    fn failed_report_summarizes_each_failure() {
        let report = SelfCheckReport {
            repo_id: "r".into(),
            checked: 3,
            failures: vec!["'f' with filepath 'a/b.rs': boom".into()],
        };
        assert!(!report.passed());
        let line = report.summary_line();
        assert!(line.contains("FAILED"), "summary should shout: {line}");
        assert!(line.contains("a/b.rs"), "summary should name paths: {line}");
    }
}
