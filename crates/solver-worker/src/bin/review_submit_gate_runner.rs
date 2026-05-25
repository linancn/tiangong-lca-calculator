use std::time::Duration;

use clap::Parser;
use solver_worker::{
    config::AppConfig,
    db::AppState,
    review_submit_gate_runner::{ReviewSubmitGateRunnerOptions, run_review_submit_gate_runner},
};
use tracing::info;

#[derive(Debug, Parser)]
#[command(name = "review-submit-gate-runner")]
#[command(about = "Claim persisted review-submit gate runs and record calculator results.")]
struct Cli {
    #[command(flatten)]
    config: AppConfig,
    #[arg(long, env = "REVIEW_SUBMIT_GATE_POLL_MS", default_value_t = 1_000_u64)]
    poll_ms: u64,
    #[arg(long, env = "REVIEW_SUBMIT_GATE_MAX_RUNS")]
    max_runs: Option<usize>,
    #[arg(long, default_value_t = false)]
    once: bool,
    #[arg(
        long,
        env = "REVIEW_SUBMIT_GATE_STALE_RUNNING_SECONDS",
        default_value_t = 21_600_u64
    )]
    stale_running_after_seconds: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let state = AppState::new(&cli.config).await?;
    let options = ReviewSubmitGateRunnerOptions {
        poll_interval: Duration::from_millis(cli.poll_ms.max(100)),
        max_runs: cli.max_runs.or_else(|| cli.once.then_some(1)),
        exit_when_idle: cli.once,
        stale_running_after: Duration::from_secs(cli.stale_running_after_seconds.max(60)),
    };

    let summary = run_review_submit_gate_runner(&state, options).await?;
    info!(?summary, "review-submit gate runner stopped");
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
