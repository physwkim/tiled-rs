use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[command(name = "tiled", version, about = "Tiled: structured scientific data access service")]
struct Cli {
    #[command(subcommand)]
    command: tiled_cli::Command,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    tiled_cli::run(cli.command).await
}
