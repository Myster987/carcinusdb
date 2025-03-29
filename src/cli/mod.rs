use clap::{Args, Parser, Subcommand};

mod validators;

#[derive(Parser)]
#[command(name = "CarnicusDB")]
#[command(about = "Small, fast and scalable sql database", long_about = None)]
#[command(version = "0.1.0")]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Run(CommandArgs),
}

#[derive(Args)]
pub struct CommandArgs {
    #[arg(short = 'D', long)]
    pub database_file_path: String,

    #[arg(short = 'H', long, value_parser = validators::validate_hostname)]
    pub hostname: String,

    #[arg(short = 'P', long, value_parser = clap::value_parser!(u16).range(1024..=49151))]
    pub port: u16,
}
