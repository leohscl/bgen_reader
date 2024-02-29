use clap::Parser;

#[derive(Parser)]
pub struct Cli {
    #[arg(short, long, value_name = "FILE")]
    pub filename: String,
}
