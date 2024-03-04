use clap::{Parser, ValueEnum};

#[derive(Parser)]
pub struct Cli {
    #[arg(short, long, value_name = "FILE")]
    pub filename: String,
    #[arg(value_enum)]
    pub command: Command,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Command {
    List,
    Index,
}
