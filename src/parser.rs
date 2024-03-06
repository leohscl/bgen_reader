use clap::error::ErrorKind;
use clap::CommandFactory;
use clap::{Args, Parser, Subcommand};
use std::str::FromStr;

#[derive(Parser)]
pub struct Cli {
    /// Bgen file name
    #[arg(short, long, value_name = "FILE")]
    pub filename: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// List the variants in the file. Takes an optional range in the format -incl-range=1:0-10000
    List { range: Option<String> },
    /// Index the bgen file
    Index,
}

pub fn validate_parsing(range: Option<String>) -> Result<Option<Range>, clap::error::Error> {
    range.as_ref().map(|v| Range::from_str(v)).transpose()
}

#[derive(Clone, Args, Debug)]
pub struct Range {
    pub chr: String,
    pub start: u32,
    pub end: u32,
    pub incl: bool,
}

impl FromStr for Range {
    type Err = clap::error::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || {
            let mut cmd = Cli::command();
            cmd.error(
                ErrorKind::ValueValidation,
                "Invalid range format. Please use the following format: \n\
                bgen_reader -f file.bgen list incl-range=1:0-10000 \
                ",
            )
        };
        let mut iter_w = s.split('=');
        let incl = match iter_w.next() {
            None => return Err(err()),
            Some(s) => match s {
                "incl-range" => true,
                "excl-range" => false,
                _ => return Err(err()),
            },
        };
        let w2 = iter_w.next().ok_or(err())?;
        let mut split_expr = w2.split(':');
        let chr = split_expr.next().ok_or(err())?;
        let expr_2 = split_expr.next().ok_or(err())?;
        let mut range_split = expr_2.split('-');
        let start = range_split
            .next()
            .ok_or(err())?
            .parse::<u32>()
            .map_err(|_| err())?;
        let end = range_split
            .next()
            .ok_or(err())?
            .parse::<u32>()
            .map_err(|_| err())?;
        Ok(Range {
            chr: chr.to_string(),
            start,
            end,
            incl,
        })
    }
}
