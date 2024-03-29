use clap::error::ErrorKind;
use clap::CommandFactory;
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
pub struct Cli {
    /// Bgen file name
    #[arg(short, long, value_name = "FILE")]
    pub filename: String,

    /// Verbose mode
    #[arg(short, long)]
    pub verbose: bool,

    /// Should the program try to find the .sample file associated with the bgen file
    #[arg(short, long, default_value_t = false)]
    pub use_sample_file: bool,

    /// What command to run
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// List the variants in the file.
    List(ListArgs),
    /// Index the bgen file
    Index,
    /// output VCF information
    Vcf(ListArgs),
}

#[derive(Parser, Default)]
pub struct ListArgs {
    #[command(flatten)]
    pub incl_range: InclRange,
    #[command(flatten)]
    pub excl_range: ExclRange,
    #[command(flatten)]
    pub incl_rsid: InclRsid,
    #[command(flatten)]
    pub excl_rsid: ExclRsid,
}

impl ListArgs {
    pub fn with_incl_str(incl_str: String) -> Self {
        ListArgs {
            incl_range: InclRange {
                incl_range: Some(incl_str),
                incl_range_file: None,
            },
            incl_rsid: InclRsid::default(),
            excl_range: ExclRange::default(),
            excl_rsid: ExclRsid::default(),
        }
    }

    pub fn get_vector_incl_and_excl(&self) -> (Vec<Range>, Vec<String>, Vec<Range>, Vec<String>) {
        let opt_incl_range = match &self.incl_range {
            InclRange {
                incl_range,
                incl_range_file: None,
            } => incl_range,
            _ => todo!(),
        };
        let vec_incl_range = match validate_parsing_range(opt_incl_range.clone()) {
            Ok(range) => range,
            Err(cmd_error) => cmd_error.exit(),
        };
        let opt_excl_range = match &self.excl_range {
            ExclRange {
                excl_range,
                excl_range_file: None,
            } => excl_range,
            _ => todo!(),
        };
        let vec_excl_range = match validate_parsing_range(opt_excl_range.clone()) {
            Ok(range) => range,
            Err(cmd_error) => cmd_error.exit(),
        };
        let opt_incl_rsid = match &self.incl_rsid {
            InclRsid {
                incl_rsid,
                incl_rsid_file: None,
            } => incl_rsid.clone(),
            _ => None,
        };
        let vec_incl_rsid: Vec<_> = opt_incl_rsid.into_iter().collect();
        let opt_excl_rsid = match &self.excl_rsid {
            ExclRsid {
                excl_rsid,
                excl_rsid_file: None,
            } => excl_rsid.clone(),
            _ => None,
        };
        let vec_excl_rsid: Vec<_> = opt_excl_rsid.into_iter().collect();
        (vec_incl_range, vec_incl_rsid, vec_excl_range, vec_excl_rsid)
    }
}

#[derive(Args, Default)]
#[group(required = false, multiple = false)]
pub struct InclRange {
    #[arg(long)]
    /// Optional range in the format --incl-range 1:0-10000
    pub incl_range: Option<String>,
    #[arg(long)]
    /// Optional range file, one range per line
    pub incl_range_file: Option<String>,
}

#[derive(Args, Default)]
#[group(required = false, multiple = false)]
pub struct ExclRange {
    #[arg(long)]
    /// Optional range in the format --excl-range 1:0-10000
    pub excl_range: Option<String>,
    #[arg(long)]
    /// Optional range file, one range per line
    pub excl_range_file: Option<String>,
}

#[derive(Args, Default)]
#[group(required = false, multiple = false)]
pub struct InclRsid {
    #[arg(long)]
    /// Optional range in the format --incl-rsid rs100
    pub incl_rsid: Option<String>,
    #[arg(long)]
    /// Optional rsid file, one rsid per line
    pub incl_rsid_file: Option<String>,
}

#[derive(Args, Default)]
#[group(required = false, multiple = false)]
pub struct ExclRsid {
    #[arg(long)]
    /// Optional range in the format --incl-rsid rs100
    pub excl_rsid: Option<String>,
    #[arg(long)]
    /// Optional rsid file, one rsid per line
    pub excl_rsid_file: Option<String>,
}

pub fn validate_parsing_range(
    incl_range: Option<String>,
) -> Result<Vec<Range>, clap::error::Error> {
    incl_range
        .as_ref()
        .iter()
        .map(|v| Range::from_str(v, true))
        .collect()
}

#[derive(Clone, Args, Debug)]
pub struct Range {
    pub chr: String,
    pub start: u32,
    pub end: u32,
    pub incl: bool,
}

impl Range {
    fn from_str(s: &str, incl: bool) -> Result<Self, clap::error::Error> {
        let err = || {
            let mut cmd = Cli::command();
            cmd.error(
                ErrorKind::ValueValidation,
                "Invalid range format. Please use the following format: \n\
                bgen_reader -f file.bgen list --incl-range 1:0-10000 \
                ",
            )
        };
        let mut split_expr = s.split(':');
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
