use clap::error::ErrorKind;
use clap::CommandFactory;
use clap::{Args, Parser, Subcommand};
use color_eyre::Report;
use color_eyre::Result;

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
    List(FilterArgsList),
    /// Index the bgen file
    Index,
    /// output VCF information
    Vcf(FilterArgsNamed),
    /// Output Bgen information
    Bgen(FilterArgsNamed),
    /// Merge multiple bgen files together
    Merge(MergeArgs),
}
#[derive(Parser, Default)]
pub struct MergeArgs {
    pub name: String,
    pub output_name: String,
}
#[derive(Parser, Default)]
pub struct FilterArgsNamed {
    #[command(flatten)]
    pub filter_args: FilterArgs,
    pub name: String,
}
#[derive(Parser, Default)]
pub struct FilterArgsList {
    #[command(flatten)]
    pub filter_args: FilterArgs,
    #[command(subcommand)]
    pub variant_output: VariantOutput,
}

#[derive(Subcommand, Default, Clone)]
pub enum VariantOutput {
    #[default]
    Bgenix,
    Rsid,
}

#[derive(Parser, Default)]
pub struct FilterArgs {
    #[command(flatten)]
    pub incl_range: InclRange,
    #[command(flatten)]
    pub excl_range: ExclRange,
    #[command(flatten)]
    pub incl_rsid: InclRsid,
    #[command(flatten)]
    pub excl_rsid: ExclRsid,
}
// TODO(lhenches): this should be Hashmap for rsids
type AllFilters = (Vec<Range>, Vec<String>, Vec<Range>, Vec<String>);

impl FilterArgs {
    pub fn with_range_incl_file(mut self, incl_file_str: String) -> Self {
        self.incl_range = InclRange {
            incl_range: None,
            incl_range_file: Some(incl_file_str),
        };
        self
    }

    pub fn with_range_excl_file(mut self, excl_file_str: String) -> Self {
        self.excl_range = ExclRange {
            excl_range: None,
            excl_range_file: Some(excl_file_str),
        };
        self
    }

    pub fn with_range_incl_str(mut self, incl_str: String) -> Self {
        self.incl_range = InclRange {
            incl_range: Some(incl_str),
            incl_range_file: None,
        };
        self
    }

    pub fn with_range_excl_str(mut self, excl_str: String) -> Self {
        self.excl_range = ExclRange {
            excl_range: Some(excl_str),
            excl_range_file: None,
        };
        self
    }

    pub fn with_rsid_incl_str(mut self, incl_str: String) -> Self {
        self.incl_rsid = InclRsid {
            incl_rsid: Some(incl_str),
            incl_rsid_file: None,
        };
        self
    }

    pub fn with_rsid_incl_file(mut self, incl_file_str: String) -> Self {
        self.incl_rsid = InclRsid {
            incl_rsid: None,
            incl_rsid_file: Some(incl_file_str),
        };
        self
    }

    pub fn get_vector_incl_and_excl(&self) -> Result<AllFilters> {
        let opt_incl_range = match &self.incl_range {
            InclRange {
                incl_range,
                incl_range_file: None,
            } => incl_range.clone(),
            InclRange {
                incl_range: None,
                incl_range_file,
            } => Some(std::fs::read_to_string(
                incl_range_file
                    .clone()
                    .ok_or(Report::msg("Range file does not exist"))?,
            )?),
            _ => panic!("Range file and range at command line specified"),
        };
        let vec_incl_range = if let Some(incl_range_string) = opt_incl_range {
            match validate_parsing_range(incl_range_string) {
                Ok(range) => range,
                Err(cmd_error) => cmd_error.exit(),
            }
        } else {
            Vec::new()
        };
        let opt_excl_range = match &self.excl_range {
            ExclRange {
                excl_range,
                excl_range_file: None,
            } => excl_range.clone(),
            ExclRange {
                excl_range: None,
                excl_range_file,
            } => Some(std::fs::read_to_string(
                excl_range_file
                    .clone()
                    .ok_or(Report::msg("Range file does not exist"))?,
            )?),
            _ => panic!("Range file and range at command line specified"),
        };
        let vec_excl_range = if let Some(excl_range_string) = opt_excl_range {
            match validate_parsing_range(excl_range_string) {
                Ok(range) => range,
                Err(cmd_error) => cmd_error.exit(),
            }
        } else {
            Vec::new()
        };
        let opt_incl_rsid: Vec<String> = match &self.incl_rsid {
            InclRsid {
                incl_rsid,
                incl_rsid_file: None,
            } => incl_rsid.iter().cloned().collect(),
            InclRsid {
                incl_rsid: None,
                incl_rsid_file: Some(incl_file),
            } => std::fs::read_to_string(incl_file)?
                .split('\n')
                .map(|s| s.to_string())
                .collect(),
            _ => panic!("Rsid file and range at command line specified"),
        };
        let vec_incl_rsid: Vec<_> = opt_incl_rsid.into_iter().collect();
        let opt_excl_rsid: Vec<String> = match &self.excl_rsid {
            ExclRsid {
                excl_rsid,
                excl_rsid_file: None,
            } => excl_rsid.iter().cloned().collect(),
            ExclRsid {
                excl_rsid: None,
                excl_rsid_file: Some(excl_file),
            } => std::fs::read_to_string(excl_file)?
                .split('\n')
                .map(|s| s.to_string())
                .collect(),
            _ => panic!("Rsid file and range at command line specified"),
        };
        let vec_excl_rsid: Vec<_> = opt_excl_rsid.into_iter().collect();
        Ok((vec_incl_range, vec_incl_rsid, vec_excl_range, vec_excl_rsid))
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

pub fn validate_parsing_range(incl_range: String) -> Result<Vec<Range>, clap::error::Error> {
    incl_range
        .trim_end_matches('\n')
        .split('\n')
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
