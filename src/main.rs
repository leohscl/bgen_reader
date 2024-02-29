use bgen_reader::bgen::BgenSteam;
use bgen_reader::parser::Cli;
use clap::Parser;
use color_eyre::Result;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut bgen_stream = BgenSteam::from_path(&cli.filename)?;
    bgen_stream.read_offset_and_header()?;
    dbg!(bgen_stream.header_flags);
    dbg!(bgen_stream.variants_data);
    Ok(())
}
