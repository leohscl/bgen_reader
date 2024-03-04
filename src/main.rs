use bgen_reader::bgen::BgenSteam;
use bgen_reader::bgi_writer::TableCreator;
use bgen_reader::parser::Cli;
use clap::Parser;
use color_eyre::Result;

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    let mut bgen_stream = BgenSteam::from_path(&cli.filename)?;
    bgen_stream.read_offset_and_header()?;
    bgen_stream.read_all_variant_data()?;
    let bgi_filename = cli.filename.to_string() + ".bgi_rust";
    let table_creator = TableCreator::new(bgi_filename)?;
    table_creator.init()?;
    table_creator.store(&bgen_stream.variants_data)?;
    Ok(())
}
