use std::io::BufWriter;

use bgen_reader::bgen::BgenSteam;
use bgen_reader::bgi_writer::TableCreator;
use bgen_reader::parser::{Cli, Command};
use bgen_reader::vcf_writer;
use clap::Parser;
use color_eyre::Result;

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    match cli.command {
        Command::Index => {
            let mut bgen_stream = BgenSteam::from_path(&cli.filename, cli.use_sample_file, false)?;
            bgen_stream.read_offset_and_header()?;
            let bgi_filename = cli.filename.to_string() + ".bgi_rust";
            let table_creator = TableCreator::new(bgi_filename)?;
            table_creator.init(bgen_stream.metadata.as_ref().unwrap())?;
            table_creator.store(bgen_stream)?;
        }
        Command::List(list_args) => {
            let mut bgen_stream = BgenSteam::from_path(&cli.filename, cli.use_sample_file, false)?;
            bgen_stream.read_offset_and_header()?;
            bgen_stream.collect_filters(list_args);
            let mut writer = BufWriter::new(std::io::stdout());
            bgen_stream.try_for_each(|variant_data| variant_data?.bgenix_print(&mut writer))?
        }
        Command::Vcf(list_args) => {
            let mut bgen_stream = BgenSteam::from_path(&cli.filename, cli.use_sample_file, true)?;
            bgen_stream.read_offset_and_header()?;
            bgen_stream.collect_filters(list_args);
            vcf_writer::write_vcf("test.vcf", bgen_stream)?;
        }
    }
    Ok(())
}
