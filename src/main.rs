use bgen_reader::bgen::BgenSteam;
use bgen_reader::bgi_writer::TableCreator;
use bgen_reader::parser::{Cli, Command};
use bgen_reader::vcf_writer;
use clap::Parser;
use color_eyre::Result;

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    let mut bgen_stream = BgenSteam::from_path(&cli.filename)?;
    bgen_stream.read_offset_and_header()?;
    match cli.command {
        Command::Index => {
            let bgi_filename = cli.filename.to_string() + ".bgi_rust";
            let table_creator = TableCreator::new(bgi_filename)?;
            table_creator.init(bgen_stream.metadata.unwrap())?;
            table_creator.store(&bgen_stream.variants_data)?;
        }
        Command::List(list_args) => {
            bgen_stream.collect_filters(list_args);
            let variant_data_str = bgen_stream
                .into_iter()
                .map(|variant_data| Ok(variant_data?.bgenix_print()))
                .collect::<Result<Vec<String>>>()?
                .join("\n");
            println!("{}", variant_data_str);
        }
        Command::Vcf(list_args) => {
            bgen_stream.collect_filters(list_args);
            vcf_writer::write_vcf("test.vcf", bgen_stream)?;
        }
    }
    Ok(())
}
