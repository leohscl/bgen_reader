use bgen_reader::bgen::{bgen_merge, BgenStream, MetadataBgi};
use bgen_reader::bgi_writer::TableCreator;
use bgen_reader::parser::{Cli, Command};
use bgen_reader::vcf_writer;
use clap::Parser;
use color_eyre::Report;
use color_eyre::Result;
use env_logger::Builder;
use log::LevelFilter;
use std::io::{BufWriter, Write};

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    if cli.verbose {
        Builder::new().filter_level(LevelFilter::Info).init();
    } else {
        Builder::new().filter_level(LevelFilter::Warn).init();
    }
    match cli.command {
        Command::Index => {
            let mut bgen_stream = BgenStream::from_path(&cli.filename, cli.use_sample_file, false)?;
            bgen_stream.read_offset_and_header()?;
            let bgi_filename = cli.filename.to_string() + ".bgi_rust";
            let table_creator = TableCreator::new(bgi_filename)?;
            let file_metadata = match bgen_stream.metadata.clone() {
                MetadataBgi::File(file_metadata) => file_metadata,
                _ => {
                    return Err(Report::msg(
                        "No file metadata in bgen constructed from file",
                    ))
                }
            };
            table_creator.init(&file_metadata)?;
            table_creator.store(bgen_stream)?;
        }
        Command::List(list_args) => {
            let mut bgen_stream = BgenStream::from_path(&cli.filename, cli.use_sample_file, false)?;
            bgen_stream.read_offset_and_header()?;
            bgen_stream.collect_filters(list_args)?;
            let mut writer = BufWriter::new(std::io::stdout());
            let line_header = b"alternate_ids\trsid\tchromosome\tposition\tnumber_of_alleles\tfirst_allele\talternative_alleles\n";
            writer.write_all(line_header)?;
            bgen_stream.try_for_each(|variant_data| variant_data?.bgenix_print(&mut writer))?
        }
        Command::Vcf(list_args_named) => {
            let mut bgen_stream = BgenStream::from_path(&cli.filename, cli.use_sample_file, true)?;
            bgen_stream.read_offset_and_header()?;
            bgen_stream.collect_filters(list_args_named.list_args)?;
            vcf_writer::write_vcf(&list_args_named.name, bgen_stream)?;
        }
        Command::Bgen(list_args_named) => {
            let mut bgen_stream = BgenStream::from_path(&cli.filename, cli.use_sample_file, true)?;
            bgen_stream.read_offset_and_header()?;
            bgen_stream.collect_filters(list_args_named.list_args)?;
            bgen_stream.to_bgen(&list_args_named.name)?;
        }
        Command::Merge(merge_filename) => {
            bgen_merge(merge_filename.name, merge_filename.output_name)?;
        }
    }
    Ok(())
}
