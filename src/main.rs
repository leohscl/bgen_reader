use bgen_reader::bgen::BgenSteam;
use bgen_reader::bgi_writer::TableCreator;
use bgen_reader::parser::{validate_parsing, Cli, Command};
use clap::Parser;
use color_eyre::Result;

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    let mut bgen_stream = BgenSteam::from_path(&cli.filename)?;
    bgen_stream.read_offset_and_header()?;
    bgen_stream.read_all_variant_data()?;
    match cli.command {
        Command::Index => {
            let bgi_filename = cli.filename.to_string() + ".bgi_rust";
            let table_creator = TableCreator::new(bgi_filename)?;
            table_creator.init(bgen_stream.metadata.unwrap())?;
            table_creator.store(&bgen_stream.variants_data)?;
        }
        Command::List { range: range_str } => {
            dbg!(&range_str);
            let opt_range = match validate_parsing(range_str) {
                Ok(range) => range,
                Err(cmd_error) => cmd_error.exit(),
            };
            dbg!(&opt_range);
            let variant_data_str: String = bgen_stream
                .variants_data
                .iter()
                .filter(|variant_data| variant_data.filter_with_range(&opt_range))
                .map(|variant_data| variant_data.bgenix_print())
                .collect::<Vec<String>>()
                .join("\n");
            println!("{}", variant_data_str);
        }
    }
    Ok(())
}
