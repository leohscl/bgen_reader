use bgen_reader::bgen::BgenSteam;
use bgen_reader::bgi_writer::TableCreator;
use bgen_reader::parser::{Cli, Command};
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
        Command::List(list_args) => {
            let (vec_incl_range, vec_incl_rsid, vec_excl_range, vec_excl_rsid) =
                list_args.get_vector_incl_and_excl();
            let variant_data_str: String = bgen_stream
                .variants_data
                .iter()
                .filter(|variant_data| {
                    variant_data.filter_with_args(
                        vec_incl_range.clone(),
                        vec_incl_rsid.clone(),
                        vec_excl_range.clone(),
                        vec_excl_rsid.clone(),
                    )
                })
                .map(|variant_data| variant_data.bgenix_print())
                .collect::<Vec<String>>()
                .join("\n");
            println!("{}", variant_data_str);
        }
    }
    Ok(())
}
