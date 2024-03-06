use bgen_reader::bgen::BgenSteam;
use bgen_reader::bgi_writer::TableCreator;
use bgen_reader::parser::{
    validate_parsing_range, Cli, Command, ExclRange, ExclRsid, InclRange, InclRsid,
};
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
            // TODO: refactor, with macros ?
            let opt_incl_range = match list_args.incl_range {
                InclRange {
                    incl_range,
                    incl_range_file: None,
                } => incl_range,
                _ => todo!(),
            };
            let vec_incl_range = match validate_parsing_range(opt_incl_range) {
                Ok(range) => range,
                Err(cmd_error) => cmd_error.exit(),
            };
            let opt_excl_range = match list_args.excl_range {
                ExclRange {
                    excl_range,
                    excl_range_file: None,
                } => excl_range,
                _ => todo!(),
            };
            let vec_excl_range = match validate_parsing_range(opt_excl_range) {
                Ok(range) => range,
                Err(cmd_error) => cmd_error.exit(),
            };
            let opt_incl_rsid = match list_args.incl_rsid {
                InclRsid {
                    incl_rsid,
                    incl_rsid_file: None,
                } => incl_rsid,
                _ => None,
            };
            let vec_incl_rsid: Vec<_> = opt_incl_rsid.into_iter().collect();
            let opt_excl_rsid = match list_args.excl_rsid {
                ExclRsid {
                    excl_rsid,
                    excl_rsid_file: None,
                } => excl_rsid,
                _ => None,
            };
            let vec_excl_rsid: Vec<_> = opt_excl_rsid.into_iter().collect();
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
