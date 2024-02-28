use bgen_reader::bgen::BgenSteam;
use color_eyre::Result;

fn main() -> Result<()> {
    // let bgen_path = "../data_test/samp_100_var_100.bgen";
    // let mut bgen_stream = BgenSteam::from_path(bgen_path)?;
    let bgen_bytes = include_bytes!("../data_test/samp_100_var_100.bgen");
    let mut bgen_stream = BgenSteam::from_bytes(bgen_bytes.to_vec())?;
    bgen_stream.read_header_size()?;
    dbg!(bgen_stream.header_size);
    Ok(())
}
