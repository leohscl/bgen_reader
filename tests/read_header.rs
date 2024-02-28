extern crate bgen_reader;
use bgen_reader::bgen::BgenSteam;

#[test]
fn read_header() {
    let bgen_bytes = include_bytes!("../data_test/samp_100_var_100.bgen");
    let mut bgen_stream = BgenSteam::from_bytes(bgen_bytes.to_vec()).unwrap();
    bgen_stream.read_header_size().unwrap();
    assert_eq!(204, bgen_stream.header_size);
}
