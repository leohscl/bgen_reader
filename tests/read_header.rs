extern crate bgen_reader;
use bgen_reader::bgen::BgenSteam;
use std::io::Cursor;

#[test]
fn read_offset() {
    let bgen_stream = create_bgen_and_read();
    assert_eq!(204, bgen_stream.start_data_offset);
}

#[test]
fn read_header_size() {
    let bgen_stream = create_bgen_and_read();
    assert_eq!(20, bgen_stream.header_size);
}

fn create_bgen_and_read() -> BgenSteam<Cursor<Vec<u8>>> {
    let bgen_bytes = include_bytes!("../data_test/samp_100_var_100.bgen");
    let mut bgen_stream = BgenSteam::from_bytes(bgen_bytes.to_vec()).unwrap();
    bgen_stream.read_offset_and_header().unwrap();
    bgen_stream
}
