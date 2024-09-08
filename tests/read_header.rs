extern crate bgen_reader;
use bgen_reader::bgen::BgenStream;
use bgen_reader::bgen::HeaderFlags;
use std::io::Cursor;

#[test]
fn read_offset() {
    let bgen_stream = create_bgen_and_read();
    assert_eq!(1728, bgen_stream.header.start_data_offset);
}

#[test]
fn read_header_size() {
    let bgen_stream = create_bgen_and_read();
    assert_eq!(20, bgen_stream.header.header_size);
}

#[test]
fn read_header_flags() {
    let header_flag = HeaderFlags {
        compressed_snp_blocks: true,
        layout_id: 2,
        sample_id_present: true,
    };
    let bgen_stream = create_bgen_and_read();
    assert_eq!(header_flag, bgen_stream.header.header_flags);
}

fn create_bgen_and_read() -> BgenStream<Cursor<Vec<u8>>> {
    let bgen_bytes = include_bytes!("../data_test/samp_100_var_100.bgen");
    let mut bgen_stream = BgenStream::from_bytes(bgen_bytes.to_vec(), true).unwrap();
    bgen_stream.read_offset_and_header().unwrap();
    bgen_stream
}
