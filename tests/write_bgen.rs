extern crate bgen_reader;
use bgen_reader::bgen::bgen_stream::BgenStream;
use bgen_reader::parser::FilterArgs;
use serial_test::serial;
use std::io::Cursor;

const OUT_FILE: &str = "test.bgen";

#[test]
#[serial]
fn compare_original_and_rewrite() {
    create_bgen_and_read().to_bgen(OUT_FILE).unwrap();
    let mut bgen_stream_test = BgenStream::from_path(OUT_FILE, false, true).unwrap();
    bgen_stream_test.read_offset_and_header().unwrap();
    let bgen_bytes = include_bytes!("../data_test/samp_100_var_100.bgen");
    let mut bgen_stream_oracle = BgenStream::from_bytes(bgen_bytes.to_vec(), true).unwrap();
    bgen_stream_oracle.read_offset_and_header().unwrap();

    assert_eq!(bgen_stream_test.header, bgen_stream_oracle.header);
    assert_eq!(bgen_stream_test.samples, bgen_stream_oracle.samples);
    //dbg!("reading data block test");
    let data_blocks_test = bgen_stream_test.collect::<Result<Vec<_>, _>>().unwrap();
    let data_blocks_oracle = bgen_stream_oracle.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        data_blocks_test.len(),
        data_blocks_oracle.len(),
        "Length is not equal !"
    );
    assert_eq!(data_blocks_test, data_blocks_oracle);
    std::fs::remove_file(OUT_FILE).unwrap();
}

#[test]
#[serial]
fn filtering_on_bgen_write() {
    let mut bgen_stream = create_bgen_and_read();
    let list_args = FilterArgs::default().with_incl_str("1:0-752567".to_string());
    bgen_stream.collect_filters(list_args).unwrap();
    bgen_stream.to_bgen(OUT_FILE).unwrap();
    let mut bgen_stream_test = BgenStream::from_path(OUT_FILE, false, true).unwrap();
    bgen_stream_test.read_offset_and_header().unwrap();
    assert_eq!(1, bgen_stream_test.header.variant_num);
    let variant_data: Vec<_> = bgen_stream_test.map(|r| r.unwrap()).collect();
    dbg!(&variant_data);
    assert_eq!(1, variant_data.len());
    std::fs::remove_file(OUT_FILE).unwrap();
}

fn create_bgen_and_read() -> BgenStream<Cursor<Vec<u8>>> {
    let bgen_bytes = include_bytes!("../data_test/samp_100_var_100.bgen");
    let mut bgen_stream = BgenStream::from_bytes(bgen_bytes.to_vec(), true).unwrap();
    bgen_stream.read_offset_and_header().unwrap();
    bgen_stream
}
