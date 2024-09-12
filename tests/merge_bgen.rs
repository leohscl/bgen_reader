extern crate bgen_reader;
use bgen_reader::bgen::bgen_stream::bgen_merge;
use bgen_reader::bgen::bgen_stream::BgenStream;
use bgen_reader::parser::FilterArgs;
use serial_test::serial;
use std::fs::File;
use std::io::Cursor;
use std::io::LineWriter;
use std::io::Write;

const OUT_FILE_P1: &str = "with_1_var.bgen";
const OUT_FILE_P2: &str = "with_99_var.bgen";

#[test]
#[serial]
fn filtering_then_merging() {
    let mut bgen_stream = create_bgen_and_read();
    let list_args = FilterArgs::default().with_range_incl_str("1:0-952567".to_string());
    bgen_stream.collect_filters(list_args).unwrap();
    bgen_stream.to_bgen(OUT_FILE_P1, false).unwrap();
    let mut bgen_stream = create_bgen_and_read();
    let list_args = FilterArgs::default().with_range_excl_str("1:0-952567".to_string());
    bgen_stream.collect_filters(list_args).unwrap();
    bgen_stream.to_bgen(OUT_FILE_P2, false).unwrap();
    let merge_name = "tmp.merge";
    let file_merge = File::create(merge_name).unwrap();
    let mut writer = LineWriter::new(file_merge);
    writer.write_all(OUT_FILE_P1.as_bytes()).unwrap();
    writer.write_all(b"\n").unwrap();
    writer.write_all(OUT_FILE_P2.as_bytes()).unwrap();
    writer.write_all(b"\n").unwrap();
    let merge_output = "reconstructed.bgen";
    bgen_merge(
        merge_name.to_string(),
        merge_output.to_string(),
        OUT_FILE_P1.to_string(),
    )
    .unwrap();
    assert_bgen_equality("data_test/samp_100_var_100.bgen", merge_output);
    std::fs::remove_file(OUT_FILE_P2).unwrap();
    std::fs::remove_file(OUT_FILE_P1).unwrap();
    std::fs::remove_file(merge_output).unwrap();
    std::fs::remove_file(merge_name).unwrap();
}

fn create_bgen_and_read() -> BgenStream<Cursor<Vec<u8>>> {
    let bgen_bytes = include_bytes!("../data_test/samp_100_var_100.bgen");
    let mut bgen_stream = BgenStream::from_bytes(bgen_bytes.to_vec(), true).unwrap();
    bgen_stream.read_offset_and_header().unwrap();
    bgen_stream
}

fn assert_bgen_equality(file_1: &str, file_2: &str) {
    let mut bgen_stream_1 = BgenStream::from_path(file_1, false, true).unwrap();
    bgen_stream_1.read_offset_and_header().unwrap();
    let mut bgen_stream_2 = BgenStream::from_path(file_2, false, true).unwrap();
    bgen_stream_2.read_offset_and_header().unwrap();

    assert_eq!(bgen_stream_1.header, bgen_stream_2.header);
    assert_eq!(bgen_stream_1.samples, bgen_stream_2.samples);
    //dbg!("reading data block 1");
    let data_blocks_1 = bgen_stream_1.collect::<Result<Vec<_>, _>>().unwrap();
    let data_blocks_2 = bgen_stream_2.collect::<Result<Vec<_>, _>>().unwrap();
    dbg!(&data_blocks_2);
    assert_eq!(
        data_blocks_1.len(),
        data_blocks_2.len(),
        "Length is not equal !"
    );
    for i in 0..data_blocks_2.len() {
        println!("i: {i}");
        assert_eq!(data_blocks_1[i], data_blocks_2[i]);
    }
}
