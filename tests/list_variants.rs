extern crate bgen_reader;
use bgen_reader::bgen::BgenSteam;
use bgen_reader::parser::ListArgs;
use bgen_reader::variant_data::{DataBlock, VariantData};
use std::io::Cursor;

#[test]
fn variants_num_read() {
    let bgen_stream = create_bgen_and_read();
    let variant_data: Vec<_> = bgen_stream.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(100, variant_data.len());
}

#[test]
fn first_variant_correct() {
    let bgen_stream = create_bgen_and_read();
    let variant_data: Vec<_> = bgen_stream.into_iter().map(|r| r.unwrap()).collect();
    let first_variant_data = VariantData {
        number_individuals: None,
        variants_id: "".to_string(),
        rsid: "1_752566_G_A".to_string(),
        chr: "1".to_string(),
        pos: 752566,
        number_alleles: 2,
        alleles: vec!["G".to_string(), "A".to_string()],
        file_start_position: 1732,
        size_in_bytes: 127,
        data_block: DataBlock::default(),
    };
    assert_eq!(first_variant_data, variant_data[0]);
}

#[test]
fn test_no_filter() {
    let mut bgen_stream = create_bgen_and_read();
    let list_args = ListArgs::default();
    bgen_stream.collect_filters(list_args);
    let variant_data: Vec<_> = bgen_stream.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(100, variant_data.len());
}

#[test]
fn test_filter() {
    let mut bgen_stream = create_bgen_and_read();
    let list_args = ListArgs::with_incl_str("1:0-752567".to_string());
    bgen_stream.collect_filters(list_args);
    let variant_data: Vec<_> = bgen_stream.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(1, variant_data.len());
}

fn create_bgen_and_read() -> BgenSteam<Cursor<Vec<u8>>> {
    let bgen_bytes = include_bytes!("../data_test/samp_100_var_100.bgen");
    let mut bgen_stream = BgenSteam::from_bytes(bgen_bytes.to_vec()).unwrap();
    bgen_stream.read_offset_and_header().unwrap();
    bgen_stream
}
