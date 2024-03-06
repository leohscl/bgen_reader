extern crate bgen_reader;
use bgen_reader::bgen::{BgenSteam, VariantData};
use std::io::Cursor;

#[test]
fn variants_num_read() {
    let bgen_stream = create_bgen_and_read();
    assert_eq!(100, bgen_stream.variants_data.len());
}

#[test]
fn first_variant_correct() {
    let bgen_stream = create_bgen_and_read();
    let variant_data = VariantData {
        number_individuals: None,
        variants_id: "".to_string(),
        rsid: "1_752566_G_A".to_string(),
        chr: "1".to_string(),
        pos: 752566,
        number_alleles: 2,
        alleles: vec!["G".to_string(), "A".to_string()],
        file_start_position: 1732,
        size_in_bytes: 127,
    };
    assert_eq!(variant_data, bgen_stream.variants_data[0]);
}

fn create_bgen_and_read() -> BgenSteam<Cursor<Vec<u8>>> {
    let bgen_bytes = include_bytes!("../data_test/samp_100_var_100.bgen");
    let mut bgen_stream = BgenSteam::from_bytes(bgen_bytes.to_vec()).unwrap();
    bgen_stream.read_offset_and_header().unwrap();
    bgen_stream.read_all_variant_data().unwrap();
    bgen_stream
}
