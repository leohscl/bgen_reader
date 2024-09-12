extern crate bgen_reader;
use bgen_reader::bgen::bgen_stream::BgenStream;
use bgen_reader::bgen::variant_data::{DataBlock, VariantData};
use bgen_reader::parser::FilterArgs;
use std::io::Cursor;
use std::io::Write;
use tempfile::tempdir;

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

    let probabilities = [
        0, 65535, 0, 65535, 65535, 0, 65535, 0, 65535, 0, 0, 0, 0, 65535, 65535, 0, 0, 65535, 0,
        65535, 65535, 0, 65535, 0, 0, 0, 0, 65535, 65535, 0, 0, 65535, 0, 65535, 0, 0, 0, 65535,
        65535, 0, 65535, 0, 65535, 0, 65535, 0, 65535, 0, 0, 65535, 0, 65535, 0, 65535, 65535, 0,
        0, 65535, 0, 65535, 65535, 0, 0, 0, 0, 65535, 0, 65535, 0, 65535, 65535, 0, 0, 65535,
        65535, 0, 65535, 0, 0, 0, 65535, 0, 0, 65535, 0, 65535, 0, 0, 65535, 0, 0, 65535, 65535, 0,
        0, 65535, 0, 65535, 65535, 0, 65535, 0, 0, 0, 65535, 0, 0, 65535, 0, 65535, 0, 0, 65535, 0,
        0, 65535, 0, 65535, 65535, 0, 65535, 0, 65535, 0, 0, 65535, 0, 65535, 65535, 0, 0, 65535,
        0, 65535, 65535, 0, 0, 65535, 0, 0, 0, 0, 0, 65535, 65535, 0, 65535, 0, 0, 0, 0, 65535,
        65535, 0, 0, 65535, 0, 0, 0, 0, 0, 0, 65535, 0, 0, 0, 0, 0, 65535, 0, 0, 0, 65535, 0, 0,
        65535, 0, 65535, 0, 65535, 0, 65535, 0, 65535, 65535, 0, 65535, 0, 0, 65535, 65535, 0, 0,
        65535, 0, 65535, 0, 0, 65535, 0,
    ]
    .to_vec();
    let data_block = DataBlock {
        number_individuals: 100,
        number_alleles: 2,
        minimum_ploidy: 2,
        maximum_ploidy: 2,
        ploidy_missingness: [
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        ]
        .to_vec(),
        phased: false,
        bytes_probability: 16,
        probabilities,
    };
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
        data_block,
    };
    assert_eq!(first_variant_data, variant_data[0]);
}

#[test]
fn test_no_filter() {
    let mut bgen_stream = create_bgen_and_read();
    let list_args = FilterArgs::default();
    bgen_stream.collect_filters(list_args).unwrap();
    let variant_data: Vec<_> = bgen_stream.map(|r| r.unwrap()).collect();
    assert_eq!(100, variant_data.len());
}

#[test]
fn test_filter() {
    let mut bgen_stream = create_bgen_and_read();
    let list_args = FilterArgs::default().with_range_incl_str("1:0-752567".to_string());
    bgen_stream.collect_filters(list_args).unwrap();
    let variant_data: Vec<_> = bgen_stream.map(|r| r.unwrap()).collect();
    assert_eq!(1, variant_data.len());
}

#[test]
fn test_filter_rsid() {
    let mut bgen_stream = create_bgen_and_read();
    let list_args = FilterArgs::default().with_rsid_incl_str("1_752566_G_A".to_string());
    bgen_stream.collect_filters(list_args).unwrap();
    let variant_data: Vec<_> = bgen_stream.map(|r| r.unwrap()).collect();
    assert_eq!(1, variant_data.len());
    assert_eq!(
        ["1_752566_G_A",].to_vec(),
        variant_data
            .into_iter()
            .map(|v| v.rsid)
            .collect::<Vec<String>>()
    );
}

#[test]
fn test_double_filter() {
    let mut bgen_stream = create_bgen_and_read();
    let list_args = FilterArgs::default()
        .with_range_incl_str("1:0-900000".to_string())
        .with_range_excl_str("1:800000-850000".to_string());
    bgen_stream.collect_filters(list_args).unwrap();
    let variant_data: Vec<_> = bgen_stream.map(|r| r.unwrap()).collect();
    assert_eq!(
        [
            "1_752566_G_A",
            "1_752721_A_G",
            "1_873558_G_T",
            "1_881627_G_A",
            "1_888659_T_C",
            "1_891945_A_G",
            "1_894573_G_A"
        ]
        .to_vec(),
        variant_data
            .into_iter()
            .map(|v| v.rsid)
            .collect::<Vec<String>>()
    );
}

#[test]
fn test_filter_file() {
    let mut bgen_stream = create_bgen_and_read();
    let dir = tempdir().unwrap();
    let filename = "tmp_range";
    let filepath = dir.path().join(filename);
    let mut file = std::fs::File::create(filepath.clone()).unwrap();
    writeln!(file, "1:0-752567").unwrap();
    let list_args = FilterArgs::default()
        .with_range_incl_file(filepath.into_os_string().into_string().unwrap());
    bgen_stream.collect_filters(list_args).unwrap();
    let variant_data: Vec<_> = bgen_stream.map(|r| r.unwrap()).collect();
    assert_eq!(1, variant_data.len());
}

#[test]
fn test_filter_rsid_file() {
    let mut bgen_stream = create_bgen_and_read();
    let dir = tempdir().unwrap();
    let filename = "tmp_range";
    let filepath = dir.path().join(filename);
    let mut file = std::fs::File::create(filepath.clone()).unwrap();
    writeln!(file, "1_752566_G_A").unwrap();
    writeln!(file, "1_752721_A_G").unwrap();
    writeln!(file, "1_873558_G_T").unwrap();
    writeln!(file, "1_881627_G_A").unwrap();
    writeln!(file, "1_888659_T_C").unwrap();
    writeln!(file, "1_891945_A_G").unwrap();
    writeln!(file, "1_894573_G_A").unwrap();
    let list_args =
        FilterArgs::default().with_rsid_incl_file(filepath.into_os_string().into_string().unwrap());
    bgen_stream.collect_filters(list_args).unwrap();
    let variant_data: Vec<_> = bgen_stream.map(|r| r.unwrap()).collect();
    assert_eq!(7, variant_data.len());
    assert_eq!(
        [
            "1_752566_G_A",
            "1_752721_A_G",
            "1_873558_G_T",
            "1_881627_G_A",
            "1_888659_T_C",
            "1_891945_A_G",
            "1_894573_G_A"
        ]
        .to_vec(),
        variant_data
            .into_iter()
            .map(|v| v.rsid)
            .collect::<Vec<String>>()
    );
}

#[test]
fn test_double_filter_file() {
    let mut bgen_stream = create_bgen_and_read();
    let dir = tempdir().unwrap();
    let filename_incl = "tmp_range_incl";
    let filepath_incl = dir.path().join(filename_incl);
    let mut file_incl = std::fs::File::create(filepath_incl.clone()).unwrap();
    writeln!(file_incl, "1:0-900000").unwrap();
    let filename_excl = "tmp_range_excl";
    let filepath_excl = dir.path().join(filename_excl);
    let mut file_excl = std::fs::File::create(filepath_excl.clone()).unwrap();
    writeln!(file_excl, "1:800000-850000").unwrap();
    let list_args = FilterArgs::default()
        .with_range_incl_file(filepath_incl.into_os_string().into_string().unwrap())
        .with_range_excl_file(filepath_excl.into_os_string().into_string().unwrap());
    bgen_stream.collect_filters(list_args).unwrap();
    let variant_data: Vec<_> = bgen_stream.map(|r| r.unwrap()).collect();
    assert_eq!(
        [
            "1_752566_G_A",
            "1_752721_A_G",
            "1_873558_G_T",
            "1_881627_G_A",
            "1_888659_T_C",
            "1_891945_A_G",
            "1_894573_G_A"
        ]
        .to_vec(),
        variant_data
            .into_iter()
            .map(|v| v.rsid)
            .collect::<Vec<String>>()
    );
}

fn create_bgen_and_read() -> BgenStream<Cursor<Vec<u8>>> {
    let bgen_bytes = include_bytes!("../data_test/samp_100_var_100.bgen");
    let mut bgen_stream = BgenStream::from_bytes(bgen_bytes.to_vec(), true).unwrap();
    bgen_stream.read_offset_and_header().unwrap();
    bgen_stream
}
