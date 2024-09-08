extern crate bgen_reader;
use bgen_reader::bgen::BgenStream;
use std::fs::File;
use std::io::Cursor;
use std::io::Read;
use std::u8;

#[test]
fn variants_num_read() {
    let bgen_stream = create_bgen_and_read();
    let output_file = "test.bgen";
    bgen_stream.to_bgen(output_file).unwrap();
    let mut file_test = File::open(output_file).unwrap();
    const NUM_BYTES_CMP: usize = 4050;
    let mut buffer_input = Vec::new();
    file_test.read_to_end(&mut buffer_input).unwrap();
    let bgen_bytes = include_bytes!("../data_test/samp_100_var_100.bgen")
        .iter()
        .take(NUM_BYTES_CMP)
        .cloned()
        .collect::<Vec<_>>();
    //for chunk in (0..NUM_BYTES_CMP).collect::<Vec<usize>>().chunks(50) {
    //    assert_eq!(
    //        bgen_bytes[chunk[0]..chunk[1]],
    //        buffer_input[chunk[0]..chunk[1]]
    //    );
    //}
    //assert_eq!(bgen_bytes, buffer_input);
}

fn create_bgen_and_read() -> BgenStream<Cursor<Vec<u8>>> {
    let bgen_bytes = include_bytes!("../data_test/samp_100_var_100.bgen");
    let mut bgen_stream = BgenStream::from_bytes(bgen_bytes.to_vec(), true).unwrap();
    bgen_stream.read_offset_and_header().unwrap();
    bgen_stream
}
