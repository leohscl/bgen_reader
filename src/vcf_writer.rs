use color_eyre::Result;
use itertools::Itertools;
use rayon::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use vcf::VCFHeader;
use vcf::VCFHeaderLine;
use vcf::VCFWriter;

use crate::bgen::BgenSteam;

pub fn write_vcf<T: std::io::Read>(output_path: &str, bgen_stream: BgenSteam<T>) -> Result<()> {
    let file = File::create(output_path)?;
    let reader = BufWriter::new(file);

    let line_version = b"##fileformat=VCFv4.2\n".to_vec();
    //
    let header_line_0 = VCFHeaderLine::from_bytes(&line_version, 0)?;
    let header_line_1 = VCFHeaderLine::from_bytes(
        b"##FORMAT=<ID=GT,Type=String,Number=1,Description=\"Threshholded genotype call\">\n",
        0,
    )?;
    let header_line_2 = VCFHeaderLine::from_bytes(
        b"##FORMAT=<ID=GP,Type=Float,Number=G,Description=\"Genotype call probabilities\">\n",
        0,
    )?;
    let header_line_3 = VCFHeaderLine::from_bytes(
        b"##FORMAT=<ID=HP,Type=Float,Number=.,Description=\"Haplotype call probabilities\">\n",
        0,
    )?;
    let vec_header_line = [header_line_0, header_line_1, header_line_2, header_line_3].to_vec();
    let vec_samples = bgen_stream
        .samples
        .clone()
        .into_iter()
        .map(|s| s.bytes().collect())
        .collect_vec();
    let header = VCFHeader::new(vec_header_line, vec_samples);
    let mut vcf_writer = vcf::VCFWriter::new(reader, &header)?;
    bgen_stream
        .into_iter()
        .chunks(1000)
        .into_iter()
        .try_for_each(|chunk| {
            let variants = chunk.into_iter().collect_vec();
            let vec_rec: Vec<_> = variants
                .par_iter()
                .map(|var_data| var_data.as_ref().unwrap().to_record(header.clone()))
                .collect();

            vec_rec
                .into_iter()
                .try_for_each(|rec| Ok(vcf_writer.write_record(&rec?)?))
        })
}

fn write_header<T: Write>(writer: T) -> Result<VCFWriter<T>> {
    let line_version = b"##fileformat=VCFv4.2\n".to_vec();
    let header_line_0 = VCFHeaderLine::from_bytes(&line_version, 0)?;
    let header_line_1 = VCFHeaderLine::from_bytes(
        b"##FORMAT=<ID=GT,Type=String,Number=1,Description=\"Threshholded genotype call\">\n",
        0,
    )?;
    let header_line_2 = VCFHeaderLine::from_bytes(
        b"##FORMAT=<ID=GP,Type=Float,Number=G,Description=\"Genotype call probabilities\">\n",
        0,
    )?;
    let header_line_3 = VCFHeaderLine::from_bytes(
        b"##FORMAT=<ID=HP,Type=Float,Number=.,Description=\"Haplotype call probabilities\">\n",
        0,
    )?;
    let vec_header_line = [header_line_0, header_line_1, header_line_2, header_line_3].to_vec();
    let vec_samples = vec![];
    let header = VCFHeader::new(vec_header_line, vec_samples);
    Ok(vcf::VCFWriter::new(writer, &header)?)
}

pub fn write_vcf_dummy() -> Result<()> {
    let output_vcf_path = "./output.vcf";
    let writer = File::create(output_vcf_path)?;
    write_header(writer)?;
    Ok(())
}
