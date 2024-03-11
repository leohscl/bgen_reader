use color_eyre::Result;
use std::fs::File;
use std::io::Write;
use vcf::VCFHeader;
use vcf::VCFHeaderLine;
use vcf::VCFWriter;

use crate::bgen::BgenSteam;

pub fn write_vcf<T: std::io::Read>(output_path: &str, bgen_stream: BgenSteam<T>) -> Result<()> {
    let writer = File::create(output_path)?;
    let line_version = b"##fileformat=VCFv4.2\n".to_vec();
    //
    let header_line_0 = VCFHeaderLine::from_bytes(&line_version, 0)?;
    let header_line_1 = VCFHeaderLine::from_bytes(
        b"##FORMAT=<ID=GT,Type=String,Number=1,Description=\"Thresholded genotype call\">\n",
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
    let mut vcf_writer = vcf::VCFWriter::new(writer, &header)?;
    bgen_stream
        .into_iter()
        .map(|var_data| Ok(vcf_writer.write_record(&var_data?.to_record(header.clone())?)?))
        .collect()
}

fn write_header<T: Write>(writer: T) -> Result<VCFWriter<T>> {
    let line_version = b"##fileformat=VCFv4.2\n".to_vec();
    let header_line_0 = VCFHeaderLine::from_bytes(&line_version, 0)?;
    let header_line_1 = VCFHeaderLine::from_bytes(
        b"##FORMAT=<ID=GT,Type=String,Number=1,Description=\"Thresholded genotype call\">\n",
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
