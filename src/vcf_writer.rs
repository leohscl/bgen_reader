use color_eyre::Result;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;

use crate::bgen::BgenSteam;
const HEADER_LINES: &[&str] = &[
    "##fileformat=VCFv4.2\n",
    "##FORMAT=<ID=GT,Type=String,Number=1,Description=\"Threshholded genotype call\">\n",
    "##FORMAT=<ID=GP,Type=Float,Number=G,Description=\"Genotype call probabilities\">\n",
    "##FORMAT=<ID=HP,Type=Float,Number=.,Description=\"Haplotype call probabilities\">\n",
];

pub fn write_vcf<T: std::io::Read>(output_path: &str, bgen_stream: BgenSteam<T>) -> Result<()> {
    let file = File::create(output_path)?;
    let mut writer = BufWriter::new(file);
    for line in HEADER_LINES {
        writer.write_all(line.as_bytes())?;
    }
    write!(writer, "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO")?;
    write!(writer, "\tFORMAT")?;
    for sample in bgen_stream.samples.iter() {
        writer.write_all(b"\t")?;
        writer.write_all(sample.as_bytes())?;
    }
    writer.write_all(b"\n")?;
    bgen_stream
        .into_iter()
        .try_for_each(|v_data| v_data?.write_vcf_line(&mut writer))?;
    Ok(())
}
