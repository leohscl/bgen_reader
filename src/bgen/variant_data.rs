use crate::bgen::utils::{
    compress_data, write_u16, write_u16_sized_string, write_u32, write_u32_sized_string, write_u8,
};
use crate::parser::{Range, VariantOutput};
use color_eyre::Result;
use core::panic;
use derivative::Derivative;
use itertools::Itertools;
use numtoa::NumToA;
use std::fs::File;
use std::io::{BufWriter, Write};

#[derive(Derivative)]
#[derivative(Default, Debug, PartialEq, Eq, Clone)]
pub struct VariantData {
    pub number_individuals: Option<u32>,
    pub variants_id: String,
    pub rsid: String,
    pub chr: String,
    pub pos: u32,
    pub number_alleles: u16,
    pub alleles: Vec<String>,
    #[derivative(PartialEq = "ignore")]
    pub file_start_position: usize,
    #[derivative(PartialEq = "ignore")]
    pub size_in_bytes: usize,
    pub data_block: DataBlock,
}

#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct DataBlock {
    pub number_individuals: u32,
    pub number_alleles: u16,
    pub minimum_ploidy: u8,
    pub maximum_ploidy: u8,
    pub ploidy_missingness: Vec<u8>,
    pub phased: bool,
    pub bytes_probability: u8,
    pub probabilities: Vec<u32>,
}
static SEPARATOR: &[u8] = "\t".as_bytes();

impl VariantData {
    fn bgenix_print(&self, mut writer: impl Write) -> Result<()> {
        let mut buffer = [0u8; 20];
        Self::write_with_sep(&mut writer, self.variants_id.as_bytes())?;
        Self::write_with_sep(&mut writer, self.rsid.as_bytes())?;
        let b_pos = self.pos.numtoa(10, &mut buffer);
        Self::write_with_sep(&mut writer, b_pos)?;
        let b_number_alleles = self.number_alleles.numtoa(10, &mut buffer);
        Self::write_with_sep(&mut writer, b_number_alleles)?;
        Self::write_with_sep(&mut writer, self.alleles[0].as_bytes())?;
        Self::write_with_sep(&mut writer, self.alleles[1].as_bytes())?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    pub fn print(&self, writer: impl Write, variant_output: VariantOutput) -> Result<()> {
        match variant_output {
            VariantOutput::Bgenix => self.bgenix_print(writer),
            VariantOutput::Rsid => self.rsid_print(writer),
        }
    }

    fn rsid_print(&self, mut writer: impl Write) -> Result<()> {
        writer.write_all(self.rsid.as_bytes())?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    pub fn write_with_sep(mut writer: impl Write, b: &[u8]) -> Result<()> {
        if b == b"" {
            writer.write_all(b".")?;
        } else {
            writer.write_all(b)?;
        }
        writer.write_all(SEPARATOR)?;
        Ok(())
    }

    pub fn write_vcf_line(&self, mut writer: impl Write) -> Result<()> {
        let separator = "\t".as_bytes();
        writer.write_all(self.chr.as_bytes())?;
        writer.write_all(separator)?;
        writer.write_all(self.pos.to_string().as_bytes())?;
        writer.write_all(separator)?;
        writer.write_all(self.rsid.as_bytes())?;
        writer.write_all(separator)?;
        writer.write_all(self.alleles[0].as_bytes())?;
        writer.write_all(separator)?;
        writer.write_all(self.alleles[1].as_bytes())?;
        writer.write_all(separator)?;
        for _ in 0..3 {
            writer.write_all(".".as_bytes())?;
            writer.write_all(separator)?;
        }
        writer.write_all("GT:GP".as_bytes())?;
        writer.write_all(separator)?;
        let mut taken: usize = 0;
        for ploidy_miss in &self.data_block.ploidy_missingness {
            let missingness = ploidy_miss & (1 << 7);
            if missingness == 1 {
                continue;
            }
            let ploidy = (ploidy_miss & ((1 << 7) - 1)) as usize;
            assert_eq!(ploidy, 2, "ploidy other than 2 not yet supported");
            let until = taken + ploidy;
            let (vec_calls, vec_geno) = if self.data_block.phased {
                let vec_geno_phased_f = self.data_block.probabilities[taken..until]
                    .iter()
                    .map(|&n| n as f64 / 65535f64)
                    .collect_vec();
                let vec_calls = Self::geno_to_calls(&vec_geno_phased_f);
                let vec_geno = vec_geno_phased_f
                    .iter()
                    .map(|&p_g| f64_round(p_g))
                    .collect_vec();
                (vec_calls, vec_geno)
            } else {
                let vec_calls_unphased = Self::calls_probabilities_unphased(
                    &self.data_block.probabilities[taken..until],
                );
                let vec_geno_unphased = Self::calls_to_geno_unphased_raw(&vec_calls_unphased);
                (vec_calls_unphased, vec_geno_unphased)
            };
            itertools::Itertools::intersperse(
                vec_geno.into_iter().map(|g| match g {
                    0 => "0",
                    1 => "1",
                    2 => "2",
                    _ => panic!("genotype not in 0..2"),
                }),
                "|",
            )
            .try_for_each(|s| writer.write_all(s.as_bytes()))?;
            writer.write_all(b":")?;

            let mut buffer = ryu::Buffer::new();
            for i in 0..vec_calls.len() {
                writer.write_all(buffer.format(vec_calls[i]).as_bytes())?;
                if i != vec_calls.len() - 1 {
                    writer.write_all(",".as_bytes())?;
                }
            }
            writer.write_all(separator)?;
            taken = until;
        }
        writer.write_all(b"\n")?;
        Ok(())
    }

    fn calls_to_geno_unphased_raw(vec_calls: &[f64]) -> Vec<u8> {
        let ph1 = f64_round(vec_calls[2]);
        let ph2 = f64_round(vec_calls[2] + vec_calls[1]);
        [ph1, ph2].to_vec()
    }

    fn geno_to_calls(slice_geno: &[f64]) -> Vec<f64> {
        let mut vec_ret = Vec::with_capacity(3);
        let p00 = slice_geno[0] * slice_geno[1];
        let p11 = (1f64 - slice_geno[0]) * (1f64 - slice_geno[1]);
        let p10 = 1f64 - p00 - p11;
        vec_ret.extend([p00, p10, p11]);
        vec_ret
    }

    fn calls_probabilities_unphased(vec_geno: &[u32]) -> Vec<f64> {
        let mut vec_probas = Vec::with_capacity(3);
        let mut iter_probas = vec_geno.iter().map(|e| *e as f64 / 65535f64);
        let p00 = iter_probas.next().unwrap();
        let p10 = iter_probas.next().unwrap();
        let p11 = 1f64 - p10 - p00;
        vec_probas.push(p00);
        vec_probas.push(p10);
        vec_probas.push(p11);
        vec_probas
    }

    pub fn filter_with_args(
        &self,
        incl_ranges: &[Range],
        incl_rsids: &[std::string::String],
        excl_ranges: &[Range],
        excl_rsid: &[std::string::String],
    ) -> bool {
        // edge case: no inclusion filters, all variants are included if not excluded
        if incl_ranges.is_empty() && incl_rsids.is_empty() {
            return !self.in_filters(excl_ranges, excl_rsid);
        }
        self.in_filters(incl_ranges, incl_rsids) && !self.in_filters(excl_ranges, excl_rsid)
    }

    fn in_filters(&self, ranges: &[Range], rsids: &[std::string::String]) -> bool {
        let in_ranges = ranges.iter().any(|r| self.in_range(r));
        let in_rsids = rsids.iter().any(|r| &self.rsid == r);
        in_rsids || in_ranges
    }

    fn in_range(&self, range: &Range) -> bool {
        range.chr == self.chr && range.start <= self.pos && self.pos <= range.end
    }
    pub fn write_self(self, writer: &mut BufWriter<File>, layout_id: u8) -> Result<()> {
        if layout_id == 1 {
            write_u32(writer, self.number_individuals.unwrap())?;
        }
        write_u16_sized_string(writer, self.variants_id)?;
        write_u16_sized_string(writer, self.rsid)?;
        write_u16_sized_string(writer, self.chr)?;
        write_u32(writer, self.pos)?;
        if layout_id != 1 {
            write_u16(writer, self.number_alleles)?;
        }
        self.alleles
            .into_iter()
            .map(|allele| write_u32_sized_string(writer, allele))
            .collect::<Result<Vec<_>>>()?;
        Self::write_data_block(writer, self.data_block)?;
        Ok(())
    }

    fn write_data_block(writer: &mut BufWriter<File>, data_block: DataBlock) -> Result<()> {
        let mut data = Vec::new();
        let mut data_writer = BufWriter::new(&mut data);
        write_u32(&mut data_writer, data_block.number_individuals)?;
        write_u16(&mut data_writer, data_block.number_alleles)?;
        write_u8(&mut data_writer, data_block.minimum_ploidy)?;
        write_u8(&mut data_writer, data_block.maximum_ploidy)?;
        data_block
            .ploidy_missingness
            .into_iter()
            .map(|p| write_u8(&mut data_writer, p))
            .collect::<Result<Vec<_>>>()?;
        write_u8(&mut data_writer, data_block.phased as u8)?;
        write_u8(&mut data_writer, data_block.bytes_probability)?;
        assert_eq!(data_block.bytes_probability % 8, 0);
        let chunk_size = (data_block.bytes_probability / 8) as usize;
        data_block
            .probabilities
            .into_iter()
            .map(|probability| {
                probability
                    .to_le_bytes()
                    .into_iter()
                    .take(chunk_size)
                    .map(|byte_proba| write_u8(&mut data_writer, byte_proba))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        data_writer.flush()?;
        drop(data_writer);
        let uncompressed_length = data.len() as u32;
        let block = compress_data(&data)?;
        let length_data_block = block.len() as u32 + 4;
        write_u32(writer, length_data_block)?;
        write_u32(writer, uncompressed_length)?;
        writer.write_all(&block)?;
        Ok(())
    }
}

fn f64_round(f: f64) -> u8 {
    match f {
        x if (0f64..=0.5f64).contains(&x) => 0,
        x if (0.5f64..=1.5f64).contains(&x) => 1,
        x if (1.5f64..=2f64).contains(&x) => 2,
        _ => panic!("float not between 0 and 2 !"),
    }
}
