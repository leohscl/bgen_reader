use crate::parser::Range;
use color_eyre::Result;
use core::panic;
use itertools::Itertools;
use vcf::{VCFHeader, VCFRecord};

#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct VariantData {
    pub number_individuals: Option<u32>,
    pub variants_id: String,
    pub rsid: String,
    pub chr: String,
    pub pos: u32,
    pub number_alleles: u16,
    pub alleles: Vec<String>,
    pub file_start_position: usize,
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
    pub probabilities: Vec<Vec<u32>>,
}

impl VariantData {
    pub fn bgenix_print(&self) -> String {
        [
            self.variants_id.to_string(),
            self.rsid.to_string(),
            self.pos.to_string(),
            self.number_alleles.to_string(),
            self.alleles[0].to_string(),
            self.alleles[1].to_string(),
        ]
        .join("\t")
    }

    pub fn to_record(&self, header: VCFHeader) -> Result<VCFRecord> {
        let mut record = VCFRecord::new(header);
        record.chromosome = self.chr.bytes().collect();
        record.position = self.pos as u64;
        record.id = vec![self.rsid.bytes().collect()];
        record.reference = self.alleles[0].bytes().collect();
        record.alternative = self.alleles[1..]
            .iter()
            .map(|allele| allele.bytes().chain(std::iter::once(b' ')).collect())
            .collect();
        record.format = vec![b"GT".to_vec(), b"GP".to_vec()];
        record.genotype = if self.data_block.phased {
            self.data_block
                .probabilities
                .iter()
                .map(|v| vec![Self::geno_to_bytes_phased(v), Self::geno_to_calls_phased(v)])
                .collect()
        } else {
            self.data_block
                .probabilities
                .iter()
                .map(|v| {
                    let vec_calls_unphased = Self::calls_probabilities_unphased_v21(v);
                    let vec_geno_unphased = Self::calls_to_geno_unphased(&vec_calls_unphased);
                    let vec_calls_fmt = vec_calls_unphased
                        .into_iter()
                        .map(Self::round_to_str)
                        .collect();
                    vec![vec_geno_unphased, vec_calls_fmt]
                })
                .collect()
        };
        Ok(record)
    }

    fn geno_to_bytes_phased(vec_geno: &[u32]) -> Vec<Vec<u8>> {
        vec_geno
            .iter()
            .map(|g| match g {
                0 => "1".bytes().collect(),
                65535 => "0".bytes().collect(),
                _ => panic!("unhandeled byte"),
            })
            .collect()
    }

    fn calls_to_geno_unphased(vec_calls: &[f64]) -> Vec<Vec<u8>> {
        let ph1 = f64_round_tobytes(vec_calls[2]);
        let ph2 = f64_round_tobytes(vec_calls[2] + vec_calls[1]);
        [ph1, ph2].to_vec()
    }

    fn calls_probabilities_unphased(vec_geno: &[u32]) -> Vec<f64> {
        let vec_probas = vec_geno.iter().map(|e| *e as f64 / 65535f64).collect_vec();
        let p00 = vec_probas[0];
        let p10 = vec_probas[1];
        let p11 = 1f64 - p10 - p00;
        [p00, p10, p11].to_vec()
    }

    fn calls_probabilities_unphased_v2(vec_geno: &[u32]) -> Vec<f64> {
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

    fn geno_to_calls_phased(vec_geno: &[u32]) -> Vec<Vec<u8>> {
        let vec_probas = vec_geno.iter().map(|e| *e as f64 / 65535f64).collect_vec();
        let p00 = vec_probas[0] * vec_probas[1];
        let p11 = (1f64 - vec_probas[0]) * (1f64 - vec_probas[1]);
        let pm = 1f64 - p00 - p11;
        [
            Self::round_to_str(p00),
            Self::round_to_str(pm),
            Self::round_to_str(p11),
        ]
        .to_vec()
    }

    fn round_to_str(f: f64) -> Vec<u8> {
        let mut buff = ryu::Buffer::new();
        buff.format(f).bytes().collect()
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
}

fn f64_round_tobytes(f: f64) -> Vec<u8> {
    match f {
        x if (0f64..=0.5f64).contains(&x) => "0".bytes().collect(),
        x if (0.5f64..=1.5f64).contains(&x) => "1".bytes().collect(),
        x if (1.5f64..=2f64).contains(&x) => "2".bytes().collect(),
        _ => panic!("float not between 0 and 2 !"),
    }
}
