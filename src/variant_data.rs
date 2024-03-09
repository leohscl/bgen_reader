use crate::parser::Range;
use color_eyre::Result;
use vcf::{VCFHeader, VCFRecord};

#[derive(Default, Debug, PartialEq, Eq)]
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
        Ok(record)
    }

    pub fn filter_with_args(
        &self,
        incl_ranges: &Vec<Range>,
        incl_rsids: &Vec<String>,
        excl_ranges: &Vec<Range>,
        excl_rsid: &Vec<String>,
    ) -> bool {
        // edge case: no inclusion filters, all variants are included if not excluded
        if incl_ranges.is_empty() && incl_rsids.is_empty() {
            return !self.in_filters(excl_ranges, excl_rsid);
        }
        self.in_filters(incl_ranges, incl_rsids) && !self.in_filters(excl_ranges, excl_rsid)
    }

    fn in_filters(&self, ranges: &Vec<Range>, rsids: &Vec<String>) -> bool {
        let in_ranges = ranges.iter().fold(false, |acc, r| acc || self.in_range(r));
        let in_rsids = rsids.iter().fold(false, |acc, r| acc || &self.rsid == r);
        in_rsids || in_ranges
    }

    fn in_range(&self, range: &Range) -> bool {
        range.chr == self.chr && range.start <= self.pos && self.pos <= range.end
    }
}
