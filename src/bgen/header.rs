use crate::bgen::utils::write_u32;
use color_eyre::Result;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Header {
    pub start_data_offset: u32,
    pub header_size: u32,
    pub variant_num: u32,
    pub variant_count: u32,
    pub sample_num: u32,
    pub header_flags: HeaderFlags,
}

impl Header {
    pub fn write_header(&self, writer: &mut BufWriter<File>) -> Result<()> {
        write_u32(writer, self.start_data_offset)?;
        write_u32(writer, self.header_size)?;
        write_u32(writer, self.variant_num)?;
        write_u32(writer, self.sample_num)?;
        writer.write_all(b"bgen")?;
        write_u32(writer, self.header_flags.to_u32())?;
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct HeaderFlags {
    pub compressed_snp_blocks: bool,
    pub layout_id: u8,
    pub sample_id_present: bool,
}

impl HeaderFlags {
    pub fn from_u32(value: u32) -> Result<HeaderFlags> {
        let compressed_snp_blocks = (value & 1) == 1;
        let sample_id_present = ((value >> 31) & 1) == 1;
        let layout_id = ((value >> 2) & 3) as u8;
        Ok(HeaderFlags {
            compressed_snp_blocks,
            layout_id,
            sample_id_present,
        })
    }
    fn to_u32(&self) -> u32 {
        ((self.sample_id_present as u32) << 31)
            + (self.compressed_snp_blocks as u32)
            + ((self.layout_id as u32) << 2)
    }
}
