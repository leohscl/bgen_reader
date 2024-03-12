use crate::parser::ListArgs;
use crate::parser::Range;
use crate::variant_data::{DataBlock, VariantData};
use color_eyre::Report;
use color_eyre::Result;
use itertools::Itertools;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Read;
use std::path::Path;
use std::time::SystemTime;

pub struct BgenSteam<T> {
    stream: BufReader<T>,
    pub start_data_offset: u32,
    pub header_size: u32,
    pub variant_num: u32,
    variant_count: u32,
    pub sample_num: u32,
    pub header_flags: HeaderFlags,
    pub byte_count: usize,
    pub metadata: Option<MetadataBgi>,
    pub incl_range: Vec<Range>,
    pub incl_rsids: Vec<String>,
    pub excl_range: Vec<Range>,
    pub excl_rsids: Vec<String>,
    pub samples: Vec<String>,
}

pub struct MetadataBgi {
    pub filename: String,
    pub file_size: u64,
    pub last_write_time: SystemTime,
    pub first_1000_bytes: Vec<u8>,
    pub index_creation_time: SystemTime,
}

macro_rules! read_into_buffer {
    ($buffer:ident, $self:ident, $bytes:literal) => {
        let mut $buffer = [0; $bytes];
        $self.add_counter($bytes);
        $self.read_exact(&mut $buffer)?;
    };
}
macro_rules! read_into_vector {
    ($buffer:ident, $self:ident, $bytes:ident) => {
        let mut $buffer = vec![0; $bytes];
        $self.add_counter($bytes);
        $self.read_exact(&mut $buffer.as_mut_slice())?;
    };
}

impl<T: Read> BgenSteam<T> {
    pub fn new(stream: BufReader<T>, metadata: Option<MetadataBgi>, samples: Vec<String>) -> Self {
        BgenSteam {
            stream,
            start_data_offset: 0,
            header_size: 0,
            variant_num: 0,
            variant_count: 0,
            sample_num: 0,
            header_flags: HeaderFlags::default(),
            byte_count: 0,
            metadata,
            incl_range: vec![],
            incl_rsids: vec![],
            excl_range: vec![],
            excl_rsids: vec![],
            samples,
        }
    }
    fn add_counter(&mut self, bytes: usize) {
        self.byte_count += bytes;
    }
    pub fn read_offset_and_header(&mut self) -> Result<()> {
        self.start_data_offset = self.read_u32()?;
        println!("start_data_offset: {}", self.start_data_offset);
        self.header_size = self.read_u32()?;
        println!("Header size: {}", self.header_size);
        if self.header_size < 20 {
            return Err(Report::msg(
                "Header size of bgen is less than 20. The data is most likely corrupted",
            ));
        }
        self.variant_num = self.read_u32()?;
        println!("Number of variants: {}", self.variant_num);
        assert!(self.variant_num > 0);
        self.sample_num = self.read_u32()?;
        println!("Number of samples: {}", self.sample_num);
        assert!(self.sample_num > 0);
        read_into_buffer!(magic_num, self, 4);
        if !(&magic_num == &[0u8; 4] || &magic_num == b"bgen") {
            return Err(Report::msg(
                "Magic number in header is not correct. The data is most likely corrupted",
            ));
        }
        self.skip_bytes(self.header_size as usize - 20)?;
        self.header_flags = HeaderFlags::from_u32(self.read_u32()?)?;
        println!("Layout id: {}", self.header_flags.layout_id);

        if self.header_flags.sample_id_present {
            self.read_samples()?;
        }
        assert!(self.start_data_offset as usize == self.byte_count - 4);
        Ok(())
    }

    pub fn read_samples(&mut self) -> Result<()> {
        let _length_samples_id = self.read_u32()?;
        let num_samples = self.read_u32()?;
        let new_samples: Vec<_> = (0..num_samples)
            .map(|_| {
                let length_s = self.read_u16()?;
                self.read_string(length_s as usize)
            })
            .collect::<Result<Vec<_>>>()?;
        if !self.samples.is_empty() {
            assert_eq!(
                self.samples, new_samples,
                "Samples embedded in bgen file and in .sample file do not match."
            );
        }
        self.samples = new_samples;
        Ok(())
    }

    fn read_variant_data(&mut self) -> Result<VariantData> {
        let file_start_position = self.byte_count;
        let layout_id = self.header_flags.layout_id;
        let number_individuals = if layout_id == 1 {
            Some(self.read_u32()?)
        } else {
            None
        };
        let variants_id = self.read_u16_sized_string()?;
        let rsid = self.read_u16_sized_string()?;
        let chr = self.read_u16_sized_string()?;
        let pos = self.read_u32()?;
        let num_alleles = if layout_id == 1 { 2 } else { self.read_u16()? };
        let alleles: Result<Vec<String>> = (0..num_alleles)
            .map(|_| self.read_u32_sized_string())
            .collect();
        let read_data_block = false;
        let file_end_position = self.byte_count;
        let size_in_bytes = file_end_position - file_start_position;
        let data_block = if read_data_block {
            todo!()
        } else {
            let bytes_until_next_data_block = self.read_u32()?;
            self.skip_bytes(bytes_until_next_data_block as usize)?;
            DataBlock::default()
        };
        let variant_data = VariantData {
            number_individuals,
            variants_id,
            rsid,
            chr,
            pos,
            number_alleles: num_alleles,
            alleles: alleles?,
            file_start_position,
            size_in_bytes,
            data_block,
        };
        Ok(variant_data)
    }

    fn read_u32_sized_string(&mut self) -> Result<String> {
        let size = self.read_u32()? as usize;
        self.read_string(size)
    }

    fn read_u16_sized_string(&mut self) -> Result<String> {
        let size = self.read_u16()? as usize;
        self.read_string(size)
    }

    fn read_string(&mut self, size: usize) -> Result<String> {
        read_into_vector!(str_bytes, self, size);
        String::from_utf8(str_bytes).map_err(|e| e.into())
    }

    fn read_u16(&mut self) -> Result<u16> {
        read_into_buffer!(buffer, self, 2);
        Ok(buffer
            .iter()
            .enumerate()
            .map(|(i, b)| (1 << i * 8) * (*b as u16))
            .sum())
    }

    fn read_u32(&mut self) -> Result<u32> {
        read_into_buffer!(buffer, self, 4);
        Ok(buffer
            .iter()
            .enumerate()
            .map(|(i, b)| (1 << i * 8) * (*b as u32))
            .sum())
    }

    fn skip_bytes(&mut self, num_bytes: usize) -> Result<()> {
        if num_bytes > 0 {
            let mut vec = vec![0; num_bytes];
            self.add_counter(num_bytes);
            self.read_exact(vec.as_mut_slice())?;
        }
        Ok(())
    }

    pub fn collect_filters(&mut self, list_args: ListArgs) {
        let (vec_incl_range, vec_incl_rsid, vec_excl_range, vec_excl_rsid) =
            list_args.get_vector_incl_and_excl();
        self.incl_range = vec_incl_range;
        self.incl_rsids = vec_incl_rsid;
        self.excl_range = vec_excl_range;
        self.excl_rsids = vec_excl_rsid;
    }
}

impl<T: Read> Iterator for BgenSteam<T> {
    type Item = Result<VariantData>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.variant_count >= self.variant_num {
            None
        } else {
            while let Ok(var_data) = self.read_variant_data() {
                self.variant_count += 1;
                if var_data.filter_with_args(
                    &self.incl_range,
                    &self.incl_rsids,
                    &self.excl_range,
                    &self.excl_rsids,
                ) {
                    return Some(Ok(var_data));
                }
            }
            None
        }
    }
}

impl BgenSteam<File> {
    pub fn from_path(path_str: &str, use_sample_file: bool) -> Result<Self> {
        // Build metadata for file
        let path = Path::new(path_str);
        let filename = path.file_name().ok_or(Report::msg(format!(
            "File name cannot be extracted from {}",
            path_str
        )))?;
        let sample_path = path.with_extension("sample");
        let samples = if let Ok(file) = File::open(sample_path) {
            if use_sample_file {
                println!("Reading samples from .sample file");
                let samples_reader = BufReader::new(file);
                samples_reader
                    .lines()
                    .skip(2)
                    .flatten()
                    .map(|line| line.split_whitespace().take(2).join(" "))
                    .collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let metadata_std = std::fs::metadata(path)?;
        let file_size = metadata_std.len();
        let index_creation_time = metadata_std.created()?;
        let last_write_time = metadata_std.modified()?;
        let mut first_1000_bytes = vec![0; 1000];
        let mut file = File::open(path_str)?;
        file.read(first_1000_bytes.as_mut_slice())?;

        let file = File::open(path_str)?;
        let stream = BufReader::new(file);
        let metadata = MetadataBgi {
            filename: filename.to_str().unwrap().to_string(),
            file_size,
            index_creation_time,
            first_1000_bytes,
            last_write_time,
        };
        Ok(BgenSteam::new(stream, Some(metadata), samples))
    }
}

impl BgenSteam<Cursor<Vec<u8>>> {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        let stream = BufReader::new(Cursor::new(bytes));
        let metadata = None;
        Ok(BgenSteam::new(stream, metadata, vec![]))
    }
}

impl<T: Read> Read for BgenSteam<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}

impl<T: Read> BufRead for BgenSteam<T> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.stream.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.stream.consume(amt)
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct HeaderFlags {
    pub compressed_snp_blocks: bool,
    pub layout_id: u8,
    pub sample_id_present: bool,
}

impl HeaderFlags {
    fn from_u32(value: u32) -> Result<HeaderFlags> {
        let compressed_snp_blocks = (value & 1) == 1;
        let sample_id_present = ((value >> 31) & 1) == 1;
        let layout_id = ((value >> 2) & 3) as u8;
        Ok(HeaderFlags {
            compressed_snp_blocks,
            layout_id,
            sample_id_present,
        })
    }
}
