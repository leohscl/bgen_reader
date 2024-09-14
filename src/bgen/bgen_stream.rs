use crate::bgen::header::{Header, HeaderFlags};
use crate::bgen::utils::{decompress_block, read_lines, write_u16, write_u32};
use crate::bgen::variant_data::{DataBlock, VariantData};
use crate::parser::{FilterArgs, Range};
use bitvec::prelude::*;
use color_eyre::{Report, Result};
use itertools::Itertools;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Cursor, Read, Write};
use std::path::Path;
use std::time::SystemTime;

pub struct BgenStream<T> {
    stream: BufReader<T>,
    read_data_block: bool,
    len_samples_block: u32,
    pub header: Header,
    pub metadata: MetadataBgi,
    pub ranges: Ranges,
    pub byte_count: usize,
    pub samples: Vec<String>,
}

pub trait BgenClone<T> {
    fn create_identical_bgen(&self) -> Result<BgenStream<T>>;
}

#[derive(Clone, Default, Debug)]
pub struct Ranges {
    pub incl_range: Vec<Range>,
    pub incl_rsids: Vec<String>,
    pub excl_range: Vec<Range>,
    pub excl_rsids: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct FileMetadata {
    pub filename: String,
    pub path: String,
    pub file_size: u64,
    pub last_write_time: SystemTime,
    pub first_1000_bytes: Vec<u8>,
    pub index_creation_time: SystemTime,
}

#[derive(Clone, Debug)]
pub struct BytesMetadata {
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub enum MetadataBgi {
    File(FileMetadata),
    Bytes(BytesMetadata),
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

impl<T: Read> BgenStream<T> {
    pub fn new(
        stream: BufReader<T>,
        metadata: MetadataBgi,
        samples: Vec<String>,
        read_data_block: bool,
    ) -> Self {
        let header = Header::default();
        let ranges = Ranges::default();
        BgenStream {
            stream,
            len_samples_block: 0,
            read_data_block,
            header,
            ranges,
            byte_count: 0,
            metadata,
            samples,
        }
    }

    fn add_counter(&mut self, bytes: usize) {
        self.byte_count += bytes;
    }

    pub fn read_offset_and_header(&mut self) -> Result<()> {
        let start_data_offset = self.read_u32()?;
        log::info!("start_data_offset: {}", start_data_offset);
        let header_size = self.read_u32()?;
        log::info!("Header size: {}", header_size);
        if header_size < 20 {
            return Err(Report::msg(
                "Header size of bgen is less than 20. The data is most likely corrupted",
            ));
        }
        let variant_num = self.read_u32()?;
        log::info!("Number of variants: {}", variant_num);
        assert!(variant_num > 0);
        let sample_num = self.read_u32()?;
        assert!(sample_num > 0);
        log::info!("Number of samples: {}", sample_num);
        read_into_buffer!(magic_num, self, 4);
        if !(magic_num == [0u8; 4] || &magic_num == b"bgen") {
            return Err(Report::msg(
                "Magic number in header is not correct. The data is most likely corrupted",
            ));
        }
        self.skip_bytes(header_size as usize - 20)?;
        let header_flags = HeaderFlags::from_u32(self.read_u32()?)?;
        log::info!("Layout id: {}", header_flags.layout_id);
        log::info!("sample_id_present: {}", header_flags.sample_id_present);
        if header_flags.sample_id_present {
            self.read_samples()?;
        }

        log::info!("byte_count: {}", self.byte_count);
        log::info!("start_data_offset: {}", start_data_offset);
        if start_data_offset as usize != (self.byte_count - 4) {
            log::warn!(
                "Header has extra bytes, starting at {} and ending at {}. File might be corrupted",
                self.byte_count - 4,
                start_data_offset
            );
            self.skip_bytes(start_data_offset as usize - (self.byte_count - 4))?;
        }
        self.header = Header {
            start_data_offset,
            header_size,
            variant_num,
            variant_count: 0,
            sample_num,
            header_flags,
        };
        Ok(())
    }

    pub fn read_samples(&mut self) -> Result<()> {
        let len_samples_block = self.read_u32()?;
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
        self.len_samples_block = len_samples_block;
        self.samples = new_samples;
        Ok(())
    }

    fn read_variant_data(&mut self) -> Result<VariantData> {
        let file_start_position = self.byte_count;
        let layout_id = self.header.header_flags.layout_id;
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
        let data_block = if self.read_data_block {
            self.read_data_block()?
        } else {
            let bytes_until_next_data_block = self.read_u32()?;
            self.skip_bytes(bytes_until_next_data_block as usize)?;
            DataBlock::default()
        };
        let file_end_position = self.byte_count;
        let size_in_bytes = file_end_position - file_start_position;
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

    fn read_data_block(&mut self) -> Result<DataBlock> {
        assert_eq!(
            self.header.header_flags.layout_id, 2,
            "Layouts other than 2 are not yet supported"
        );
        let length_data_block = self.read_u32()?;
        let compressed_snp_blocks = self.header.header_flags.compressed_snp_blocks;
        let uncompressed_length = if compressed_snp_blocks {
            self.read_u32()?
        } else {
            length_data_block
        };
        // TODO(lhenches): handle other cases
        assert!(compressed_snp_blocks);
        let compressed_block = self.read_vector_length((length_data_block - 4) as usize)?;
        let uncompressed_block = decompress_block(compressed_block, uncompressed_length as usize)?;
        assert_eq!(
            uncompressed_block.len(),
            uncompressed_length as usize,
            "Uncompressed data length is expected to be the same as computed uncompressed length"
        );
        Self::build_from_uncompressed_block(uncompressed_block)
    }

    fn build_from_uncompressed_block(block: Vec<u8>) -> Result<DataBlock> {
        let mut bytes = block.into_iter();
        let number_individuals = u32::from_le_bytes(Self::convert(&mut bytes));
        let number_alleles = u16::from_le_bytes(Self::convert(&mut bytes));
        let minimum_ploidy = u8::from_le_bytes(Self::convert(&mut bytes));
        let maximum_ploidy = u8::from_le_bytes(Self::convert(&mut bytes));
        let mut ploidy_missingness = Vec::with_capacity(number_individuals as usize);
        for _ in 0..number_individuals {
            ploidy_missingness.push(bytes.next().unwrap());
        }
        let phased_u8 = u8::from_le_bytes(Self::convert(&mut bytes));
        let phased = match phased_u8 {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Report::msg("Phased byte is incorrect")),
        }?;
        let bytes_probability = u8::from_le_bytes(Self::convert(&mut bytes));
        let remaining_bytes: Vec<_> = bytes.collect();
        let all_probabilities: Vec<_> = if bytes_probability % 8 == 0 {
            let chunk_size = (bytes_probability / 8) as usize;
            remaining_bytes
                .chunks(chunk_size)
                .map(|c| Self::convert_u8_chunk(c))
                .collect()
        } else {
            println!("Warning: the probability stored are not a multiple of 8");
            let iterate_bits = remaining_bytes.view_bits::<Lsb0>();
            iterate_bits
                .chunks(bytes_probability as usize)
                .map(|c| Self::convert_u32(c))
                .collect()
        };

        let data_block = DataBlock {
            number_individuals,
            bytes_probability,
            maximum_ploidy,
            minimum_ploidy,
            ploidy_missingness,
            phased,
            number_alleles,
            probabilities: all_probabilities,
        };

        Ok(data_block)
    }

    fn write_samples(
        samples: &[String],
        writer: &mut BufWriter<File>,
        len_samples_block: u32,
    ) -> Result<()> {
        write_u32(writer, len_samples_block)?;
        write_u32(writer, samples.len() as u32)?;
        for sample in samples {
            let bytes = sample.clone().into_bytes();
            write_u16(writer, bytes.len() as u16)?;
            writer.write_all(&bytes)?;
        }
        Ok(())
    }

    fn convert_u8_chunk(to_convert: &[u8]) -> u32 {
        to_convert
            .iter()
            .enumerate()
            .map(|(i, &b)| b as u32 * (1 << (i * 8)))
            .sum()
    }

    fn convert_u32(to_convert: &BitSlice<u8>) -> u32 {
        to_convert
            .into_iter()
            .map(|b| if *b { 1u32 } else { 0u32 })
            .enumerate()
            .map(|(i, b)| b * (1 << i))
            .sum()
    }

    fn convert<U: std::fmt::Debug, const N: usize>(block: &mut dyn Iterator<Item = U>) -> [U; N] {
        block
            .take(N)
            .collect::<Vec<U>>()
            .try_into()
            .expect("Conversion failed. Data is most likely corrupted")
    }
    fn read_vector_length(&mut self, length: usize) -> Result<Vec<u8>> {
        read_into_vector!(bytes, self, length);
        Ok(bytes)
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

    fn read_u32(&mut self) -> Result<u32> {
        read_into_buffer!(buffer, self, 4);
        Ok(u32::from_le_bytes(buffer))
    }

    fn read_u16(&mut self) -> Result<u16> {
        read_into_buffer!(buffer, self, 2);
        Ok(u16::from_le_bytes(buffer))
    }

    fn skip_bytes(&mut self, num_bytes: usize) -> Result<()> {
        io::copy(
            &mut std::io::Read::take(std::io::Read::by_ref(self), num_bytes.try_into()?),
            &mut io::sink(),
        )?;
        Ok(())
    }

    pub fn collect_filters(&mut self, list_args: FilterArgs) -> Result<()> {
        let (vec_incl_range, vec_incl_rsid, vec_excl_range, vec_excl_rsid) =
            list_args.get_vector_incl_and_excl()?;
        self.ranges.incl_range = vec_incl_range;
        self.ranges.incl_rsids = vec_incl_rsid;
        self.ranges.excl_range = vec_excl_range;
        self.ranges.excl_rsids = vec_excl_rsid;
        Ok(())
    }
}

pub fn bgen_merge(merge_filename: String, output_name: String, cli_filename: String) -> Result<()> {
    let file = File::create(output_name)?;
    let mut writer = BufWriter::new(file);
    let mut lines = read_lines(merge_filename.clone())?;
    if !lines.contains(&cli_filename) {
        lines.push(cli_filename)
    }
    lines.retain(|s| !s.is_empty());
    let mut num_variants = 0;
    // first pass to read
    println!("First pass for merging, computing the number of variants and checking samples");
    let mut samples = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        println!("Reading file {}, at line {}", line, i);
        let mut bgen_stream = BgenStream::from_path(line, false, false)?;
        bgen_stream.read_offset_and_header()?;
        num_variants += bgen_stream.header.variant_num;
        if i == 0 {
            samples = bgen_stream.samples;
        } else {
            assert_eq!(samples, bgen_stream.samples)
        }
    }

    println!("Second pass for merging, writing variant data");
    for (i, line) in lines.iter().enumerate() {
        println!("Reading file {}, at line {}", line, i);
        let mut bgen_stream = BgenStream::from_path(line, false, true)?;
        bgen_stream.read_offset_and_header()?;
        if i == 0 {
            let mut header = bgen_stream.header.clone();
            header.variant_num = num_variants;
            header.write_header(&mut writer)?;
            BgenStream::<File>::write_samples(
                &bgen_stream.samples,
                &mut writer,
                bgen_stream.len_samples_block,
            )?;
        }
        let mut buf = [0; 8192];
        loop {
            let len_read = bgen_stream.read(&mut buf)?;
            if len_read == 0 {
                break;
            } else {
                writer.write_all(&buf[0..len_read])?;
            }
        }
    }
    Ok(())
}

impl<T: Read> Iterator for BgenStream<T> {
    type Item = Result<VariantData>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.header.variant_count >= self.header.variant_num {
            None
        } else {
            while let Ok(var_data) = self.read_variant_data() {
                self.header.variant_count += 1;
                if var_data.filter_with_args(
                    &self.ranges.incl_range,
                    &self.ranges.incl_rsids,
                    &self.ranges.excl_range,
                    &self.ranges.excl_rsids,
                ) {
                    return Some(Ok(var_data));
                }
            }
            None
        }
    }
}

impl<T: Read> BgenStream<T>
where
    BgenStream<T>: BgenClone<T>,
{
    pub fn to_bgen(mut self, output_path: &str, no_samples: bool) -> Result<()> {
        let file = File::create(output_path)?;
        let mut writer = BufWriter::new(file);
        let mut other = self.create_identical_bgen()?;
        other.read_offset_and_header()?;
        self.read_data_block = false;
        // first pass to get the number of variants
        let mut header_final = self.header.clone();
        let num_variants = self.count();
        header_final.variant_num = num_variants as u32;
        if no_samples {
            header_final.header_flags.sample_id_present = false;
            header_final.start_data_offset -= 8u32
                + other
                    .samples
                    .iter()
                    .map(|s| s.len() as u32 + 2u32)
                    .sum::<u32>();
        }
        header_final.write_header(&mut writer)?;
        if header_final.header_flags.sample_id_present {
            Self::write_samples(&other.samples, &mut writer, other.len_samples_block)?;
        }
        let layout_id = other.header.header_flags.layout_id;
        other.try_for_each(|variant_data| {
            let var_data = variant_data?;
            var_data.write_self(&mut writer, layout_id)
        })
    }
}

impl BgenStream<File> {
    pub fn from_path(path_str: &str, use_sample_file: bool, read_data_block: bool) -> Result<Self> {
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
        let index_creation_time = metadata_std.created().unwrap_or(SystemTime::UNIX_EPOCH);
        let last_write_time = metadata_std.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let mut first_1000_bytes = vec![0; 1000];
        let mut file = File::open(path_str)?;
        file.read_exact(first_1000_bytes.as_mut_slice())?;

        let file = File::open(path_str)?;
        let stream = BufReader::new(file);
        let metadata_file = FileMetadata {
            filename: filename.to_str().unwrap().to_string(),
            path: path_str.to_string(),
            file_size,
            index_creation_time,
            first_1000_bytes,
            last_write_time,
        };
        Ok(BgenStream::new(
            stream,
            MetadataBgi::File(metadata_file),
            samples,
            read_data_block,
        ))
    }
}

impl BgenClone<File> for BgenStream<File> {
    fn create_identical_bgen(&self) -> Result<BgenStream<File>> {
        let mut new_bgen = match self.metadata.clone() {
            MetadataBgi::File(file_meta) => BgenStream::from_path(&file_meta.path, false, true),
            _ => Err(Report::msg(
                "No file metadata in bgen constructed from file",
            )),
        }?;
        new_bgen.ranges.clone_from(&self.ranges);
        Ok(new_bgen)
    }
}

impl BgenStream<Cursor<Vec<u8>>> {
    pub fn from_bytes(bytes: Vec<u8>, read_data_block: bool) -> Result<Self> {
        let metadata = MetadataBgi::Bytes(BytesMetadata {
            bytes: bytes.clone(),
        });
        let stream = BufReader::new(Cursor::new(bytes));
        Ok(BgenStream::new(stream, metadata, vec![], read_data_block))
    }
}

impl BgenClone<Cursor<Vec<u8>>> for BgenStream<Cursor<Vec<u8>>> {
    fn create_identical_bgen(&self) -> Result<BgenStream<Cursor<Vec<u8>>>> {
        let mut new_bgen = match self.metadata.clone() {
            MetadataBgi::Bytes(meta_bytes) => BgenStream::from_bytes(meta_bytes.bytes, true),
            MetadataBgi::File(_) => Err(Report::msg(
                "No bytes metadata in bgen constructed from file",
            )),
        }?;
        new_bgen.ranges.clone_from(&self.ranges);
        Ok(new_bgen)
    }
}

impl<T: Read> Read for BgenStream<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}

impl<T: Read> BufRead for BgenStream<T> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.stream.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.stream.consume(amt)
    }
}
