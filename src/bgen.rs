use crate::parser::ListArgs;
use crate::parser::Range;
use crate::variant_data::{DataBlock, VariantData};
use bitvec::prelude::*;
use color_eyre::Report;
use color_eyre::Result;
use flate2::bufread::ZlibDecoder;
use flate2::bufread::ZlibEncoder;
use flate2::Compression;
use itertools::Itertools;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::time::SystemTime;

pub struct BgenStream<T> {
    stream: BufReader<T>,
    read_data_block: bool,
    len_samples_block: u32,
    pub header: Header,
    pub metadata: MetadataBgi,
    pub byte_count: usize,
    pub incl_range: Vec<Range>,
    pub incl_rsids: Vec<String>,
    pub excl_range: Vec<Range>,
    pub excl_rsids: Vec<String>,
    pub samples: Vec<String>,
}

pub trait BgenClone<T> {
    fn create_identical_bgen(&self) -> Result<BgenStream<T>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub start_data_offset: u32,
    pub header_size: u32,
    pub variant_num: u32,
    variant_count: u32,
    pub sample_num: u32,
    pub header_flags: HeaderFlags,
}

impl Header {
    fn write_header(&self, writer: &mut BufWriter<File>) -> Result<()> {
        write_u32(writer, self.start_data_offset)?;
        write_u32(writer, self.header_size)?;
        write_u32(writer, self.variant_num)?;
        write_u32(writer, self.sample_num)?;
        writer.write_all(b"bgen")?;
        write_u32(writer, self.header_flags.to_u32())?;
        Ok(())
    }
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
        let header = Header {
            start_data_offset: 0,
            header_size: 0,
            variant_num: 0,
            variant_count: 0,
            sample_num: 0,
            header_flags: HeaderFlags::default(),
        };
        BgenStream {
            stream,
            len_samples_block: 0,
            read_data_block,
            header,
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
        log::info!("Number of samples: {}", sample_num);
        assert!(sample_num > 0);
        read_into_buffer!(magic_num, self, 4);
        if !(magic_num == [0u8; 4] || &magic_num == b"bgen") {
            return Err(Report::msg(
                "Magic number in header is not correct. The data is most likely corrupted",
            ));
        }
        self.skip_bytes(header_size as usize - 20)?;
        let header_flags = HeaderFlags::from_u32(self.read_u32()?)?;
        log::info!("Layout id: {}", header_flags.layout_id);

        if header_flags.sample_id_present {
            self.read_samples()?;
        }
        assert!(start_data_offset as usize == self.byte_count - 4);
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

    fn write_variant_data(
        variant_data: VariantData,
        writer: &mut BufWriter<File>,
        layout_id: u8,
    ) -> Result<()> {
        if layout_id == 1 {
            write_u32(writer, variant_data.number_individuals.unwrap())?;
        }
        write_u16_sized_string(writer, variant_data.variants_id)?;
        write_u16_sized_string(writer, variant_data.rsid)?;
        write_u16_sized_string(writer, variant_data.chr)?;
        write_u32(writer, variant_data.pos)?;
        if layout_id != 1 {
            write_u16(writer, variant_data.number_alleles)?;
        }
        variant_data
            .alleles
            .into_iter()
            .map(|allele| write_u32_sized_string(writer, allele))
            .collect::<Result<Vec<_>>>()?;
        Self::write_data_block(writer, variant_data.data_block)?;
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
        let uncompressed_block =
            Self::decompress_block(compressed_block, uncompressed_length as usize)?;
        assert_eq!(
            uncompressed_block.len(),
            uncompressed_length as usize,
            "Uncompressed data length is expected to be the same as computed uncompressed length"
        );
        Self::build_from_uncompressed_block(uncompressed_block)
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
        let block = Self::compress_data(&data)?;
        let length_data_block = block.len() as u32 + 4;
        write_u32(writer, length_data_block)?;
        write_u32(writer, uncompressed_length)?;
        writer.write_all(&block)?;
        Ok(())
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

    fn decompress_block(block: Vec<u8>, length: usize) -> Result<Vec<u8>> {
        let mut decoder = ZlibDecoder::new(Cursor::new(block));
        let mut decoded = vec![0; length];
        decoder
            .read_exact(&mut decoded)
            .map_err(|_| Report::msg("Error in decompression"))?;
        Ok(decoded)
    }

    fn compress_data(data: &Vec<u8>) -> Result<Vec<u8>> {
        let mut encoder = ZlibEncoder::new(Cursor::new(data), Compression::fast());
        let mut block = Vec::new();
        encoder.read_to_end(&mut block)?;
        Ok(block)
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

    pub fn collect_filters(&mut self, list_args: ListArgs) -> Result<()> {
        let (vec_incl_range, vec_incl_rsid, vec_excl_range, vec_excl_rsid) =
            list_args.get_vector_incl_and_excl()?;
        self.incl_range = vec_incl_range;
        self.incl_rsids = vec_incl_rsid;
        self.excl_range = vec_excl_range;
        self.excl_rsids = vec_excl_rsid;
        Ok(())
    }
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

impl<T: Read> BgenStream<T>
where
    BgenStream<T>: BgenClone<T>,
{
    pub fn to_bgen(mut self, output_path: &str) -> Result<()> {
        let mut other = self.create_identical_bgen()?;
        other.read_offset_and_header()?;
        self.read_data_block = false;
        // first pass to get the number of variants
        let num_variants = self.count();
        other.header.variant_num = num_variants as u32;
        let file = File::create(output_path)?;
        let mut writer = BufWriter::new(file);
        other.header.write_header(&mut writer)?;
        Self::write_samples(&other.samples, &mut writer, other.len_samples_block)?;
        let layout_id = other.header.header_flags.layout_id;
        other.try_for_each(|variant_data| {
            Self::write_variant_data(variant_data?, &mut writer, layout_id)
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
        let index_creation_time = metadata_std.created()?;
        let last_write_time = metadata_std.modified()?;
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
        match self.metadata.clone() {
            MetadataBgi::File(file_meta) => BgenStream::from_path(&file_meta.path, false, true),
            _ => Err(Report::msg(
                "No file metadata in bgen constructed from file",
            )),
        }
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

    pub fn create_identical_bgen(&self) -> Result<BgenStream<Cursor<Vec<u8>>>> {
        match self.metadata.clone() {
            MetadataBgi::Bytes(meta_bytes) => BgenStream::from_bytes(meta_bytes.bytes, true),
            MetadataBgi::File(_) => Err(Report::msg(
                "No bytes metadata in bgen constructed from file",
            )),
        }
    }
}

impl BgenClone<Cursor<Vec<u8>>> for BgenStream<Cursor<Vec<u8>>> {
    fn create_identical_bgen(&self) -> Result<BgenStream<Cursor<Vec<u8>>>> {
        match self.metadata.clone() {
            MetadataBgi::Bytes(meta_bytes) => BgenStream::from_bytes(meta_bytes.bytes, true),
            MetadataBgi::File(_) => Err(Report::msg(
                "No bytes metadata in bgen constructed from file",
            )),
        }
    }
}

fn write_u16_sized_string(writer: &mut BufWriter<File>, string: String) -> Result<()> {
    let num = string.len() as u16;
    writer.write_all(&num.to_le_bytes())?;
    writer.write_all(&string.into_bytes())?;
    Ok(())
}

fn write_u32_sized_string(writer: &mut BufWriter<File>, string: String) -> Result<()> {
    let num = string.len() as u32;
    writer.write_all(&num.to_le_bytes())?;
    writer.write_all(&string.into_bytes())?;
    Ok(())
}

fn write_u8<T>(writer: &mut BufWriter<T>, num: u8) -> Result<()>
where
    T: std::io::Write,
{
    writer.write_all(&num.to_le_bytes())?;
    Ok(())
}

fn write_u16<T>(writer: &mut BufWriter<T>, num: u16) -> Result<()>
where
    T: std::io::Write,
{
    writer.write_all(&num.to_le_bytes())?;
    Ok(())
}

fn write_u32<T>(writer: &mut BufWriter<T>, num: u32) -> Result<()>
where
    T: std::io::Write,
{
    writer.write_all(&num.to_le_bytes())?;
    Ok(())
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

#[derive(Debug, Default, PartialEq, Eq, Clone)]
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
    fn to_u32(&self) -> u32 {
        ((self.sample_id_present as u32) << 31)
            + (self.compressed_snp_blocks as u32)
            + ((self.layout_id as u32) << 2)
    }
}
