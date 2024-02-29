use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Read;

use color_eyre::Report;
use color_eyre::Result;

pub struct BgenSteam<T> {
    stream: BufReader<T>,
    pub start_data_offset: u32,
    pub header_size: u32,
    pub variant_num: u32,
    pub sample_num: u32,
}

macro_rules! read_into_buffer {
    ($buffer:ident, $self:ident, $bytes:literal) => {
        let mut $buffer = [0; $bytes];
        $self.read(&mut $buffer)?;
    };
}

impl<T: Read> BgenSteam<T> {
    pub fn new(stream: BufReader<T>) -> Self {
        BgenSteam {
            stream,
            start_data_offset: 0,
            header_size: 0,
            variant_num: 0,
            sample_num: 0,
        }
    }
    pub fn read_offset_and_header(&mut self) -> Result<()> {
        self.start_data_offset = self.read_u32()?;
        self.header_size = self.read_u32()?;
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

        Ok(())
    }
    fn read_u32(&mut self) -> Result<u32> {
        read_into_buffer!(buffer, self, 4);
        Ok(buffer
            .iter()
            .enumerate()
            .map(|(i, b)| (1 << i) * (*b as u32))
            .sum())
    }
}

impl BgenSteam<File> {
    pub fn from_path(path: &str) -> Result<Self> {
        let file = File::open(path)?;
        let stream = BufReader::new(file);
        Ok(BgenSteam::new(stream))
    }
}

impl BgenSteam<Cursor<Vec<u8>>> {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        let stream = BufReader::new(Cursor::new(bytes));
        Ok(BgenSteam::new(stream))
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
