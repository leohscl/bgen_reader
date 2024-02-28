use std::fs::File;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Read;

use color_eyre::Result;

pub struct BgenSteam<T> {
    stream: BufReader<T>,
    pub header_size: u32,
}

impl BgenSteam<File> {
    pub fn from_path(path: &str) -> Result<Self> {
        let file = File::open(path)?;
        let stream = BufReader::new(file);
        Ok(BgenSteam {
            stream,
            header_size: 0,
        })
    }
}

impl BgenSteam<Cursor<Vec<u8>>> {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        let stream = BufReader::new(Cursor::new(bytes));
        Ok(BgenSteam {
            stream,
            header_size: 0,
        })
    }

    pub fn read_header_size(&mut self) -> Result<()> {
        //TODO(lhenches): implement bufreader for BgenSize
        let mut buffer = [0; 4];
        self.stream.read(&mut buffer)?;
        self.header_size = read_u8_buffer(&buffer);
        Ok(())
    }
}

fn read_u8_buffer(buffer: &[u8]) -> u32 {
    assert!(buffer.len() == 4);
    buffer
        .iter()
        .enumerate()
        .map(|(i, b)| (1 << i) * (*b as u32))
        .sum()
}
