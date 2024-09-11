use color_eyre::Report;
use color_eyre::Result;
use flate2::bufread::{ZlibDecoder, ZlibEncoder};
use flate2::Compression;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Cursor, Read, Write};
use std::path::Path;

pub fn write_u16_sized_string(writer: &mut BufWriter<File>, string: String) -> Result<()> {
    let num = string.len() as u16;
    writer.write_all(&num.to_le_bytes())?;
    writer.write_all(&string.into_bytes())?;
    Ok(())
}

pub fn write_u32_sized_string(writer: &mut BufWriter<File>, string: String) -> Result<()> {
    let num = string.len() as u32;
    writer.write_all(&num.to_le_bytes())?;
    writer.write_all(&string.into_bytes())?;
    Ok(())
}

pub fn write_u8<T>(writer: &mut BufWriter<T>, num: u8) -> Result<()>
where
    T: std::io::Write,
{
    writer.write_all(&num.to_le_bytes())?;
    Ok(())
}

pub fn write_u16<T>(writer: &mut BufWriter<T>, num: u16) -> Result<()>
where
    T: std::io::Write,
{
    writer.write_all(&num.to_le_bytes())?;
    Ok(())
}

pub fn write_u32<T>(writer: &mut BufWriter<T>, num: u32) -> Result<()>
where
    T: std::io::Write,
{
    writer.write_all(&num.to_le_bytes())?;
    Ok(())
}

pub fn read_lines<P>(filename: P) -> Result<Vec<String>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    let result = BufReader::new(file)
        .lines()
        .collect::<io::Result<Vec<_>>>()?;
    Ok(result)
}

pub fn compress_data(data: &Vec<u8>) -> Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Cursor::new(data), Compression::fast());
    let mut block = Vec::new();
    encoder.read_to_end(&mut block)?;
    Ok(block)
}

pub fn decompress_block(block: Vec<u8>, length: usize) -> Result<Vec<u8>> {
    let mut decoder = ZlibDecoder::new(Cursor::new(block));
    let mut decoded = vec![0; length];
    decoder
        .read_exact(&mut decoded)
        .map_err(|_| Report::msg("Error in decompression"))?;
    Ok(decoded)
}
