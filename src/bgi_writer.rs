use crate::bgen::MetadataBgi;
use crate::variant_data::VariantData;
use color_eyre::Result;
use itertools::Itertools;
use sqlite::Connection;
use sqlite::Value;

static VARIANT_CREATION_STRING: &str = r#"CREATE TABLE Variant (
  chromosome TEXT NOT NULL,
  position INT NOT NULL,
  rsid TEXT NOT NULL,
  number_of_alleles INT NOT NULL,
  allele1 TEXT NOT NULL,
  allele2 TEXT NULL,
  file_start_position INT NOT NULL,
  size_in_bytes INT NOT NULL,
  PRIMARY KEY (chromosome, position, rsid, allele1, allele2, file_start_position )
) WITHOUT ROWID;"#;

static METADATA_CREATION_STRING: &str = r#"CREATE TABLE Metadata (
  filename TEXT NOT NULL,
  file_size INT NOT NULL,
  last_write_time INT NOT NULL,
  first_1000_bytes BLOB NOT NULL,
  index_creation_time INT NOT NULL
);"#;

pub struct TableCreator {
    conn: Connection,
}

impl TableCreator {
    pub fn new(filename: String) -> Result<Self> {
        let conn = sqlite::open(filename)?;
        let table_creator = TableCreator { conn };
        Ok(table_creator)
    }

    pub fn init(&self, meta: &MetadataBgi) -> Result<()> {
        // self.conn.execute("PRAGMA journal_mode = OFF", ())?;
        self.conn
            .execute(
                "PRAGMA journal_mode = OFF;
                 PRAGMA synchronous = 0;
                 PRAGMA cache_size = 1000000;
                 PRAGMA locking_mode = EXCLUSIVE;
                 PRAGMA temp_store = MEMORY;",
            )
            .expect("PRAGMA");
        self.conn.execute(VARIANT_CREATION_STRING)?;
        self.conn.execute(METADATA_CREATION_STRING)?;
        let query = "INSERT INTO Metadata (filename, file_size, last_write_time, first_1000_bytes, index_creation_time) VALUES (?1, ?2, ?3, ?4, ?5)";
        let mut statement = self.conn.prepare(query)?;
        statement.bind(
            &[
                Value::String(meta.filename.clone()),
                Value::Integer(meta.file_size as i64),
                Value::Integer(meta.last_write_time.elapsed().unwrap().as_secs() as i64),
                Value::Binary(meta.first_1000_bytes.clone()),
                Value::Integer(meta.index_creation_time.elapsed().unwrap().as_secs() as i64),
            ][..],
        )?;
        Ok(())
    }

    pub fn store(&self, data: impl Iterator<Item = Result<VariantData>>) -> Result<()> {
        let size = 10000;
        data.chunks(size)
            .into_iter()
            .map(|chunk| {
                let query = "INSERT INTO Variant Values (?, ?, ?, ?, ?, ?, ?, ?)";
                let mut statement = self.conn.prepare(query)?;
                chunk
                    .into_iter()
                    .map(|res_var_data| {
                        let var_data = res_var_data?;
                        statement.bind(
                            &[
                                Some(Value::String(var_data.chr)),
                                Some(Value::Integer(var_data.pos as i64)),
                                Some(Value::String(var_data.rsid)),
                                Some(Value::Integer(var_data.number_alleles as i64)),
                                Some(Value::String(var_data.alleles[0].clone())),
                                Some(Value::String(var_data.alleles[1].clone())),
                                Some(Value::Integer(var_data.file_start_position as i64)),
                                Some(Value::Integer(var_data.size_in_bytes as i64)),
                            ][..],
                        )?;
                        Ok(())
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(())
    }

    // pub fn store(&self, data: impl Iterator<Item = Result<VariantData>>) -> Result<()> {
    //     let size = 10000;
    //     data.chunks(size)
    //         .into_iter()
    //         .map(|chunk| {
    //             let chunk_vec: Vec<VariantData> =
    //                 chunk.into_iter().collect::<Result<Vec<VariantData>>>()?;
    //             let statement = create_statement_batch_params(size);
    //             let mut cached_statement = self
    //                 .conn
    //                 .prepare_cached(&statement)
    //                 .map_err(|e| Report::msg(e.to_string()))?;
    //             let mut params = Vec::new();
    //             chunk_vec.iter().for_each(|var_data| {
    //                 params.push(&var_data.chr as &dyn ToSql);
    //                 params.push(&var_data.pos as &dyn ToSql);
    //                 params.push(&var_data.rsid as &dyn ToSql);
    //                 params.push(&var_data.number_alleles as &dyn ToSql);
    //                 params.push(&var_data.alleles[0] as &dyn ToSql);
    //                 params.push(&var_data.alleles[1] as &dyn ToSql);
    //                 params.push(&var_data.file_start_position as &dyn ToSql);
    //                 params.push(&var_data.size_in_bytes as &dyn ToSql);
    //             });
    //             cached_statement
    //                 .execute(params_from_iter(params.iter()))
    //                 .map_err(|e| Report::msg(e.to_string()))
    //         })
    //         .collect::<Result<Vec<_>>>()?;
    //     Ok(())
    // }
}

// fn create_statement_batch_params(length: usize) -> String {
//     let mut params = "(?, ?, ?, ?, ?, ?, ?, ?),".repeat(length);
//     params.pop();
//     format!("INSERT INTO Variant Values {}", params.as_str())
// }
