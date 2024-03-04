use crate::bgen::VariantData;
use rusqlite::{Connection, Result, ToSql};

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
        let table_creator = TableCreator {
            conn: Connection::open(&filename)?,
        };
        Ok(table_creator)
    }

    pub fn init(&self) -> Result<()> {
        self.conn.execute(VARIANT_CREATION_STRING, ())?;
        self.conn.execute(METADATA_CREATION_STRING, ())?;
        Ok(())
    }

    pub fn store(&self, data: &Vec<VariantData>) -> Result<()> {
        data.chunks(10000)
            .map(|chunk| {
                let statement = create_statement_batch_params(chunk.len());
                let mut cached_statement = self.conn.prepare_cached(&statement)?;
                let mut params = Vec::new();
                chunk.iter().for_each(|var_data| {
                    params.push(&var_data.chr as &dyn ToSql);
                    params.push(&var_data.pos as &dyn ToSql);
                    params.push(&var_data.rsid as &dyn ToSql);
                    params.push(&var_data.number_alleles as &dyn ToSql);
                    params.push(&var_data.alleles[0] as &dyn ToSql);
                    params.push(&var_data.alleles[1] as &dyn ToSql);
                    params.push(&var_data.file_start_position as &dyn ToSql);
                    params.push(&var_data.size_in_bytes as &dyn ToSql);
                });
                cached_statement.execute(&*params)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

fn create_statement_batch_params(length: usize) -> String {
    let mut params = "(?, ?, ?, ?, ?, ?, ?, ?),".repeat(length);
    params.pop();
    format!("INSERT INTO Variant Values {}", params.as_str())
}
