use crate::adaptor::DbAdaptor;
use crate::errors::{Error, Result};
use crate::migration::{Migration, MigrationBuilder};

impl DbAdaptor for postgres::Client {
    fn init(&mut self) -> Result<()> {
        if self.batch_execute(CHECK_MIGRATIONS_TABLE).is_err() {
            debug!("Table movine_migrations does not exist, creating...");
            self.batch_execute(INIT_UP_SQL)?;
        };
        Ok(())
    }

    fn load_migrations(&mut self) -> Result<Vec<Migration>> {
        let mut migrations = Vec::new();
        let sql = "
            SELECT name, hash, down_sql
            FROM movine_migrations
            ORDER BY created_at DESC;
        ";
        let rows = self.query(sql, &[])?;
        for row in &rows {
            let name: String = row.get(0);
            let hash: String = row.get(1);
            let down_sql: String = row.get(2);
            let migration = MigrationBuilder::new()
                .compound_name(&name)
                .hash(&hash)
                .down_sql(&down_sql)
                .build()?;
            migrations.push(migration);
        }
        Ok(migrations)
    }

    fn run_up_migration(&mut self, migration: &Migration) -> Result<()> {
        let name = &migration.name;
        let hash = migration.hash.as_ref().ok_or(Error::BadMigration)?;
        let up_sql = migration.up_sql.as_ref().ok_or(Error::BadMigration)?;
        let empty_string = "".to_string();
        let down_sql = migration.down_sql.as_ref().unwrap_or(&empty_string);

        let mut transaction = self.transaction()?;
        transaction.batch_execute(up_sql)?;
        transaction.execute(LOG_UP_MIGRATION, &[&name, &hash, &down_sql])?;
        transaction.commit()?;
        Ok(())
    }

    fn run_down_migration(&mut self, migration: &Migration) -> Result<()> {
        let name = &migration.name;
        let down_sql = migration.down_sql.as_ref().ok_or(Error::BadMigration)?;

        let mut transaction = self.transaction()?;
        transaction.batch_execute(down_sql)?;
        transaction.execute(LOG_DOWN_MIGRATION, &[&name])?;
        transaction.commit()?;
        Ok(())
    }
}

pub const LOG_UP_MIGRATION: &str = "\
INSERT INTO movine_migrations (name, hash, down_sql)
VALUES ($1, $2, $3);
";

pub const LOG_DOWN_MIGRATION: &str = "\
DELETE FROM movine_migrations 
WHERE name = $1;
";

pub const CHECK_MIGRATIONS_TABLE: &str = "\
SELECT * FROM movine_migrations WHERE false;
";

pub const INIT_UP_SQL: &str = "\
CREATE TABLE IF NOT EXISTS movine_migrations (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    name TEXT NOT NULL,
    hash TEXT NOT NULL,
    down_sql TEXT
);
";
