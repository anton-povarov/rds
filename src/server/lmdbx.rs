use std::{path::Path, sync::Arc};

type Writemap = libmdbx::NoWriteMap;

pub type DBType = libmdbx::Database<Writemap>;
pub type TableT<'a> = libmdbx::Table<'a>;
pub type TransactionRW<'a> = libmdbx::Transaction<'a, libmdbx::RW, Writemap>;
pub type TransactionRO<'a> = libmdbx::Transaction<'a, libmdbx::RO, Writemap>;

pub struct LmdbxStorage {
    pub db: DBType,
}

impl LmdbxStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Arc<LmdbxStorage>, libmdbx::Error> {
        let mut options = libmdbx::DatabaseOptions::default();
        options.exclusive = true;
        options.mode = {
            let mut opt = libmdbx::ReadWriteOptions::default();
            opt.sync_mode = libmdbx::SyncMode::Durable;
            // opt.sync_mode = libmdbx::SyncMode::NoMetaSync;
            // opt.sync_mode = libmdbx::SyncMode::SafeNoSync;
            // opt.sync_mode = libmdbx::SyncMode::UtterlyNoSync;
            libmdbx::Mode::ReadWrite(opt)
        };

        let lmdbx = libmdbx::Database::open_with_options(path, options)?;
        let db = Arc::new(LmdbxStorage { db: lmdbx });
        Ok(db)
    }
}
