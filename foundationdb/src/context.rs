use error::{self, Result};
use foundationdb_sys as fdb;

// The Fdb states that setting the Client version should happen only once
//   and is not thread-safe, thus the choice of a lazy static enforcing a single
//   init.
lazy_static! {
    // TODO: how should we allow multi-versions
    static ref CONTEXT: Context = Context::new().expect("error initializing FoundationDB");
}

pub struct Context {}

impl Context {
    /// Get the singleton context, initializes FoundationDB version.
    pub fn get() -> &'static Context {
        &CONTEXT
    }

    fn new() -> Result<Self> {
        Self::init(fdb::FDB_API_VERSION as i32, fdb::FDB_API_VERSION as i32)
    }

    fn init(runtime_version: i32, header_version: i32) -> Result<Self> {
        unsafe {
            error::eval(fdb::fdb_select_api_version_impl(
                runtime_version,
                header_version,
            ))?;
        }

        Ok(Context {})
    }

    pub fn get_max_api_version() -> i32 {
        unsafe { fdb::fdb_get_max_api_version() as i32 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_context() {
        // checks that the initialization occured
        Context::get();
    }
}
