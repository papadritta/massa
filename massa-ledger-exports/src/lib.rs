//! # General description
//!
//! TODO

#![feature(let_chains)]

mod config;
mod controller;
mod error;
mod key;
mod ledger_changes;
mod ledger_entry;
mod types;

pub use config::LedgerConfig;
pub use controller::LedgerController;
pub use error::LedgerError;
pub use key::{
    get_address_from_key, KeyDeserializer, KeySerializer, BYTECODE_IDENT, DATASTORE_IDENT,
    PAR_BALANCE_IDENT, SEQ_BALANCE_IDENT,
};
pub use ledger_changes::{
    DatastoreUpdateDeserializer, DatastoreUpdateSerializer, LedgerChanges,
    LedgerChangesDeserializer, LedgerChangesSerializer, LedgerEntryUpdate,
    LedgerEntryUpdateDeserializer, LedgerEntryUpdateSerializer,
};
pub use ledger_entry::{
    DatastoreDeserializer, DatastoreSerializer, LedgerEntry, LedgerEntryDeserializer,
    LedgerEntrySerializer,
};
pub use types::{Applicable, SetOrDelete, SetOrKeep, SetUpdateOrDelete};

#[cfg(feature = "testing")]
pub mod test_exports;
