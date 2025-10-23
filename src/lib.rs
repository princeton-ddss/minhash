use std::error::Error;

use duckdb::core::Inserter;
use duckdb::ffi;
use duckdb::ffi::duckdb_string_t;
use duckdb::types::DuckString;
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;

struct Repeat {}

impl VScalar for Repeat {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let output = output.flat_vector();
        let counts = input.flat_vector(1);
        let values = input.flat_vector(0);
        let values = values.as_slice_with_len::<duckdb_string_t>(input.len());
        let strings = values
            .iter()
            .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string());
        let counts = counts.as_slice_with_len::<i32>(input.len());
        for (i, (count, value)) in counts.iter().zip(strings).enumerate().take(input.len()) {
            output.insert(i, value.repeat((*count) as usize).as_str());
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Integer),
            ],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<Repeat>("nobie_repeat")
        .expect("Failed to register nobie_repeat scalar function");
    Ok(())
}
