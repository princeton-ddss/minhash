use std::error::Error;

use itertools::izip;
use rand::rngs::StdRng;
use rand::SeedableRng;

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

pub mod minihasher;
pub mod shingleset;

use crate::minihasher::MinHasher;
use crate::shingleset::ShingleSet;

struct MinHash {}

impl VScalar for MinHash {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let input_strings = input.flat_vector(0);
        let input_ngram_widths = input.flat_vector(1);
        let input_band_counts = input.flat_vector(2);
        let input_band_sizes = input.flat_vector(3);
        let input_seeds = input.flat_vector(4);

        let strings = input_strings
            .as_slice_with_len::<duckdb_string_t>(input.len())
            .iter()
            .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string());
        let ngram_widths = input_ngram_widths.as_slice_with_len::<usize>(input.len());
        let band_counts = input_band_counts.as_slice_with_len::<usize>(input.len());
        let band_sizes = input_band_sizes.as_slice_with_len::<usize>(input.len());
        let seeds = input_seeds.as_slice_with_len::<u64>(input.len());

        let mut output_hashes = output.list_vector();
        let total_len: usize = band_counts.iter().sum();
        let mut hashes_vec = output_hashes.child(total_len);
        let hashes: &mut [u64] = hashes_vec.as_mut_slice_with_len(total_len);

        let mut offset = 0;
        for (row_idx, (string, ngram_width, band_count, band_size, seed)) in
            izip!(strings, ngram_widths, band_counts, band_sizes, seeds)
                .enumerate()
                .take(input.len())
        {
            let shingle_set = ShingleSet::new(&string, *ngram_width, row_idx, None);
            let mut rng = StdRng::seed_from_u64(*seed);
            for band_idx in 0..*band_count {
                let hasher = MinHasher::new(*band_size, &mut rng);
                hashes[offset + band_idx] = hasher.hash(&shingle_set);
            }
            output_hashes.set_entry(row_idx, offset, *band_count);
            offset += band_count;
        }
        output_hashes.set_len(input.len());

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
            ],
            LogicalTypeHandle::list(&LogicalTypeId::UBigint.into()),
        )]
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<MinHash>("minhash")
        .expect("Failed to register minhash function");
    Ok(())
}
