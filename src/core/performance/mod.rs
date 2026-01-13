//! Performance Optimization Module
//! 
//! Provides performance optimizations for large-scale RCA:
//! - Chunked extraction for memory efficiency
//! - Sampling for quick analysis
//! - Hash-based diff for fast comparison
//! - Pushdown predicates for efficient data loading

pub mod chunked_extraction;
pub mod sampling;
pub mod hash_diff;
pub mod pushdown;

pub use chunked_extraction::{ChunkedExtractor, ChunkConfig};
pub use sampling::{Sampler, SamplingStrategy};
pub use hash_diff::{HashDiffEngine, HashDiffResult};
pub use pushdown::{PushdownPredicate, PushdownOptimizer};

