//! Root Cause Analysis Module
//! 
//! Contains attribution and narrative building for explaining root causes.

pub mod attribution;
pub mod narrative;
pub mod mode;
pub mod dimension_aggregation;

pub use attribution::{AttributionEngine, RowExplanation, ExplanationItem, DifferenceType};
pub use narrative::{NarrativeBuilder, RowNarrative};
pub use mode::{RCAMode, RCAConfig, LineageLevel, SamplingConfig, RCASamplingStrategy, ModeSelector};
pub use dimension_aggregation::{DimensionAggregator, DimensionAggregationResult, DimensionContribution};

