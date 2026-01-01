//! Database models for Garmin data
//!
//! This module contains data models used throughout the application.
//! The models were originally designed for DuckDB but are now used with
//! Parquet storage for concurrent read access.

pub mod models;

pub use models::*;
