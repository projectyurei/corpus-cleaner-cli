use anyhow::Result;
use dashmap::DashMap;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub trait Filter: Send + Sync {
    /// Returns true if the transaction should be KEPT.
    fn keep(&self, tx: &Value) -> bool;
}

pub struct Pipeline {
    filters: Vec<Box<dyn Filter>>,
    deduplicator: Option<Deduplicator>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
            deduplicator: None,
        }
    }

    pub fn add_filter<F: Filter + 'static>(mut self, filter: F) -> Self {
        self.filters.push(Box::new(filter));
        self
    }

    pub fn with_deduplication(mut self, dedup: Deduplicator) -> Self {
        self.deduplicator = Some(dedup);
        self
    }

    pub fn process(&self, tx: &Value) -> bool {
        // 1. Run stateless filters
        for filter in &self.filters {
            if !filter.keep(tx) {
                return false;
            }
        }

        // 2. Run stateful deduplication
        if let Some(dedup) = &self.deduplicator {
            if !dedup.is_unique(tx) {
                return false;
            }
        }

        true
    }
}

// --- Concrete Filters ---

/// Aggressively removes failed transactions.
pub struct StatusFilter;

impl Filter for StatusFilter {
    fn keep(&self, tx: &Value) -> bool {
        // Assumption: Solana JSON logs usually have "meta" -> "err" which is null on success.
        // Adjust field path based on specific Geyser schema.
        match tx.pointer("/meta/err") {
            Some(Value::Null) => true,
            Some(_) => false, // Error present
            None => true,     // No error field, assume valid or filter elsewhere
        }
    }
}

/// Removes spam/dust transfers below a threshold.
pub struct SpamFilter {
    pub min_lamports: u64,
}

impl Filter for SpamFilter {
    fn keep(&self, tx: &Value) -> bool {
        // Heuristic: Check "meta" -> "preBalances" vs "postBalances" diff or instruction data.
        // For simplicity in this scaffold, we'll check if any instruction involves >= min_lamports.
        // NOTE: This is a placeholder for the actual complex spam logic.
        // Real implementation would parse instruction data or balance changes.
        true // strict implementation requires specific schema knowledge
    }
}

/// Checks for malformed UTF-8 in memo fields.
pub struct Utf8Filter;

impl Filter for Utf8Filter {
    fn keep(&self, tx: &Value) -> bool {
        // Recursively walk the JSON to find strings and validate? 
        // serde_json::Value strings are already valid UTF-8 by definition.
        // This filter is likely creating a check for specific byte fields that are encoded as strings but might contain garbage.
        // For now, we assume if it parsed as JSON, strict UTF-8 is already enforced by serde.
        // We'll implement a logic check for "memo" program logic if needed.
        true
    }
}

// --- Deduplication ---

#[derive(Clone)]
pub struct Deduplicator {
    // Maps signature hash to unit.
    // Using DashMap for high-concurrency access.
    seen: Arc<DashMap<[u8; 32], ()>>,
}

impl Deduplicator {
    pub fn new() -> Self {
        Self {
            seen: Arc::new(DashMap::new()),
        }
    }

    pub fn is_unique(&self, tx: &Value) -> bool {
        // Extract signature.
        let signature = match tx.get("signature") {
            Some(Value::String(s)) => s,
            _ => return false, // No signature, treat as distinct or invalid? Let's say invalid/skip.
        };

        let mut hasher = Sha256::new();
        hasher.update(signature.as_bytes());
        let hash: [u8; 32] = hasher.finalize().into();

        if self.seen.contains_key(&hash) {
            return false;
        }

        self.seen.insert(hash, ());
        true
    }
}
