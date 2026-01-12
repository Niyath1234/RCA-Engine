//! Vector Store - Embedding-based similarity search

use super::concepts::BusinessConcept;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Result from vector similarity search
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConceptSearchResult {
    /// Concept that matched
    pub concept: BusinessConcept,
    
    /// Similarity score (0.0-1.0)
    pub similarity: f32,
    
    /// Match reason
    pub match_reason: String,
}

/// Vector Store - Simple in-memory vector database
/// 
/// In production, this would use a proper vector DB like Pinecone, Weaviate, or Qdrant.
/// For now, we use cosine similarity on embeddings.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorStore {
    /// Concept ID → Embedding
    embeddings: HashMap<String, Vec<f32>>,
    
    /// Concept ID → Concept (for retrieving full data)
    concepts: HashMap<String, BusinessConcept>,
}

impl VectorStore {
    pub fn new() -> Self {
        Self {
            embeddings: HashMap::new(),
            concepts: HashMap::new(),
        }
    }
    
    /// Add or update a concept with its embedding
    pub fn add_concept(&mut self, concept: BusinessConcept, embedding: Vec<f32>) {
        let concept_id = concept.concept_id.clone();
        self.embeddings.insert(concept_id.clone(), embedding);
        self.concepts.insert(concept_id, concept);
    }
    
    /// Remove a concept
    pub fn remove_concept(&mut self, concept_id: &str) {
        self.embeddings.remove(concept_id);
        self.concepts.remove(concept_id);
    }
    
    /// Get embedding for a concept
    pub fn get_embedding(&self, concept_id: &str) -> Option<&Vec<f32>> {
        self.embeddings.get(concept_id)
    }
    
    /// Search for similar concepts using cosine similarity
    pub fn search(&self, query_embedding: &[f32], top_k: usize) -> Vec<ConceptSearchResult> {
        let mut results: Vec<ConceptSearchResult> = self.concepts
            .values()
            .filter_map(|concept| {
                let embedding = self.embeddings.get(&concept.concept_id)?;
                
                // Compute cosine similarity
                let similarity = cosine_similarity(query_embedding, embedding);
                
                Some(ConceptSearchResult {
                    concept: concept.clone(),
                    similarity,
                    match_reason: format!("Vector similarity: {:.2}%", similarity * 100.0),
                })
            })
            .collect();
        
        // Sort by similarity (descending)
        results.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
        
        // Return top K
        results.into_iter().take(top_k).collect()
    }
    
    /// Search by text (requires generating embedding first via LLM)
    /// This is a placeholder - in production, would call embedding API
    pub fn search_by_text(&self, query_text: &str, top_k: usize) -> Vec<ConceptSearchResult> {
        // For now, do simple text matching
        // In production, would:
        // 1. Generate embedding for query_text using embedding model
        // 2. Call search() with the embedding
        
        let query_lower = query_text.to_lowercase();
        let mut results: Vec<ConceptSearchResult> = self.concepts
            .values()
            .filter_map(|concept| {
                // Simple text matching for now
                let text = format!("{} {}", concept.name, concept.definition).to_lowercase();
                
                if text.contains(&query_lower) {
                    let similarity = if concept.name.to_lowercase().contains(&query_lower) {
                        0.9
                    } else {
                        0.7
                    };
                    
                    Some(ConceptSearchResult {
                        concept: concept.clone(),
                        similarity,
                        match_reason: format!("Text match: '{}'", query_text),
                    })
                } else {
                    None
                }
            })
            .collect();
        
        results.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
        results.into_iter().take(top_k).collect()
    }
    
    /// RAG: Retrieve relevant concepts for a query with context
    /// Returns concepts that are semantically similar to the query
    pub fn rag_retrieve(&self, query_text: &str, top_k: usize) -> Vec<ConceptSearchResult> {
        // Use text search for now (embedding-based search would be better)
        self.search_by_text(query_text, top_k)
    }
    
    /// Get concepts for RAG context (formatted for LLM prompts)
    pub fn get_rag_context(&self, query_text: &str, top_k: usize) -> String {
        let results = self.rag_retrieve(query_text, top_k);
        
        if results.is_empty() {
            return String::new();
        }
        
        let context_parts: Vec<String> = results.iter()
            .map(|result| {
                format!(
                    "- {} ({:?}): {}\n  Related tables: {}\n  Tags: {}",
                    result.concept.name,
                    result.concept.concept_type,
                    result.concept.definition,
                    result.concept.related_tables.join(", "),
                    result.concept.tags.join(", ")
                )
            })
            .collect();
        
        format!("Relevant Business Concepts:\n{}", context_parts.join("\n\n"))
    }
}

/// Compute cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
    
    let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    
    dot_product / (norm_a * norm_b)
}

impl Default for VectorStore {
    fn default() -> Self {
        Self::new()
    }
}

