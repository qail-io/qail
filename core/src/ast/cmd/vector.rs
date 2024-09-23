//! Vector database builder methods for Qdrant.

use crate::ast::{Action, Qail};

impl Qail {
    /// Create a vector similarity search command.
    ///
    /// # Example
    /// ```ignore
    /// use qail_core::prelude::*;
    ///
    /// let cmd = Qail::search("products")
    ///     .vector(vec![0.1, 0.2, 0.3])
    ///     .limit(10);
    /// ```
    pub fn search(collection: &str) -> Self {
        Self {
            action: Action::Search,
            table: collection.to_string(),
            ..Default::default()
        }
    }

    /// Create a vector upsert command (insert or update points).
    ///
    /// # Example
    /// ```ignore
    /// let cmd = Qail::upsert("products");
    /// ```
    pub fn upsert(collection: &str) -> Self {
        Self {
            action: Action::Upsert,
            table: collection.to_string(),
            ..Default::default()
        }
    }

    /// Create a scroll command for paginated iteration.
    ///
    /// # Example
    /// ```ignore
    /// let cmd = Qail::scroll("products").limit(100);
    /// ```
    pub fn scroll(collection: &str) -> Self {
        Self {
            action: Action::Scroll,
            table: collection.to_string(),
            ..Default::default()
        }
    }

    /// Set the query vector for similarity search.
    ///
    /// # Example
    /// ```
    /// use qail_core::prelude::*;
    ///
    /// let embedding = vec![0.1, 0.2, 0.3, 0.4];
    /// let cmd = Qail::search("products").vector(embedding);
    /// assert!(cmd.vector.is_some());
    /// ```
    pub fn vector(mut self, embedding: Vec<f32>) -> Self {
        self.vector = Some(embedding);
        self
    }

    /// Set minimum similarity score threshold.
    ///
    /// Points with similarity below this threshold will be filtered out.
    ///
    /// # Example
    /// ```
    /// use qail_core::prelude::*;
    ///
    /// let cmd = Qail::search("products")
    ///     .vector(vec![0.1, 0.2])
    ///     .score_threshold(0.8);
    /// assert_eq!(cmd.score_threshold, Some(0.8));
    /// ```
    pub fn score_threshold(mut self, threshold: f32) -> Self {
        self.score_threshold = Some(threshold);
        self
    }

    /// Specify which named vector to search (for multi-vector collections).
    ///
    /// # Example
    /// ```
    /// use qail_core::prelude::*;
    ///
    /// // Collection with separate "title" and "content" vectors
    /// let title_embedding = vec![0.1, 0.2, 0.3];
    /// let cmd = Qail::search("articles")
    ///     .vector_name("title")
    ///     .vector(title_embedding);
    /// ```
    pub fn vector_name(mut self, name: &str) -> Self {
        self.vector_name = Some(name.to_string());
        self
    }

    /// Include vectors in search results.
    ///
    /// # Example
    /// ```
    /// use qail_core::prelude::*;
    ///
    /// let embedding = vec![0.1, 0.2, 0.3];
    /// let cmd = Qail::search("products")
    ///     .vector(embedding)
    ///     .with_vectors();
    /// assert!(cmd.with_vector);
    /// ```
    pub fn with_vectors(mut self) -> Self {
        self.with_vector = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_builder() {
        let cmd = Qail::search("products")
            .vector(vec![0.1, 0.2, 0.3])
            .score_threshold(0.8)
            .limit(10);
        
        assert_eq!(cmd.action, Action::Search);
        assert_eq!(cmd.table, "products");
        assert_eq!(cmd.vector, Some(vec![0.1, 0.2, 0.3]));
        assert_eq!(cmd.score_threshold, Some(0.8));
    }

    #[test]
    fn test_vector_name() {
        let cmd = Qail::search("articles")
            .vector_name("title")
            .vector(vec![0.5, 0.5]);
        
        assert_eq!(cmd.vector_name, Some("title".to_string()));
    }

    #[test]
    fn test_with_vectors() {
        let cmd = Qail::search("products")
            .vector(vec![0.1])
            .with_vectors();
        
        assert!(cmd.with_vector);
    }
}
