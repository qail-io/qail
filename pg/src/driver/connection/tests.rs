//! Connection unit tests.

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::super::helpers::{md5_password_message, select_scram_mechanism};
    use crate::driver::ScramChannelBindingMode;
    #[cfg(unix)]
    use {
        super::super::types::{PgConnection, StatementCache},
        crate::driver::ColumnInfo,
        crate::driver::stream::PgStream,
        bytes::BytesMut,
        std::collections::{HashMap, VecDeque},
        std::num::NonZeroUsize,
        std::sync::Arc,
        tokio::net::UnixStream,
    };

    #[cfg(unix)]
    fn test_conn() -> PgConnection {
        let (unix_stream, _peer) = UnixStream::pair().expect("unix stream pair");
        PgConnection {
            stream: PgStream::Unix(unix_stream),
            buffer: BytesMut::with_capacity(1024),
            write_buf: BytesMut::with_capacity(1024),
            sql_buf: BytesMut::with_capacity(256),
            params_buf: Vec::new(),
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(NonZeroUsize::new(2).expect("non-zero")),
            column_info_cache: HashMap::new(),
            process_id: 0,
            secret_key: 0,
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled: false,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        }
    }

    #[test]
    fn test_md5_password_message_known_vector() {
        let hash = md5_password_message("postgres", "secret", [0x12, 0x34, 0x56, 0x78]);
        assert_eq!(hash, "md521561af64619ca746c2a6c4d6cbedb30");
    }

    #[test]
    fn test_md5_password_message_is_stable() {
        let a = md5_password_message("user_a", "pw", [1, 2, 3, 4]);
        let b = md5_password_message("user_a", "pw", [1, 2, 3, 4]);
        assert_eq!(a, b);
        assert!(a.starts_with("md5"));
        assert_eq!(a.len(), 35);
    }

    #[test]
    fn test_select_scram_plus_when_binding_available() {
        let mechanisms = vec![
            "SCRAM-SHA-256".to_string(),
            "SCRAM-SHA-256-PLUS".to_string(),
        ];
        let binding = vec![1, 2, 3];
        let (mechanism, selected_binding) = select_scram_mechanism(
            &mechanisms,
            Some(binding.clone()),
            ScramChannelBindingMode::Prefer,
        )
        .unwrap();
        assert_eq!(mechanism, "SCRAM-SHA-256-PLUS");
        assert_eq!(selected_binding, Some(binding));
    }

    #[test]
    fn test_select_scram_fallback_without_binding() {
        let mechanisms = vec![
            "SCRAM-SHA-256".to_string(),
            "SCRAM-SHA-256-PLUS".to_string(),
        ];
        let (mechanism, selected_binding) =
            select_scram_mechanism(&mechanisms, None, ScramChannelBindingMode::Prefer).unwrap();
        assert_eq!(mechanism, "SCRAM-SHA-256");
        assert_eq!(selected_binding, None);
    }

    #[test]
    fn test_select_scram_plus_only_requires_binding() {
        let mechanisms = vec!["SCRAM-SHA-256-PLUS".to_string()];
        let err =
            select_scram_mechanism(&mechanisms, None, ScramChannelBindingMode::Prefer).unwrap_err();
        assert!(err.contains("SCRAM-SHA-256-PLUS"));
    }

    #[test]
    fn test_select_scram_require_fails_without_plus() {
        let mechanisms = vec!["SCRAM-SHA-256".to_string()];
        let err = select_scram_mechanism(
            &mechanisms,
            Some(vec![1, 2, 3]),
            ScramChannelBindingMode::Require,
        )
        .unwrap_err();
        assert!(err.contains("channel_binding=require"));
        assert!(err.contains("SCRAM-SHA-256-PLUS"));
    }

    #[test]
    fn test_select_scram_disable_rejects_plus_only() {
        let mechanisms = vec!["SCRAM-SHA-256-PLUS".to_string()];
        let err = select_scram_mechanism(&mechanisms, None, ScramChannelBindingMode::Disable)
            .unwrap_err();
        assert!(err.contains("channel_binding=disable"));
    }

    #[test]
    fn test_select_scram_require_fails_without_tls_binding() {
        let mechanisms = vec![
            "SCRAM-SHA-256".to_string(),
            "SCRAM-SHA-256-PLUS".to_string(),
        ];
        let err = select_scram_mechanism(&mechanisms, None, ScramChannelBindingMode::Require)
            .unwrap_err();
        assert!(err.contains("channel_binding=require"));
        assert!(err.contains("unavailable"));
    }

    #[test]
    fn test_select_scram_require_succeeds_with_plus_and_binding() {
        let mechanisms = vec![
            "SCRAM-SHA-256".to_string(),
            "SCRAM-SHA-256-PLUS".to_string(),
        ];
        let binding = vec![10, 20, 30];
        let (mechanism, selected_binding) = select_scram_mechanism(
            &mechanisms,
            Some(binding.clone()),
            ScramChannelBindingMode::Require,
        )
        .unwrap();
        assert_eq!(mechanism, "SCRAM-SHA-256-PLUS");
        assert_eq!(selected_binding, Some(binding));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_evict_prepared_if_full_queues_server_close_and_clears_column_info() {
        let mut conn = test_conn();
        conn.stmt_cache = StatementCache::new(
            NonZeroUsize::new(PgConnection::MAX_PREPARED_PER_CONN).expect("non-zero"),
        );
        for i in 0..PgConnection::MAX_PREPARED_PER_CONN {
            let name = format!("s{}", i);
            conn.prepared_statements
                .insert(name.clone(), format!("SELECT {}", i));
            conn.stmt_cache.put(i as u64, name);
        }
        conn.column_info_cache.insert(
            0,
            Arc::new(ColumnInfo {
                name_to_index: HashMap::new(),
                oids: Vec::new(),
                formats: Vec::new(),
            }),
        );

        conn.evict_prepared_if_full();

        assert_eq!(
            conn.prepared_statements.len(),
            PgConnection::MAX_PREPARED_PER_CONN - 1
        );
        assert_eq!(conn.pending_statement_closes, vec!["s0".to_string()]);
        assert!(!conn.column_info_cache.contains_key(&0));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_clear_prepared_statement_state_clears_pending_closes() {
        let mut conn = test_conn();
        conn.pending_statement_closes.push("s_dead".to_string());
        conn.prepared_statements
            .insert("s1".to_string(), "SELECT 1".to_string());
        conn.stmt_cache.put(1, "s1".to_string());
        conn.column_info_cache.insert(
            1,
            Arc::new(ColumnInfo {
                name_to_index: HashMap::new(),
                oids: Vec::new(),
                formats: Vec::new(),
            }),
        );

        conn.clear_prepared_statement_state();

        assert!(conn.pending_statement_closes.is_empty());
        assert!(conn.prepared_statements.is_empty());
        assert_eq!(conn.stmt_cache.len(), 0);
        assert!(conn.column_info_cache.is_empty());
    }
}
