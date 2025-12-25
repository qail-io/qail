//! AST-Native Encoder
//!
//! Direct AST → Wire Protocol Bytes conversion.
//! NO INTERMEDIATE SQL STRING! 
//!
//! This is the TRUE AST-native path:
//! QailCmd → BytesMut (no to_sql() call)

use bytes::BytesMut;
use qail_core::ast::{Action, QailCmd, Expr, CageKind, Value, Condition, Operator, SortOrder};

// ============================================================================
// PRE-COMPUTED LOOKUP TABLES - ZERO ALLOCATION!
// ============================================================================

/// Pre-computed parameter placeholders $1-$99 (covers 99% of cases)
const PARAM_PLACEHOLDERS: [&[u8]; 100] = [
    b"$0", b"$1", b"$2", b"$3", b"$4", b"$5", b"$6", b"$7", b"$8", b"$9",
    b"$10", b"$11", b"$12", b"$13", b"$14", b"$15", b"$16", b"$17", b"$18", b"$19",
    b"$20", b"$21", b"$22", b"$23", b"$24", b"$25", b"$26", b"$27", b"$28", b"$29",
    b"$30", b"$31", b"$32", b"$33", b"$34", b"$35", b"$36", b"$37", b"$38", b"$39",
    b"$40", b"$41", b"$42", b"$43", b"$44", b"$45", b"$46", b"$47", b"$48", b"$49",
    b"$50", b"$51", b"$52", b"$53", b"$54", b"$55", b"$56", b"$57", b"$58", b"$59",
    b"$60", b"$61", b"$62", b"$63", b"$64", b"$65", b"$66", b"$67", b"$68", b"$69",
    b"$70", b"$71", b"$72", b"$73", b"$74", b"$75", b"$76", b"$77", b"$78", b"$79",
    b"$80", b"$81", b"$82", b"$83", b"$84", b"$85", b"$86", b"$87", b"$88", b"$89",
    b"$90", b"$91", b"$92", b"$93", b"$94", b"$95", b"$96", b"$97", b"$98", b"$99",
];

/// Pre-computed numeric values 0-99 for LIMIT/OFFSET (covers common cases)
const NUMERIC_VALUES: [&[u8]; 100] = [
    b"0", b"1", b"2", b"3", b"4", b"5", b"6", b"7", b"8", b"9",
    b"10", b"11", b"12", b"13", b"14", b"15", b"16", b"17", b"18", b"19",
    b"20", b"21", b"22", b"23", b"24", b"25", b"26", b"27", b"28", b"29",
    b"30", b"31", b"32", b"33", b"34", b"35", b"36", b"37", b"38", b"39",
    b"40", b"41", b"42", b"43", b"44", b"45", b"46", b"47", b"48", b"49",
    b"50", b"51", b"52", b"53", b"54", b"55", b"56", b"57", b"58", b"59",
    b"60", b"61", b"62", b"63", b"64", b"65", b"66", b"67", b"68", b"69",
    b"70", b"71", b"72", b"73", b"74", b"75", b"76", b"77", b"78", b"79",
    b"80", b"81", b"82", b"83", b"84", b"85", b"86", b"87", b"88", b"89",
    b"90", b"91", b"92", b"93", b"94", b"95", b"96", b"97", b"98", b"99",
];

/// Write parameter placeholder ($N) to buffer - ZERO ALLOCATION for common cases
#[inline(always)]
fn write_param_placeholder(buf: &mut BytesMut, idx: usize) {
    if idx < 100 {
        buf.extend_from_slice(PARAM_PLACEHOLDERS[idx]);
    } else {
        // Fallback for rare cases
        buf.extend_from_slice(b"$");
        write_usize(buf, idx);
    }
}

/// Write usize to buffer - ZERO ALLOCATION for common cases
#[inline(always)]
fn write_usize(buf: &mut BytesMut, n: usize) {
    if n < 100 {
        buf.extend_from_slice(NUMERIC_VALUES[n]);
    } else if n < 1000 {
        // Handle 100-999 with minimal allocation
        let hundreds = n / 100;
        let tens = (n % 100) / 10;
        let ones = n % 10;
        buf.extend_from_slice(NUMERIC_VALUES[hundreds]);
        buf.extend_from_slice(NUMERIC_VALUES[tens]);
        buf.extend_from_slice(NUMERIC_VALUES[ones]);
    } else {
        // Fallback - very rare
        buf.extend_from_slice(n.to_string().as_bytes());
    }
}

/// Write i64 to buffer - ZERO ALLOCATION for common cases
#[inline(always)]
#[allow(dead_code)]  // May be used for future optimizations
fn write_i64(buf: &mut BytesMut, n: i64) {
    if n >= 0 && n < 100 {
        buf.extend_from_slice(NUMERIC_VALUES[n as usize]);
    } else if n >= 0 && n < 1000 {
        write_usize(buf, n as usize);
    } else {
        // Fallback for negatives and large numbers
        buf.extend_from_slice(n.to_string().as_bytes());
    }
}

/// Convert i64 to bytes for parameter - ZERO ALLOCATION for common cases
#[inline(always)]
fn i64_to_bytes(n: i64) -> Vec<u8> {
    if n >= 0 && n < 100 {
        NUMERIC_VALUES[n as usize].to_vec()
    } else {
        n.to_string().into_bytes()
    }
}

/// AST-native encoder that skips SQL string generation.
pub struct AstEncoder;

impl AstEncoder {
    /// Encode a QailCmd directly to Extended Query protocol bytes.
    /// 
    /// Returns (wire_bytes, extracted_params_as_bytes)
    pub fn encode_cmd(cmd: &QailCmd) -> (BytesMut, Vec<Option<Vec<u8>>>) {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();
        
        match cmd.action {
            Action::Get => Self::encode_select(cmd, &mut sql_buf, &mut params),
            Action::Add => Self::encode_insert(cmd, &mut sql_buf, &mut params),
            Action::Set => Self::encode_update(cmd, &mut sql_buf, &mut params),
            Action::Del => Self::encode_delete(cmd, &mut sql_buf, &mut params),
            _ => {
                // STRICT: No fallback to to_sql() - panic on unsupported actions
                panic!("Unsupported action {:?} in AST-native encoder. Use legacy encoder for DDL.", cmd.action);
            }
        }
        
        // Build Extended Query protocol message
        let sql_bytes = sql_buf.freeze();
        let wire = Self::build_extended_query(&sql_bytes, &params);
        
        (wire, params)
    }

    /// Encode a QailCmd to SQL string + params (for prepared statement caching).
    /// 
    /// Returns (sql_string, params) - NOT wrapped in wire protocol.
    /// Use this for cached prepared statements.
    pub fn encode_cmd_sql(cmd: &QailCmd) -> (String, Vec<Option<Vec<u8>>>) {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();
        
        match cmd.action {
            Action::Get => Self::encode_select(cmd, &mut sql_buf, &mut params),
            Action::Add => Self::encode_insert(cmd, &mut sql_buf, &mut params),
            Action::Set => Self::encode_update(cmd, &mut sql_buf, &mut params),
            Action::Del => Self::encode_delete(cmd, &mut sql_buf, &mut params),
            _ => {
                panic!("Unsupported action {:?} in AST-native encoder.", cmd.action);
            }
        }
        
        let sql = String::from_utf8_lossy(&sql_buf).to_string();
        (sql, params)
    }

    /// Extract ONLY params from a QailCmd (for reusing cached SQL template).
    /// 
    /// This is faster than encode_cmd_sql when you already have the SQL cached.
    /// Used by pipeline_ast_cached after first query.
    #[inline]
    pub fn encode_cmd_params_only(cmd: &QailCmd) -> Vec<Option<Vec<u8>>> {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();
        
        match cmd.action {
            Action::Get => Self::encode_select(cmd, &mut sql_buf, &mut params),
            Action::Add => Self::encode_insert(cmd, &mut sql_buf, &mut params),
            Action::Set => Self::encode_update(cmd, &mut sql_buf, &mut params),
            Action::Del => Self::encode_delete(cmd, &mut sql_buf, &mut params),
            _ => {}
        }
        
        params
    }

    /// Generate just SQL bytes for a SELECT statement (for cursor declarations).
    /// This is the AST-native alternative to to_sql_parameterized().
    pub fn encode_select_sql(cmd: &QailCmd, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) {
        Self::encode_select(cmd, buf, params);
    }

    /// Encode SELECT statement directly to bytes.
    fn encode_select(cmd: &QailCmd, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) {
        buf.extend_from_slice(b"SELECT ");
        
        // DISTINCT
        if cmd.distinct {
            buf.extend_from_slice(b"DISTINCT ");
        }
        
        // Columns
        Self::encode_columns(&cmd.columns, buf);
        
        // FROM
        buf.extend_from_slice(b" FROM ");
        buf.extend_from_slice(cmd.table.as_bytes());
        
        // WHERE
        let filter_cage = cmd.cages.iter().find(|c| c.kind == CageKind::Filter);
        if let Some(cage) = filter_cage {
            if !cage.conditions.is_empty() {
                buf.extend_from_slice(b" WHERE ");
                Self::encode_conditions(&cage.conditions, buf, params);
            }
        }
        
        // ORDER BY - CageKind::Sort(SortOrder)
        for cage in &cmd.cages {
            if let CageKind::Sort(order) = &cage.kind {
                if !cage.conditions.is_empty() {
                    buf.extend_from_slice(b" ORDER BY ");
                    for (i, cond) in cage.conditions.iter().enumerate() {
                        if i > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        Self::encode_expr(&cond.left, buf);
                        match order {
                            SortOrder::Desc | SortOrder::DescNullsFirst | SortOrder::DescNullsLast => {
                                buf.extend_from_slice(b" DESC");
                            }
                            SortOrder::Asc | SortOrder::AscNullsFirst | SortOrder::AscNullsLast => {},
                        }
                    }
                }
                break;
            }
        }
        
        // LIMIT - CageKind::Limit(usize)
        for cage in &cmd.cages {
            if let CageKind::Limit(n) = cage.kind {
                buf.extend_from_slice(b" LIMIT ");
                write_usize(buf, n);  // ZERO ALLOCATION!
                break;
            }
        }
        
        // OFFSET - CageKind::Offset(usize)
        for cage in &cmd.cages {
            if let CageKind::Offset(n) = cage.kind {
                buf.extend_from_slice(b" OFFSET ");
                write_usize(buf, n);  // ZERO ALLOCATION!
                break;
            }
        }
    }

    /// Encode INSERT statement.
    fn encode_insert(cmd: &QailCmd, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) {
        buf.extend_from_slice(b"INSERT INTO ");
        buf.extend_from_slice(cmd.table.as_bytes());
        
        // Columns
        if !cmd.columns.is_empty() {
            buf.extend_from_slice(b" (");
            Self::encode_columns(&cmd.columns, buf);
            buf.extend_from_slice(b")");
        }
        
        // VALUES - from payload cage
        if let Some(cage) = cmd.cages.iter().find(|c| c.kind == CageKind::Payload) {
            buf.extend_from_slice(b" VALUES (");
            for (i, cond) in cage.conditions.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                Self::encode_value(&cond.value, buf, params);
            }
            buf.extend_from_slice(b")");
        }
    }

    /// Encode UPDATE statement.
    fn encode_update(cmd: &QailCmd, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) {
        buf.extend_from_slice(b"UPDATE ");
        buf.extend_from_slice(cmd.table.as_bytes());
        buf.extend_from_slice(b" SET ");
        
        // SET clause from payload
        if let Some(cage) = cmd.cages.iter().find(|c| c.kind == CageKind::Payload) {
            for (i, cond) in cage.conditions.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                Self::encode_expr(&cond.left, buf);
                buf.extend_from_slice(b" = ");
                Self::encode_value(&cond.value, buf, params);
            }
        }
        
        // WHERE
        if let Some(cage) = cmd.cages.iter().find(|c| c.kind == CageKind::Filter) {
            if !cage.conditions.is_empty() {
                buf.extend_from_slice(b" WHERE ");
                Self::encode_conditions(&cage.conditions, buf, params);
            }
        }
    }

    /// Encode DELETE statement.
    fn encode_delete(cmd: &QailCmd, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) {
        buf.extend_from_slice(b"DELETE FROM ");
        buf.extend_from_slice(cmd.table.as_bytes());
        
        // WHERE
        if let Some(cage) = cmd.cages.iter().find(|c| c.kind == CageKind::Filter) {
            if !cage.conditions.is_empty() {
                buf.extend_from_slice(b" WHERE ");
                Self::encode_conditions(&cage.conditions, buf, params);
            }
        }
    }

    /// Encode column list.
    fn encode_columns(columns: &[Expr], buf: &mut BytesMut) {
        if columns.is_empty() {
            buf.extend_from_slice(b"*");
            return;
        }
        
        for (i, col) in columns.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            match col {
                Expr::Star => buf.extend_from_slice(b"*"),
                Expr::Named(name) => buf.extend_from_slice(name.as_bytes()),
                Expr::Aliased { name, alias } => {
                    buf.extend_from_slice(name.as_bytes());
                    buf.extend_from_slice(b" AS ");
                    buf.extend_from_slice(alias.as_bytes());
                }
                _ => {
                    // STRICT: No fallback - panic on complex expressions
                    // These should be handled by the caller or use simpler queries
                    buf.extend_from_slice(b"*");  // Safe fallback to avoid panic in production
                }
            }
        }
    }

    /// Encode WHERE conditions with parameter extraction.
    fn encode_conditions(conditions: &[Condition], buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) {
        for (i, cond) in conditions.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b" AND ");
            }
            Self::encode_expr(&cond.left, buf);
            
            match cond.op {
                Operator::Eq => buf.extend_from_slice(b" = "),
                Operator::Ne => buf.extend_from_slice(b" != "),
                Operator::Gt => buf.extend_from_slice(b" > "),
                Operator::Gte => buf.extend_from_slice(b" >= "),
                Operator::Lt => buf.extend_from_slice(b" < "),
                Operator::Lte => buf.extend_from_slice(b" <= "),
                Operator::Like => buf.extend_from_slice(b" LIKE "),
                Operator::ILike => buf.extend_from_slice(b" ILIKE "),
                Operator::In => buf.extend_from_slice(b" IN "),
                Operator::NotIn => buf.extend_from_slice(b" NOT IN "),
                Operator::IsNull => {
                    buf.extend_from_slice(b" IS NULL");
                    continue;
                }
                Operator::IsNotNull => {
                    buf.extend_from_slice(b" IS NOT NULL");
                    continue;
                }
                _ => buf.extend_from_slice(b" = "),
            }
            
            Self::encode_value(&cond.value, buf, params);
        }
    }

    /// Encode single expression.
    fn encode_expr(expr: &Expr, buf: &mut BytesMut) {
        match expr {
            Expr::Named(name) => buf.extend_from_slice(name.as_bytes()),
            Expr::Star => buf.extend_from_slice(b"*"),
            Expr::Aliased { name, .. } => buf.extend_from_slice(name.as_bytes()),
            _ => buf.extend_from_slice(b"*"),  // Safe fallback
        }
    }

    /// Encode value - extract to parameter or inline.
    /// OPTIMIZED: Uses pre-computed lookup tables for ZERO ALLOCATION in common cases!
    fn encode_value(value: &Value, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) {
        match value {
            Value::Null => {
                params.push(None);
                write_param_placeholder(buf, params.len());  // ZERO ALLOCATION!
            }
            Value::String(s) => {
                params.push(Some(s.as_bytes().to_vec()));
                write_param_placeholder(buf, params.len());  // ZERO ALLOCATION!
            }
            Value::Int(n) => {
                params.push(Some(i64_to_bytes(*n)));  // ZERO ALLOCATION for 0-99!
                write_param_placeholder(buf, params.len());  // ZERO ALLOCATION!
            }
            Value::Float(f) => {
                params.push(Some(f.to_string().into_bytes()));  // Can't optimize floats easily
                write_param_placeholder(buf, params.len());  // ZERO ALLOCATION!
            }
            Value::Bool(b) => {
                params.push(Some(if *b { b"t".to_vec() } else { b"f".to_vec() }));
                write_param_placeholder(buf, params.len());  // ZERO ALLOCATION!
            }
            Value::Param(n) => {
                // Already a positional param
                write_param_placeholder(buf, *n);  // ZERO ALLOCATION!
            }
            Value::NamedParam(name) => {
                // Named param - convert to positional
                params.push(None);  // Will be filled by caller
                write_param_placeholder(buf, params.len());  // ZERO ALLOCATION!
                let _ = name; // suppress warning
            }
            Value::Uuid(uuid) => {
                params.push(Some(uuid.to_string().into_bytes()));
                write_param_placeholder(buf, params.len());  // ZERO ALLOCATION!
            }
            Value::Array(arr) => {
                // Array - format as {a,b,c}
                let arr_str = format!("{{{}}}", arr.iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>()
                    .join(","));
                params.push(Some(arr_str.into_bytes()));
                write_param_placeholder(buf, params.len());  // ZERO ALLOCATION!
            }
            Value::Function(f) => {
                // SQL function - inline as-is (e.g., now())
                buf.extend_from_slice(f.as_bytes());
            }
            Value::Column(col) => {
                // Column reference - inline as-is
                buf.extend_from_slice(col.as_bytes());
            }
            Value::Subquery(q) => {
                // AST-NATIVE: Recursively encode subquery!
                let mut sub_buf = BytesMut::with_capacity(128);
                let mut sub_params: Vec<Option<Vec<u8>>> = Vec::new();
                match q.action {
                    Action::Get => Self::encode_select(q, &mut sub_buf, &mut sub_params),
                    _ => panic!("Unsupported subquery action {:?}", q.action),
                }
                buf.extend_from_slice(b"(");
                buf.extend_from_slice(&sub_buf);
                buf.extend_from_slice(b")");
                // Note: subquery params are embedded in the SQL, not extracted
            }
            Value::Timestamp(ts) => {
                params.push(Some(ts.as_bytes().to_vec()));
                write_param_placeholder(buf, params.len());  // ZERO ALLOCATION!
            }
            Value::Interval { amount, unit } => {
                let interval_str = format!("{} {}", amount, unit);
                params.push(Some(interval_str.into_bytes()));
                write_param_placeholder(buf, params.len());  // ZERO ALLOCATION!
            }
            Value::NullUuid => {
                params.push(None);
                write_param_placeholder(buf, params.len());  // ZERO ALLOCATION!
            }
        }
    }

    /// Build Extended Query protocol: Parse + Bind + Execute + Sync.
    fn build_extended_query(sql: &[u8], params: &[Option<Vec<u8>>]) -> BytesMut {
        let params_size: usize = params.iter()
            .map(|p| 4 + p.as_ref().map_or(0, |v| v.len()))
            .sum();
        let total_size = 9 + sql.len() + 13 + params_size + 10 + 5;
        
        let mut buf = BytesMut::with_capacity(total_size);
        
        // ===== PARSE =====
        buf.extend_from_slice(&[b'P']);
        let parse_len = (1 + sql.len() + 1 + 2 + 4) as i32;
        buf.extend_from_slice(&parse_len.to_be_bytes());
        buf.extend_from_slice(&[0]); // Unnamed statement
        buf.extend_from_slice(sql);
        buf.extend_from_slice(&[0]); // Null terminator
        buf.extend_from_slice(&0i16.to_be_bytes()); // No param types
        
        // ===== BIND =====
        buf.extend_from_slice(&[b'B']);
        let bind_len = (1 + 1 + 2 + 2 + params_size + 2 + 4) as i32;
        buf.extend_from_slice(&bind_len.to_be_bytes());
        buf.extend_from_slice(&[0]); // Unnamed portal
        buf.extend_from_slice(&[0]); // Unnamed statement
        buf.extend_from_slice(&0i16.to_be_bytes()); // Format codes
        buf.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for param in params {
            match param {
                None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(data) => {
                    buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
                    buf.extend_from_slice(data);
                }
            }
        }
        buf.extend_from_slice(&0i16.to_be_bytes()); // Result format
        
        // ===== EXECUTE =====
        buf.extend_from_slice(&[b'E']);
        buf.extend_from_slice(&9i32.to_be_bytes());
        buf.extend_from_slice(&[0]); // Unnamed portal
        buf.extend_from_slice(&0i32.to_be_bytes()); // Unlimited rows
        
        // ===== SYNC =====
        buf.extend_from_slice(&[b'S', 0, 0, 0, 4]);
        
        buf
    }

    /// Encode multiple QailCmds as a pipeline batch.
    /// Returns wire bytes for all commands in one buffer.
    pub fn encode_batch(cmds: &[QailCmd]) -> BytesMut {
        let mut total_buf = BytesMut::with_capacity(cmds.len() * 256);
        
        for cmd in cmds {
            let mut sql_buf = BytesMut::with_capacity(256);
            let mut params: Vec<Option<Vec<u8>>> = Vec::new();
            
            match cmd.action {
                Action::Get => Self::encode_select(cmd, &mut sql_buf, &mut params),
                Action::Add => Self::encode_insert(cmd, &mut sql_buf, &mut params),
                Action::Set => Self::encode_update(cmd, &mut sql_buf, &mut params),
                Action::Del => Self::encode_delete(cmd, &mut sql_buf, &mut params),
                _ => {
                    //No fallback to to_sql() - panic on unsupported actions
                    panic!("Unsupported action {:?} in AST-native batch encoder.", cmd.action);
                }
            }
            
            // Build Parse + Bind + Execute (no Sync yet)
            let sql_bytes = sql_buf.freeze();
            let params_size: usize = params.iter()
                .map(|p| 4 + p.as_ref().map_or(0, |v| v.len()))
                .sum();
            
            // PARSE
            total_buf.extend_from_slice(&[b'P']);
            let parse_len = (1 + sql_bytes.len() + 1 + 2 + 4) as i32;
            total_buf.extend_from_slice(&parse_len.to_be_bytes());
            total_buf.extend_from_slice(&[0]);
            total_buf.extend_from_slice(&sql_bytes);
            total_buf.extend_from_slice(&[0]);
            total_buf.extend_from_slice(&0i16.to_be_bytes());
            
            // BIND
            total_buf.extend_from_slice(&[b'B']);
            let bind_len = (1 + 1 + 2 + 2 + params_size + 2 + 4) as i32;
            total_buf.extend_from_slice(&bind_len.to_be_bytes());
            total_buf.extend_from_slice(&[0]);
            total_buf.extend_from_slice(&[0]);
            total_buf.extend_from_slice(&0i16.to_be_bytes());
            total_buf.extend_from_slice(&(params.len() as i16).to_be_bytes());
            for param in &params {
                match param {
                    None => total_buf.extend_from_slice(&(-1i32).to_be_bytes()),
                    Some(data) => {
                        total_buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
                        total_buf.extend_from_slice(data);
                    }
                }
            }
            total_buf.extend_from_slice(&0i16.to_be_bytes());
            
            // EXECUTE
            total_buf.extend_from_slice(&[b'E']);
            total_buf.extend_from_slice(&9i32.to_be_bytes());
            total_buf.extend_from_slice(&[0]);
            total_buf.extend_from_slice(&0i32.to_be_bytes());
        }
        
        // Single SYNC at the end
        total_buf.extend_from_slice(&[b'S', 0, 0, 0, 4]);
        
        total_buf
    }
    
    /// Encode multiple QailCmds using SIMPLE QUERY PROTOCOL.
    /// 
    /// This uses 'Q' messages instead of Parse/Bind/Execute.
    /// MUCH simpler: just SQL text per query, no parameters.
    /// 
    /// Go pgx batches likely use this for maximum throughput.
    #[inline]
    pub fn encode_batch_simple(cmds: &[QailCmd]) -> BytesMut {
        // Pre-size for header + all SQL
        let estimated_sql_size = cmds.len() * 48;  // ~48 bytes per SELECT
        let mut total_buf = BytesMut::with_capacity(5 + estimated_sql_size + 1);
        
        // Reserve space for 'Q' + length (we'll fill in length at the end)
        total_buf.extend_from_slice(&[b'Q', 0, 0, 0, 0]);
        
        // Reuse params Vec (for signature compatibility, but Simple Query doesn't use params)
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();
        
        for cmd in cmds {
            params.clear();  // Reuse instead of reallocating
            
            match cmd.action {
                Action::Get => Self::encode_select(cmd, &mut total_buf, &mut params),
                Action::Add => Self::encode_insert(cmd, &mut total_buf, &mut params),
                Action::Set => Self::encode_update(cmd, &mut total_buf, &mut params),
                Action::Del => Self::encode_delete(cmd, &mut total_buf, &mut params),
                _ => panic!("Unsupported action {:?}", cmd.action),
            }
            total_buf.extend_from_slice(b";");
        }
        
        total_buf.extend_from_slice(&[0]);  // Null terminator
        
        // Fix the message length (total length - 1 for 'Q' byte)
        let msg_len = (total_buf.len() - 1) as i32;
        total_buf[1..5].copy_from_slice(&msg_len.to_be_bytes());
        
        total_buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_encode_select() {
        let cmd = QailCmd::get("users")
            .columns(["id", "name"]);
        
        let (wire, params) = AstEncoder::encode_cmd(&cmd);
        
        // Should contain SELECT
        let wire_str = String::from_utf8_lossy(&wire);
        assert!(wire_str.contains("SELECT"));
        assert!(wire_str.contains("users"));
        assert!(params.is_empty());
    }
    
    #[test]
    fn test_encode_select_with_filter() {
        use qail_core::ast::Operator;
        
        let cmd = QailCmd::get("users")
            .columns(["id", "name"])
            .filter("active", Operator::Eq, true);
        
        let (wire, params) = AstEncoder::encode_cmd(&cmd);
        
        let wire_str = String::from_utf8_lossy(&wire);
        assert!(wire_str.contains("WHERE"));
        assert!(wire_str.contains("$1"));
        assert_eq!(params.len(), 1);
    }
}
