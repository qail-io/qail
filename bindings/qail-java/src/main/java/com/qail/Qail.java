package com.qail;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

/**
 * QAIL - Query Abstraction Interface Language for Java
 * 
 * Example:
 *   String sql = Qail.transpile("get::users:'_", "postgres");
 *   System.out.println(sql); // SELECT * FROM "users"
 * 
 * Requires JNA library.
 */
public class Qail {
    
    private interface QailLib extends Library {
        QailLib INSTANCE = Native.load("qail_ffi", QailLib.class);
        
        Pointer qail_transpile(String qail);
        Pointer qail_transpile_with_dialect(String qail, String dialect);
        Pointer qail_parse_json(String qail);
        int qail_validate(String qail);
        String qail_last_error();
        void qail_free(Pointer ptr);
        Pointer qail_version();
    }
    
    /**
     * Transpile QAIL to SQL
     * 
     * @param qail QAIL query string
     * @param dialect SQL dialect (postgres, mysql, sqlite, sqlserver)
     * @return SQL query string
     * @throws RuntimeException on parse error
     */
    public static String transpile(String qail, String dialect) {
        Pointer result = QailLib.INSTANCE.qail_transpile_with_dialect(qail, dialect);
        if (result == null) {
            String error = QailLib.INSTANCE.qail_last_error();
            throw new RuntimeException(error != null ? error : "Unknown QAIL error");
        }
        String sql = result.getString(0);
        QailLib.INSTANCE.qail_free(result);
        return sql;
    }
    
    /**
     * Transpile QAIL to PostgreSQL (default)
     */
    public static String transpile(String qail) {
        return transpile(qail, "postgres");
    }
    
    /**
     * Parse QAIL and return AST as JSON
     */
    public static String parseJson(String qail) {
        Pointer result = QailLib.INSTANCE.qail_parse_json(qail);
        if (result == null) {
            String error = QailLib.INSTANCE.qail_last_error();
            throw new RuntimeException(error != null ? error : "Unknown QAIL error");
        }
        String json = result.getString(0);
        QailLib.INSTANCE.qail_free(result);
        return json;
    }
    
    /**
     * Validate QAIL syntax
     */
    public static boolean validate(String qail) {
        return QailLib.INSTANCE.qail_validate(qail) == 1;
    }
    
    /**
     * Get QAIL version
     */
    public static String version() {
        Pointer result = QailLib.INSTANCE.qail_version();
        if (result == null) {
            return "unknown";
        }
        String version = result.getString(0);
        QailLib.INSTANCE.qail_free(result);
        return version;
    }
    
    // Convenience methods
    public static String toPostgres(String qail) {
        return transpile(qail, "postgres");
    }
    
    public static String toMysql(String qail) {
        return transpile(qail, "mysql");
    }
    
    public static String toSqlite(String qail) {
        return transpile(qail, "sqlite");
    }
}
