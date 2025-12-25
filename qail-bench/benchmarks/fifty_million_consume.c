/*
 * 10 MILLION QUERY BENCHMARK - C libpq WITH RESULT CONSUMPTION
 * 
 * This test actually reads and parses row data for fair comparison.
 * Uses maximum optimization flags.
 * 
 * ## Configuration
 * 
 * Set environment variables:
 *   export PG_HOST=127.0.0.1
 *   export PG_PORT=5432
 *   export PG_USER=postgres
 *   export PG_DATABASE=postgres
 * 
 * ## Build (Maximum Optimization)
 * 
 *   gcc -O3 -march=native -flto -ffast-math \
 *       -o fifty_million_consume fifty_million_consume.c \
 *       -I$(pg_config --includedir) -L$(pg_config --libdir) -lpq
 * 
 * ## Run
 * 
 *   ./fifty_million_consume
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <libpq-fe.h>

#define TOTAL_QUERIES 10000000
#define QUERIES_PER_BATCH 1000
#define BATCHES (TOTAL_QUERIES / QUERIES_PER_BATCH)

const char* get_env_or(const char* key, const char* default_val) {
    const char* val = getenv(key);
    return val ? val : default_val;
}

double get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1000000.0;
}

int main() {
    printf("🔧 10 MILLION QUERY BENCHMARK - C libpq WITH RESULT CONSUMPTION\n");
    printf("================================================================\n");
    printf("Total queries:    %15d\n", TOTAL_QUERIES);
    printf("Batch size:       %15d\n", QUERIES_PER_BATCH);
    printf("Batches:          %15d\n", BATCHES);
    printf("\n⚠️  This test READS and PARSES all row data!\n\n");
    
    // Read connection info from environment
    const char* host = get_env_or("PG_HOST", "127.0.0.1");
    const char* port = get_env_or("PG_PORT", "5432");
    const char* user = get_env_or("PG_USER", "postgres");
    const char* database = get_env_or("PG_DATABASE", "postgres");
    
    char conninfo[256];
    snprintf(conninfo, sizeof(conninfo), 
             "host=%s port=%s user=%s dbname=%s", 
             host, port, user, database);
    
    printf("🔌 Connecting to %s:%s as %s\n", host, port, user);
    
    // Connect
    PGconn *conn = PQconnectdb(conninfo);
    
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }
    
    printf("✅ Connected to PostgreSQL\n");
    
    // Prepare statement (same as QAIL)
    PGresult *res = PQprepare(conn, "stmt1", 
        "SELECT id, name FROM harbors LIMIT $1", 1, NULL);
    
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Prepare failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return 1;
    }
    PQclear(res);
    printf("✅ Statement prepared\n\n");
    
    printf("📊 Executing %d queries with result consumption...\n\n", TOTAL_QUERIES);
    
    double start = get_time_ms();
    int successful = 0;
    long total_rows = 0;
    double last_report = start;
    
    // Enter pipeline mode (PostgreSQL 14+)
    if (PQenterPipelineMode(conn) == 0) {
        fprintf(stderr, "Failed to enter pipeline mode (requires PostgreSQL 14+)\n");
        PQfinish(conn);
        return 1;
    }
    
    // Pre-build params (FAIR: same optimization as QAIL)
    static char params[1000][16];
    static const char* paramPtrs[1000];
    for (int i = 0; i < QUERIES_PER_BATCH; i++) {
        snprintf(params[i], sizeof(params[i]), "%d", (i % 10) + 1);
        paramPtrs[i] = params[i];
    }
    printf("✅ Params pre-built (same as QAIL)\n\n");
    
    for (int batch = 0; batch < BATCHES; batch++) {
        // Send batch of queries using pre-built params
        for (int i = 0; i < QUERIES_PER_BATCH; i++) {
            const char *paramValues[1] = {paramPtrs[i]};
            
            if (PQsendQueryPrepared(conn, "stmt1", 1, paramValues, NULL, NULL, 0) == 0) {
                fprintf(stderr, "Send failed: %s\n", PQerrorMessage(conn));
                break;
            }
        }
        
        // Sync to flush pipeline
        PQpipelineSync(conn);
        
        // Consume results - ACTUALLY READ ROW DATA
        for (int i = 0; i < QUERIES_PER_BATCH; i++) {
            res = PQgetResult(conn);
            if (res == NULL) break;
            
            if (PQresultStatus(res) == PGRES_TUPLES_OK) {
                int nrows = PQntuples(res);
                int ncols = PQnfields(res);
                
                // CONSUME: Actually read each row's data
                for (int row = 0; row < nrows; row++) {
                    if (ncols >= 2) {
                        // Read id (column 0)
                        char* id_str = PQgetvalue(res, row, 0);
                        long id = atol(id_str);
                        (void)id;  // Use but don't print
                        
                        // Read name (column 1)
                        char* name = PQgetvalue(res, row, 1);
                        (void)name;  // Use but don't print
                        
                        total_rows++;
                    }
                }
                successful++;
            }
            PQclear(res);
            
            // Consume the NULL result that marks end of command
            res = PQgetResult(conn);
            if (res) PQclear(res);
        }
        
        // Consume pipeline sync result
        res = PQgetResult(conn);
        if (res) PQclear(res);
        
        // Progress report every 1M queries
        double now = get_time_ms();
        if (successful % 1000000 == 0 || (now - last_report) >= 5000) {
            double elapsed = (now - start) / 1000.0;
            double qps = successful / elapsed;
            int remaining = TOTAL_QUERIES - successful;
            double eta = remaining / qps;
            printf("   %3dM queries | %8.0f q/s | ETA: %.0fs | Rows: %ld | Batch %d/%d\n", 
                   successful / 1000000, qps, eta, total_rows, batch + 1, BATCHES);
            last_report = now;
        }
    }
    
    PQexitPipelineMode(conn);
    
    double elapsed = (get_time_ms() - start) / 1000.0;
    double qps = TOTAL_QUERIES / elapsed;
    double per_query_ns = (elapsed * 1000000000.0) / TOTAL_QUERIES;
    
    printf("\n📈 FINAL RESULTS:\n");
    printf("┌──────────────────────────────────────────────────┐\n");
    printf("│ 10 MILLION QUERY - C libpq WITH CONSUMPTION      │\n");
    printf("├──────────────────────────────────────────────────┤\n");
    printf("│ Total Time:            %20.1fs │\n", elapsed);
    printf("│ Queries/Second:        %20.0f │\n", qps);
    printf("│ Per Query:             %17.0fns │\n", per_query_ns);
    printf("│ Successful:            %20d │\n", successful);
    printf("│ Rows Parsed:           %20ld │\n", total_rows);
    printf("│ Avg Rows/Query:        %20.1f │\n", (double)total_rows / successful);
    printf("└──────────────────────────────────────────────────┘\n");
    
    printf("\n📝 Compiled with: -O3 -march=native -flto -ffast-math\n");
    
    PQfinish(conn);
    return 0;
}
