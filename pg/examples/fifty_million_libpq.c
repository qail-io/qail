/*
 * 50 MILLION QUERY STRESS TEST - C libpq (PostgreSQL native driver)
 * 
 * Tests C's memory stability with PostgreSQL 14+ pipelining.
 * Compare with QAIL's fifty_million.rs
 * 
 * Build: gcc -O3 -o fifty_million_libpq fifty_million_libpq.c -I$(pg_config --includedir) -L$(pg_config --libdir) -lpq
 * Run: ./fifty_million_libpq
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <libpq-fe.h>

#define TOTAL_QUERIES 50000000
#define QUERIES_PER_BATCH 10000
#define BATCHES (TOTAL_QUERIES / QUERIES_PER_BATCH)

double get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1000000.0;
}

int main() {
    printf("🔧 50 MILLION QUERY STRESS TEST - C libpq\n");
    printf("==========================================\n");
    printf("Total queries:    %15d\n", TOTAL_QUERIES);
    printf("Batch size:       %15d\n", QUERIES_PER_BATCH);
    printf("Batches:          %15d\n", BATCHES);
    printf("\n");
    
    // Connect
    PGconn *conn = PQconnectdb("host=127.0.0.1 port=5432 user=orion dbname=swb_staging_local");
    
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }
    
    printf("✅ Connected to PostgreSQL\n");
    
    // Prepare statement
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
    
    printf("📊 Executing %d queries...\n\n", TOTAL_QUERIES);
    
    double start = get_time_ms();
    int successful = 0;
    double last_report = start;
    
    // Enter pipeline mode (PostgreSQL 14+)
    if (PQenterPipelineMode(conn) == 0) {
        fprintf(stderr, "Failed to enter pipeline mode\n");
        PQfinish(conn);
        return 1;
    }
    
    // Pre-build params (same optimization as QAIL)
    static char params[10000][16];
    static const char* paramPtrs[10000];
    for (int i = 0; i < QUERIES_PER_BATCH; i++) {
        snprintf(params[i], sizeof(params[i]), "%d", (i % 10) + 1);
        paramPtrs[i] = params[i];
    }
    printf("✅ Params pre-built\n\n");
    
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
        
        // Consume results
        for (int i = 0; i < QUERIES_PER_BATCH; i++) {
            res = PQgetResult(conn);
            if (res == NULL) break;
            
            if (PQresultStatus(res) == PGRES_TUPLES_OK) {
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
            printf("   %3dM queries | %8.0f q/s | ETA: %.0fs | Batch %d/%d\n", 
                   successful / 1000000, qps, eta, batch + 1, BATCHES);
            last_report = now;
        }
    }
    
    PQexitPipelineMode(conn);
    
    double elapsed = (get_time_ms() - start) / 1000.0;
    double qps = TOTAL_QUERIES / elapsed;
    double per_query_ns = (elapsed * 1000000000.0) / TOTAL_QUERIES;
    
    printf("\n📈 FINAL RESULTS:\n");
    printf("┌──────────────────────────────────────────┐\n");
    printf("│ 50 MILLION QUERIES - C libpq             │\n");
    printf("├──────────────────────────────────────────┤\n");
    printf("│ Total Time:        %20.1fs │\n", elapsed);
    printf("│ Queries/Second:    %20.0f │\n", qps);
    printf("│ Per Query:         %17.0fns │\n", per_query_ns);
    printf("│ Successful:        %20d │\n", successful);
    printf("└──────────────────────────────────────────┘\n");
    
    PQfinish(conn);
    return 0;
}
