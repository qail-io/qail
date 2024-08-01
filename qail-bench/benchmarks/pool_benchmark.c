/*
 * C libpq POOL BENCHMARK
 * 
 * libpq doesn't have built-in pooling, so we simulate with:
 * - Pre-created connections (1 per thread)
 * - Pipelining within each connection
 * 
 * Compile: gcc -O3 -march=native -o pool_benchmark pool_benchmark.c -lpq -lpthread
 * Run: ./pool_benchmark
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <pthread.h>
#include <time.h>
#include <stdatomic.h>

#define TOTAL_QUERIES 150000000
#define NUM_WORKERS 10
#define POOL_SIZE 10
#define QUERIES_PER_BATCH 100

static atomic_long counter = 0;
static int batches_per_worker;

typedef struct {
    int worker_id;
    const char *conninfo;
} WorkerArgs;

void* worker_thread(void* arg) {
    WorkerArgs* args = (WorkerArgs*)arg;
    
    // Each worker has its own connection (simulating pool)
    PGconn* conn = PQconnectdb(args->conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Worker %d: Connection failed: %s\n", args->worker_id, PQerrorMessage(conn));
        PQfinish(conn);
        return NULL;
    }
    
    // Prepare statement BEFORE pipeline mode
    const char* stmt_name = "select_harbors";
    const char* sql = "SELECT id, name FROM harbors LIMIT $1";
    PGresult* res = PQprepare(conn, stmt_name, sql, 1, NULL);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Worker %d: Prepare failed: %s\n", args->worker_id, PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return NULL;
    }
    PQclear(res);
    
    // Enable pipelining AFTER prepare
    if (PQenterPipelineMode(conn) != 1) {
        fprintf(stderr, "Worker %d: Failed to enter pipeline mode\n", args->worker_id);
        PQfinish(conn);
        return NULL;
    }
    
    // Execute batches
    for (int b = 0; b < batches_per_worker; b++) {
        // Send batch of queries
        for (int i = 0; i < QUERIES_PER_BATCH; i++) {
            char limit_str[16];
            snprintf(limit_str, sizeof(limit_str), "%d", (i % 10) + 1);
            const char* params[1] = { limit_str };
            
            if (PQsendQueryPrepared(conn, stmt_name, 1, params, NULL, NULL, 0) != 1) {
                fprintf(stderr, "Worker %d: Send failed\n", args->worker_id);
                break;
            }
        }
        
        // Sync pipeline and flush
        PQpipelineSync(conn);
        PQflush(conn);
        
        // Consume results
        for (int q = 0; q < QUERIES_PER_BATCH; q++) {
            // Each query: get result until NULL
            PGresult* result;
            while ((result = PQgetResult(conn)) != NULL) {
                ExecStatusType status = PQresultStatus(result);
                if (status == PGRES_TUPLES_OK) {
                    // Consume rows
                    int nrows = PQntuples(result);
                    for (int r = 0; r < nrows; r++) {
                        char* id = PQgetvalue(result, r, 0);
                        char* name = PQgetvalue(result, r, 1);
                        (void)id; (void)name;
                    }
                    atomic_fetch_add(&counter, 1);
                }
                PQclear(result);
            }
        }
        
        // Consume pipeline sync
        PGresult* sync_result = PQgetResult(conn);
        if (sync_result) {
            PQclear(sync_result);
        }
    }
    
    PQexitPipelineMode(conn);
    PQfinish(conn);
    return NULL;
}

int main() {
    const char* host = getenv("PG_HOST") ? getenv("PG_HOST") : "127.0.0.1";
    const char* port = getenv("PG_PORT") ? getenv("PG_PORT") : "5432";
    const char* user = getenv("PG_USER") ? getenv("PG_USER") : "postgres";
    const char* database = getenv("PG_DATABASE") ? getenv("PG_DATABASE") : "postgres";
    
    char conninfo[512];
    snprintf(conninfo, sizeof(conninfo), "host=%s port=%s user=%s dbname=%s", 
             host, port, user, database);
    
    printf("🔌 Connecting to %s:%s as %s\n", host, port, user);
    printf("🚀 C LIBPQ POOL BENCHMARK\n");
    printf("=========================\n");
    printf("Total queries:    %15d\n", TOTAL_QUERIES);
    printf("Workers:          %15d\n", NUM_WORKERS);
    printf("Pool size:        %15d\n", POOL_SIZE);
    printf("Batch size:       %15d\n", QUERIES_PER_BATCH);
    printf("\n");
    
    batches_per_worker = TOTAL_QUERIES / NUM_WORKERS / QUERIES_PER_BATCH;
    
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    
    // Create worker threads
    pthread_t threads[NUM_WORKERS];
    WorkerArgs args[NUM_WORKERS];
    
    for (int i = 0; i < NUM_WORKERS; i++) {
        args[i].worker_id = i;
        args[i].conninfo = conninfo;
        pthread_create(&threads[i], NULL, worker_thread, &args[i]);
    }
    
    // Wait for all threads
    for (int i = 0; i < NUM_WORKERS; i++) {
        pthread_join(threads[i], NULL);
    }
    
    clock_gettime(CLOCK_MONOTONIC, &end);
    
    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    double qps = counter / elapsed;
    
    printf("\n📈 FINAL RESULTS:\n");
    printf("┌──────────────────────────────────────────────────┐\n");
    printf("│ C LIBPQ POOL BENCHMARK                           │\n");
    printf("├──────────────────────────────────────────────────┤\n");
    printf("│ Total Time:               %15.1fs │\n", elapsed);
    printf("│ Queries/Second:           %15.0f │\n", qps);
    printf("│ Workers:                  %15d │\n", NUM_WORKERS);
    printf("│ Pool Size:                %15d │\n", POOL_SIZE);
    printf("│ Queries Completed:        %15ld │\n", counter);
    printf("└──────────────────────────────────────────────────┘\n");
    
    return 0;
}
