

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <getopt.h>

#define MAX_COURSES         20
#define MAX_STUDENTS        200
#define MAX_NAME_LEN        64
#define LOG_FILE            "registration_log.txt"
#define CSV_FILE            "registration_report.csv"

#define PRIORITY_HIGH       1
#define PRIORITY_NORMAL     0
#define HIGH_PRIORITY_RATIO 5

#define RST   "\033[0m"
#define GRN   "\033[0;32m"
#define RED   "\033[0;31m"
#define YLW   "\033[0;33m"
#define CYN   "\033[0;36m"
#define MAG   "\033[0;35m"
#define BOLD  "\033[1m"

typedef struct {
    int  num_students;   
    int  num_courses;    
    int  stress_mode;    
    int  max_retries;    
    int  export_csv;     
    int  demo_mode;      
    int  scenario_mode;  
    unsigned int seed;   
} Config;

static Config cfg = {
    .num_students = 100,
    .num_courses  = 15,
    .stress_mode  = 0,
    .max_retries  = 2,
    .export_csv   = 0,
    .demo_mode    = 0,
    .scenario_mode= 0,
    .seed         = 0
};

static volatile sig_atomic_t shutdown_flag = 0;

typedef struct {
    int             id;
    char            name[MAX_NAME_LEN];
    int             total_seats;
    int             available_seats;
    int             enrolled;          
    pthread_mutex_t mutex;
} Course;

typedef struct {
    int  student_id;
    int  priority;
    int  course_index;   
    int  registered;     
    int  retries_used;
    int  final_course;   
    pthread_t   tid;
    unsigned int rand_seed; 

    int  attempted[MAX_COURSES];
} StudentRequest;

typedef struct PQNode {
    StudentRequest *req;
    struct PQNode  *next;
} PQNode;

typedef struct {
    PQNode          *head;
    int              size;
    pthread_mutex_t  mutex;
    pthread_cond_t   not_empty;
} PriorityQueue;

static Course         courses[MAX_COURSES];
static StudentRequest students[MAX_STUDENTS];
static PriorityQueue  pqueue;

static FILE          *log_fp     = NULL;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

static int            total_success = 0;
static int            total_failure = 0;
static int            total_retried = 0;
static pthread_mutex_t stats_mutex  = PTHREAD_MUTEX_INITIALIZER;

/* Signal handler for graceful shutdown on SIGINT. Sets shutdown_flag. */
static void handle_sigint(int sig)
{
    (void)sig;
    shutdown_flag = 1;
}

/* Prints formatted fatal errors to stderr and exits the program. */
static void die(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, RED "[FATAL] " RST);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    if (errno) fprintf(stderr, ": %s", strerror(errno));
    fputc('\n', stderr);
    exit(EXIT_FAILURE);
}

/* Strips ANSI escape codes from string src, copying the plain text to dst. */
static void strip_ansi(char *dst, const char *src)
{
    while (*src) {
        if (*src == '\033' && *(src+1) == '[') {

            src += 2;
            while (*src && *src != 'm') src++;
            if (*src) src++; 
        } else {
            *dst++ = *src++;
        }
    }
    *dst = '\0';
}

/* Thread-safe logger. Prints colored text to terminal and plain text to log file. */
static void log_event(const char *fmt, ...)
{
    va_list a1, a2;
    va_start(a1, fmt);
    va_copy(a2, a1);

    pthread_mutex_lock(&log_mutex);

    vprintf(fmt, a1);

    if (log_fp) {
        char coloured[1024], plain[1024];
        vsnprintf(coloured, sizeof coloured, fmt, a2);
        strip_ansi(plain, coloured);
        fputs(plain, log_fp);
        fflush(log_fp);
    }

    pthread_mutex_unlock(&log_mutex);
    va_end(a1);
    va_end(a2);
}

/* Initializes course structures and mutexes. Randomizes seats in stress mode, overrides for scenario mode. */
static void init_courses(void)
{
    if (cfg.scenario_mode) {
        const char *names[] = {"CS101", "CS102", "CS103"};
        const int seats[] = {2, 1, 3};
        for (int i = 0; i < 3; i++) {
            courses[i].id = i;
            courses[i].enrolled = 0;
            strncpy(courses[i].name, names[i], MAX_NAME_LEN - 1);
            courses[i].name[MAX_NAME_LEN - 1] = '\0';
            courses[i].total_seats = seats[i];
            courses[i].available_seats = seats[i];
            int rc = pthread_mutex_init(&courses[i].mutex, NULL);
            if (rc) { errno = rc; die("pthread_mutex_init course %d", i); }
        }
        return;
    }

    static const char *default_names[] = {
        "Operating Systems",    "Data Structures",
        "Algorithms",           "Computer Networks",
        "Database Systems",     "Software Engineering",
        "Artificial Intelligence","Computer Architecture",
        "Computer Graphics",    "Compiler Design",
        "Machine Learning",     "Cyber Security",
        "Cloud Computing",      "Embedded Systems",
        "Digital Logic",        "Discrete Mathematics",
        "Linear Algebra",       "Probability & Stats",
        "Calculus II",          "Technical Writing"
    };
    static const int default_seats[] = {
        10,15,12,8,20,10,6,14,8,10,
        8,12,15,6,10,20,20,15,25,12
    };

    int n = cfg.num_courses;
    for (int i = 0; i < n; i++) {
        courses[i].id       = i;
        courses[i].enrolled = 0;
        strncpy(courses[i].name, default_names[i % 20], MAX_NAME_LEN - 1);
        courses[i].name[MAX_NAME_LEN - 1] = '\0';

        if (cfg.stress_mode)
            courses[i].total_seats = 5 + rand() % 46; 
        else
            courses[i].total_seats = default_seats[i % 20];

        courses[i].available_seats = courses[i].total_seats;

        int rc = pthread_mutex_init(&courses[i].mutex, NULL);
        if (rc) { errno = rc; die("pthread_mutex_init course %d", i); }
    }
}

/* Initializes student requests, assigns target courses and priorities. */
static void init_students(void)
{
    int ratio = cfg.stress_mode ? 3 : HIGH_PRIORITY_RATIO;
    if (cfg.scenario_mode) ratio = 3; /* 10 / 3 = 4 high priority students */
    for (int i = 0; i < cfg.num_students; i++) {
        students[i].student_id   = i + 1;
        students[i].course_index = rand() % cfg.num_courses;
        students[i].registered   = 0;
        students[i].retries_used = 0;
        students[i].final_course = -1;
        students[i].rand_seed    = (unsigned)(rand() ^ (i * 2654435761u));
        students[i].priority     = ((i % ratio) == 0)
                                   ? PRIORITY_HIGH : PRIORITY_NORMAL;
        memset(students[i].attempted, 0, sizeof students[i].attempted);
    }
}

/* Initializes the priority queue, creating its mutex and condition variable. */
static void pqueue_init(PriorityQueue *pq)
{
    pq->head = NULL; pq->size = 0;
    int rc;
    if ((rc = pthread_mutex_init(&pq->mutex, NULL)))
        { errno = rc; die("pqueue mutex_init"); }
    if ((rc = pthread_cond_init(&pq->not_empty, NULL)))
        { errno = rc; die("pqueue cond_init"); }
}

/* Thread-safe enqueue. Inserts nodes keeping HIGH priority ahead of NORMAL priority in a stable FIFO order. */
static void pqueue_enqueue(PriorityQueue *pq, StudentRequest *req)
{
    PQNode *node = malloc(sizeof *node);
    if (!node) die("malloc PQNode");
    node->req = req; node->next = NULL;

    pthread_mutex_lock(&pq->mutex);
    if (!pq->head || req->priority > pq->head->req->priority) {
        node->next = pq->head;
        pq->head   = node;
    } else {
        PQNode *cur = pq->head;
        while (cur->next && cur->next->req->priority >= req->priority)
            cur = cur->next;
        node->next = cur->next;
        cur->next  = node;
    }
    pq->size++;
    pthread_cond_signal(&pq->not_empty);
    pthread_mutex_unlock(&pq->mutex);
}

/* Thread-safe dequeue. Removes and returns the highest priority request. Non-blocking; returns NULL if empty. */
static StudentRequest *pqueue_dequeue(PriorityQueue *pq)
{
    pthread_mutex_lock(&pq->mutex);
    if (!pq->head) { pthread_mutex_unlock(&pq->mutex); return NULL; }
    PQNode         *node = pq->head;
    StudentRequest *req  = node->req;
    pq->head = node->next;
    pq->size--;
    free(node);
    pthread_mutex_unlock(&pq->mutex);
    return req;
}

/* Cleans up and frees memory/mutexes used by the priority queue. */
static void pqueue_destroy(PriorityQueue *pq)
{
    pthread_mutex_lock(&pq->mutex);
    PQNode *cur = pq->head;
    while (cur) { PQNode *t = cur->next; free(cur); cur = t; }
    pq->head = NULL; pq->size = 0;
    pthread_mutex_unlock(&pq->mutex);
    pthread_mutex_destroy(&pq->mutex);
    pthread_cond_destroy(&pq->not_empty);
}

/* Attempts to decrement available seats for a course. Thread-safe via per-course mutex. Returns 1 on success. */
static int try_register(StudentRequest *req, int course_idx)
{

    if (req->attempted[course_idx]) {
        return 0; 
    }
    req->attempted[course_idx] = 1;

    Course *c = &courses[course_idx];
    pthread_mutex_lock(&c->mutex);
    int ok = (c->available_seats > 0);
    if (ok) { c->available_seats--; c->enrolled++; }
    pthread_mutex_unlock(&c->mutex);
    return ok;
}

/* Worker thread used in DEMO mode to prove strict priority. Dequeues requests and attempts registration. */
static void *worker_thread(void *arg)
{
    (void)arg;
    StudentRequest *target = pqueue_dequeue(&pqueue);
    if (!target) return NULL;

    const char *ptag = (target->priority == PRIORITY_HIGH)
                       ? MAG "[HIGH]" RST " " : CYN "[NORM]" RST " ";

    int ok = try_register(target, target->course_index);
    target->registered = ok;
    if (ok) target->final_course = target->course_index;

    pthread_mutex_lock(&stats_mutex);
    if (ok) total_success++; else total_failure++;
    pthread_mutex_unlock(&stats_mutex);

    Course *c = &courses[target->course_index];
    if (ok)
        log_event("%sStudent %3d  ✓ Enrolled  → %-28s  (seats left: %2d)\n",
                  ptag, target->student_id, c->name, c->available_seats);
    else
        log_event("%sStudent %3d  ✗ FAILED    → %-28s  (course full)\n",
                  ptag, target->student_id, c->name);
    return NULL;
}

/* Normal student thread. Enqueues its request, dequeues the highest priority request, and attempts registration. Includes retry logic. */
static void *student_thread(void *arg)
{
    if (shutdown_flag) return NULL;

    StudentRequest *req = (StudentRequest *)arg;
    unsigned int   *rs  = &req->rand_seed;  

    struct timespec delay = { .tv_sec = 0,
        .tv_nsec = (long)(rand_r(rs) % 16) * 1000000L };
    nanosleep(&delay, NULL);

    pqueue_enqueue(&pqueue, req);

    StudentRequest *target = pqueue_dequeue(&pqueue);
    if (!target) return NULL;

    unsigned int *ts = &target->rand_seed;

    const char *ptag = (target->priority == PRIORITY_HIGH)
                       ? MAG "[HIGH]" RST " " : CYN "[NORM]" RST " ";

    int ok = try_register(target, target->course_index);

    int attempt = 0;
    while (!ok && attempt < cfg.max_retries && !shutdown_flag) {
        attempt++;
        int alt = (int)(rand_r(ts) % (unsigned)cfg.num_courses);
        if (alt == target->course_index)
            alt = (alt + 1) % cfg.num_courses;

        struct timespec retry_delay = { .tv_sec = 0,
            .tv_nsec = (long)(10 + rand_r(ts) % 20) * 1000000L };
        nanosleep(&retry_delay, NULL);

        ok = try_register(target, alt);
        if (ok) {
            target->retries_used = attempt;
            target->course_index = alt;
        }
    }

    target->registered = ok;
    if (ok) target->final_course = target->course_index;

    pthread_mutex_lock(&stats_mutex);
    if (ok) total_success++; else total_failure++;
    if (target->retries_used > 0) total_retried++;
    pthread_mutex_unlock(&stats_mutex);

    Course *c = &courses[target->course_index];
    if (ok)
        log_event("%sStudent %3d  ✓ Enrolled  → %-28s  "
                  "(seats left: %2d, retries: %d)\n",
                  ptag, target->student_id, c->name,
                  c->available_seats, target->retries_used);
    else
        log_event("%sStudent %3d  ✗ FAILED    → %-28s  "
                  "(course full after %d retries)\n",
                  ptag, target->student_id, c->name,
                  target->retries_used);
    return NULL;
}

/* Exports the final allocation results into a CSV file for reporting. */
static void export_csv(void)
{
    FILE *f = fopen(CSV_FILE, "w");
    if (!f) {
        fprintf(stderr, YLW "[WARN] Cannot open %s: %s\n" RST,
                CSV_FILE, strerror(errno));
        return;
    }
    fprintf(f, "Num,CourseName,TotalSeats,Enrolled,Remaining\n");
    for (int i = 0; i < cfg.num_courses; i++) {
        fprintf(f, "%d,\"%s\",%d,%d,%d\n",
                courses[i].id, courses[i].name,
                courses[i].total_seats,
                courses[i].enrolled,
                courses[i].available_seats);
    }
    fclose(f);
    printf(GRN "  CSV report written to: %s\n" RST, CSV_FILE);
}

/* Verifies correctness constraints: no negative seats and enrolled + remaining matches total seats. */
static void verify_correctness(void)
{
    int pass = 1;
    printf("\n" BOLD "── Correctness Verification ──────────────────────────────\n" RST);

    for (int i = 0; i < cfg.num_courses; i++) {
        Course *c = &courses[i];
        if (c->available_seats < 0) {
            printf(RED "  [FAIL] %s: available_seats = %d (NEGATIVE!)\n" RST,
                   c->name, c->available_seats);
            pass = 0;
        }
        if (c->enrolled + c->available_seats != c->total_seats) {
            printf(RED "  [FAIL] %s: enrolled(%d) + remaining(%d) != total(%d)\n" RST,
                   c->name, c->enrolled, c->available_seats, c->total_seats);
            pass = 0;
        }
    }

    int processed = total_success + total_failure;
    if (processed > cfg.num_students) {
        printf(RED "  [FAIL] processed(%d) > num_students(%d)\n" RST,
               processed, cfg.num_students);
        pass = 0;
    }

    if (pass)
        printf(GRN "  [PASS] All correctness checks passed.\n" RST);
    printf(BOLD "──────────────────────────────────────────────────────────\n" RST);

    if (log_fp) {
        fprintf(log_fp, "\n=== CORRECTNESS: %s ===\n", pass ? "PASS" : "FAIL");
        fflush(log_fp);
    }
}

/* Prints the final formatted output table showing course capacities and enrollment statuses. */
static void print_final_report(void)
{
    printf("\n" BOLD
           "══════════════════════════════════════════════════════════\n"
           "            FINAL REGISTRATION REPORT                    \n"
           "══════════════════════════════════════════════════════════\n"
           RST);

    printf(CYN "%-32s %7s %8s %9s %6s\n",
           "Course Name", "Total", "Enrolled", "Remaining", "Fill%");
    printf("──────────────────────────────────────────────────────────\n"
           RST);

    for (int i = 0; i < cfg.num_courses; i++) {
        double pct = 100.0 * courses[i].enrolled / courses[i].total_seats;
        printf("%-32s %7d %8d %9d %5.1f%%\n",
               courses[i].name,
               courses[i].total_seats,
               courses[i].enrolled,
               courses[i].available_seats,
               pct);
    }

    printf(BOLD "\n──────────────────────────────────────────────────────────\n");
    printf("  Mode             : %s\n",
           cfg.stress_mode ? YLW "STRESS" RST BOLD : "Normal");
    printf("  Total Students   : %d\n",   cfg.num_students);
    printf(GRN "  Successful       : %d\n" RST BOLD, total_success);
    printf(RED "  Failed           : %d\n" RST BOLD, total_failure);
    printf(YLW "  Used Retry       : %d\n" RST BOLD, total_retried);
    printf(BOLD "══════════════════════════════════════════════════════════\n"
           RST);

    if (log_fp) {
        fprintf(log_fp, "\n=== FINAL REPORT ===\n");
        fprintf(log_fp, "Students: %d | Success: %d | Failed: %d | Retried: %d\n",
                cfg.num_students, total_success, total_failure, total_retried);
        fflush(log_fp);
    }

    if (cfg.export_csv) export_csv();
}

/* Prints the help text and usage options to the terminal and exits. */
static void print_usage(const char *prog)
{
    printf(
        BOLD "University Course Registration System\n\n" RST
        "Usage: %s [OPTIONS]\n\n"
        "Options:\n"
        "  -s, --students N    Number of student threads (1-%d, default 100)\n"
        "  -c, --courses  N    Number of courses       (1-%d, default 15)\n"
        "  -r, --retries  N    Max retries per student  (default 2)\n"
        "      --stress        Enable stress-test mode (200 students, random seats)\n"
        "      --seed   N      PRNG seed for reproducibility\n"
        "      --csv           Export final report to CSV\n"
        "      --scenario      Run the specific 3-course, 10-student scenario\n"
        "  -h, --help          Show this help\n\n"
        "Examples:\n"
        "  %s                          # default run\n"
        "  %s -s 150 -c 12 --csv       # 150 students, 12 courses, export CSV\n"
        "  %s --stress --seed 42       # stress test with fixed seed\n\n",
        prog, MAX_STUDENTS, MAX_COURSES, prog, prog, prog);
    exit(EXIT_SUCCESS);
}

/* Parses command line arguments using getopt_long, setting configuration overrides like stress/demo/scenario modes. */
static void parse_args(int argc, char **argv)
{
    static const struct option long_opts[] = {
        { "students", required_argument, NULL, 's' },
        { "courses",  required_argument, NULL, 'c' },
        { "retries",  required_argument, NULL, 'r' },
        { "stress",   no_argument,       NULL,  1  },
        { "seed",     required_argument, NULL,  2  },
        { "csv",      no_argument,       NULL,  3  },
        { "demo",     no_argument,       NULL,  4  },
        { "scenario", no_argument,       NULL,  5  },
        { "help",     no_argument,       NULL, 'h' },
        { NULL, 0, NULL, 0 }
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "s:c:r:h", long_opts, NULL)) != -1) {
        switch (opt) {
            case 's':
                cfg.num_students = atoi(optarg);
                if (cfg.num_students < 1 || cfg.num_students > MAX_STUDENTS)
                    die("--students must be 1-%d", MAX_STUDENTS);
                break;
            case 'c':
                cfg.num_courses = atoi(optarg);
                if (cfg.num_courses < 1 || cfg.num_courses > MAX_COURSES)
                    die("--courses must be 1-%d", MAX_COURSES);
                break;
            case 'r':
                cfg.max_retries = atoi(optarg);
                if (cfg.max_retries < 0)
                    die("--retries must be >= 0");
                break;
            case 1:  cfg.stress_mode = 1; break;
            case 2:  cfg.seed        = (unsigned)atoi(optarg); break;
            case 3:  cfg.export_csv  = 1; break;
            case 4:  cfg.demo_mode   = 1; break;
            case 5:  cfg.scenario_mode = 1; break;
            case 'h': print_usage(argv[0]); break;
            default:  print_usage(argv[0]);
        }
    }

    if (cfg.stress_mode) {
        cfg.num_students = MAX_STUDENTS;
        cfg.max_retries  = 3;
    }
    if (cfg.scenario_mode) {
        cfg.num_students = 10;
        cfg.num_courses  = 3;
    }
}

/* Main execution point. Initializes structs, creates threads, waits for completion, and performs post-run checks. */
int main(int argc, char **argv)
{
    parse_args(argc, argv);

    unsigned int seed = cfg.seed ? cfg.seed : (unsigned)time(NULL);
    srand(seed);

    struct sigaction sa = { .sa_handler = handle_sigint };
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGINT, &sa, NULL) == -1)
        die("sigaction");

    printf(BOLD CYN
        "\n╔══════════════════════════════════════════════════════════╗\n"
        "║     University Course Registration System               ║\n"
        "╚══════════════════════════════════════════════════════════╝\n\n"
        RST);
    if (cfg.demo_mode)
        printf("  Mode      : " MAG "DEMO (strict priority proof)" RST "\n");
    else if (cfg.stress_mode)
        printf("  Mode      : " YLW "STRESS" RST "\n");
    else
        printf("  Mode      : Normal\n");
    printf("  Students  : %d\n",   cfg.num_students);
    printf("  Courses   : %d\n",   cfg.num_courses);
    printf("  Retries   : %d\n",   cfg.max_retries);
    printf("  PRNG Seed : %u\n\n", seed);

    init_courses();
    init_students();
    pqueue_init(&pqueue);

    log_fp = fopen(LOG_FILE, "w");
    if (!log_fp) die("fopen(%s)", LOG_FILE);
    fprintf(log_fp,
            "University Course Registration System\n"
            "Seed=%u  Students=%d  Courses=%d  Retries=%d\n\n",
            seed, cfg.num_students, cfg.num_courses, cfg.max_retries);

    pthread_t threads[MAX_STUDENTS];

    if (cfg.demo_mode) {

        printf(MAG "  [DEMO] Pre-loading priority queue...\n" RST);
        for (int i = 0; i < cfg.num_students; i++)
            pqueue_enqueue(&pqueue, &students[i]);
        printf(MAG "  [DEMO] Queue loaded (%d entries). "
               "Spawning worker threads...\n\n" RST, cfg.num_students);

        for (int i = 0; i < cfg.num_students; i++) {
            int rc = pthread_create(&threads[i], NULL, worker_thread, NULL);
            if (rc) {
                fprintf(stderr, RED "[ERR] pthread_create worker %d: %s\n" RST,
                        i + 1, strerror(rc));
                threads[i] = 0;
            }
        }
    } else {
        printf(YLW "  [*] Spawning %d student threads...\n\n" RST,
               cfg.num_students);
        for (int i = 0; i < cfg.num_students; i++) {
            int rc = pthread_create(&threads[i], NULL,
                                    student_thread, &students[i]);
            if (rc) {
                fprintf(stderr, RED "[ERR] pthread_create student %d: %s\n" RST,
                        i + 1, strerror(rc));
                threads[i] = 0;
                pthread_mutex_lock(&stats_mutex);
                total_failure++;
                pthread_mutex_unlock(&stats_mutex);
            }
        }
    }

    for (int i = 0; i < cfg.num_students; i++) {
        if (threads[i]) {
            int rc = pthread_join(threads[i], NULL);
            if (rc)
                fprintf(stderr, YLW "[WARN] pthread_join student %d: %s\n" RST,
                        i + 1, strerror(rc));
        }
    }

    StudentRequest *leftover;
    while ((leftover = pqueue_dequeue(&pqueue)) != NULL) {
        int ok = try_register(leftover, leftover->course_index);
        pthread_mutex_lock(&stats_mutex);
        if (ok) total_success++; else total_failure++;
        pthread_mutex_unlock(&stats_mutex);
        log_event("[DRAIN] Student %3d  %s\n",
                  leftover->student_id, ok ? "✓" : "✗");
    }

    verify_correctness();
    print_final_report();

    for (int i = 0; i < cfg.num_courses; i++)
        pthread_mutex_destroy(&courses[i].mutex);

    pthread_mutex_destroy(&log_mutex);
    pthread_mutex_destroy(&stats_mutex);
    pqueue_destroy(&pqueue);

    if (log_fp) {
        fclose(log_fp);
        printf("  Log written to : %s\n\n", LOG_FILE);
    }

    if (shutdown_flag)
        printf(YLW "\n  [!] Interrupted by user (SIGINT).\n\n" RST);

    return EXIT_SUCCESS;
}
