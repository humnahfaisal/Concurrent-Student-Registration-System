# Concurrent-Student-Registration-System
 Multithreaded course registration system in C simulating concurrent seat allocation with priority scheduling, stress testing, and CSV export
 
## Features
- Multithreaded seat allocation
- Priority scheduling
- Stress test mode
- Demo and scenario modes
- CSV export of results
- Automatic correctness checks

## Tech Stack
C, GCC, POSIX Threads

## How to Run
gcc main.c -o registration
./registration

## Options
./registration -s 50 -c 5      # Custom students & courses
./registration --stress         # Stress test mode
./registration --demo           # Demo mode
./registration --scenario       # Scenario mode
./registration --csv            # Export to CSV
./registration --help           # View all options
