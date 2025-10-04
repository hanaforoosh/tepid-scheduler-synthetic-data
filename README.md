# Synthetic Workloads for Tepid-Aware Scheduler

This repository provides synthetic traces for evaluating the Tepid-Aware Container Scheduler. Arrival processes follow a Poisson model. Completion time is defined as arrival_time + dependency_load_time + function_duration; latency equals completion_time − arrival_time. Dataset A simulates heavy-dependency functions; Dataset B represents light-dependency functions.

## How to reproduce
```
pip install -r requirements.txt
python simulation/generate_traces.py --scenario S2 --dataset A_heavy --lambda 120 --n 10000 --seed 42 --out traces/S2_full.csv
```
The CSV schema and parameter ranges match Section 6 of the paper.
