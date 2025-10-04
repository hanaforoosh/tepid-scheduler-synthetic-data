import argparse, os, time
from multiprocessing import Pool, cpu_count
import random

def _mc(n):
    c = 0
    rnd = random.Random(os.getpid()*int(time.time()))
    for _ in range(n):
        x = rnd.random()
        y = rnd.random()
        if x*x + y*y <= 1.0:
            c += 1
    return c

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--samples", type=int, default=10_000_000)
    p.add_argument("--procs", type=int, default=0)
    args = p.parse_args()
    procs = args.procs if args.procs > 0 else cpu_count()
    per = args.samples // procs
    rem = args.samples - per*procs
    tasks = [per]*procs
    if rem > 0:
        tasks[0] += rem
    t0 = time.time()
    with Pool(processes=procs) as pool:
        hits = sum(pool.map(_mc, tasks))
    pi = 4.0 * hits / float(args.samples)
    t1 = time.time()
    print(f"samples={args.samples} procs={procs} pi={pi:.8f} elapsed_s={t1-t0:.3f}")

if __name__ == "__main__":
    main()
