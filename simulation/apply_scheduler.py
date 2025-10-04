import argparse, numpy as np, pandas as pd, yaml

def load_yaml(p):
    with open(p,"r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def ema_rate(times, alpha, t_now, last_t, last_rate):
    if last_t is None:
        return last_rate
    dt = t_now - last_t
    if dt <= 0:
        return last_rate
    inst_rate = 1.0 / dt
    return alpha*inst_rate + (1.0-alpha)*last_rate

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in_csv", required=True)
    p.add_argument("--out_csv", required=True)
    p.add_argument("--datasets_cfg", default="simulation/config/datasets.yaml")
    p.add_argument("--policy_cfg", default="simulation/config/policy.yaml")
    args = p.parse_args()

    df = pd.read_csv(args.in_csv)
    df = df.sort_values("arrival_time").reset_index(drop=True)

    dcfg = load_yaml(args.datasets_cfg)
    pcfg = load_yaml(args.policy_cfg)

    theta_l = pcfg["thresholds"]["theta_l"]
    theta_h = pcfg["thresholds"]["theta_h"]
    window = pcfg["window_sec"]
    capacity = pcfg["capacity"]

    dataset_name = df["dataset"].iloc[0]
    rng = np.random.RandomState(123)

    tlr = dcfg[dataset_name]["tepid_load_time_range_s"]
    tepid_load = lambda n: rng.uniform(tlr[0], tlr[1], n)

    running_until = []
    last_arrival = {}
    rate_ema = {}
    alpha = 2.0/(1.0+window)  # simple mapping to EMA weight

    chosen_state = []
    eff_load = []
    start_time = []
    completion_sched = []
    latency_sched = []

    for idx,row in df.iterrows():
        fid = int(row["function_id"])
        t = float(row["arrival_time"])
        last_t = last_arrival.get(fid, None)
        r_prev = rate_ema.get(fid, 0.0)
        r_now = ema_rate(None, alpha, t, last_t, r_prev)
        rate_ema[fid] = r_now
        last_arrival[fid] = t

        if r_now >= theta_h:
            st = "Warm"
        elif r_now > theta_l:
            st = "Tepid"
        else:
            st = "Cold"

        if st == "Warm":
            ltime = 0.0
        elif st == "Tepid":
            ltime = tepid_load(1)[0]
        else:
            ltime = float(row["dependency_load_time"])

        while len(running_until) > 0 and min(running_until) <= t:
            m = np.argmin(running_until)
            running_until.pop(m)

        if len(running_until) < capacity:
            s = t
        else:
            s = min(running_until)

        dur = float(row["function_duration"])
        c = s + ltime + dur

        running_until.append(c)

        chosen_state.append(st)
        eff_load.append(ltime)
        start_time.append(s)
        completion_sched.append(c)
        latency_sched.append(c - t)

    df["chosen_state"] = chosen_state
    df["effective_load_time"] = eff_load
    df["start_time"] = start_time
    df["completion_time_scheduled"] = completion_sched
    df["latency_scheduled"] = latency_sched

    df.to_csv(args.out_csv, index=False)

if __name__ == "__main__":
    main()
