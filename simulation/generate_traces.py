import argparse, json, numpy as np, pandas as pd, yaml
def load_yaml(p):
    with open(p,"r",encoding="utf-8") as f:
        return yaml.safe_load(f)
def sample_uniform(a,b,n,rs):
    return rs.uniform(a,b,n)
def generate_constant(lambda_rate,n_requests,rs):
    inter = rs.exponential(1.0/lambda_rate, n_requests)
    arr = np.cumsum(inter)
    return arr, inter
def generate_burst(low_lambda,high_lambda,low_dur_s,high_dur_s,total_dur_s,rs):
    t=0.0
    arr=[]
    inter=[]
    on_high=False
    while t<total_dur_s:
        lam = high_lambda if on_high else low_lambda
        dur = high_dur_s if on_high else low_dur_s
        end = t+dur
        while True:
            x = rs.exponential(1.0/lam)
            if t + x > end: break
            t = t + x
            arr.append(t)
            inter.append(x)
        t = end
        on_high = not on_high
    return np.array(arr), np.array(inter)
def attach_service(arr, dataset_name, datasets_cfg, rs):
    cfg = datasets_cfg[dataset_name]
    n = len(arr)
    dl = sample_uniform(cfg["dependency_load_time_range_s"][0], cfg["dependency_load_time_range_s"][1], n, rs)
    fd = sample_uniform(cfg["function_duration_range_s"][0], cfg["function_duration_range_s"][1], n, rs)
    comp = arr + dl + fd
    lat = comp - arr
    return dl, fd, comp, lat
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", required=True)
    p.add_argument("--dataset", required=True, choices=["A_heavy","B_light"])
    p.add_argument("--lambda", dest="lam", type=float)
    p.add_argument("--n", dest="n", type=int)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", required=True)
    p.add_argument("--scenarios_cfg", default="simulation/config/scenarios.yaml")
    p.add_argument("--datasets_cfg", default="simulation/config/datasets.yaml")
    args = p.parse_args()
    rs = np.random.RandomState(args.seed)
    sc = load_yaml(args.scenarios_cfg).get(args.scenario, {})
    if sc.get("profile","constant") == "burst":
        arr, inter = generate_burst(sc["low_lambda"], sc["high_lambda"], sc["low_dur_s"], sc["high_dur_s"], sc["total_dur_s"], rs)
    else:
        lam = args.lam if args.lam is not None else sc.get("lambda", 20.0)
        n = args.n if args.n is not None else sc.get("n_requests", 1000)
        arr, inter = generate_constant(lam, n, rs)
    datasets_cfg = load_yaml(args.datasets_cfg)
    dl, fd, comp, lat = attach_service(arr, args.dataset, datasets_cfg, rs)
    n = len(arr)
    fid = np.arange(n)
    df = pd.DataFrame({
        "function_id": fid,
        "scenario": [args.scenario]*n,
        "dataset": [args.dataset]*n,
        "arrival_time": arr,
        "inter_arrival": inter if len(inter)==n else pd.Series(inter).reindex(range(n), fill_value=np.nan),
        "dependency_load_time": dl,
        "function_duration": fd,
        "completion_time": comp,
        "latency": lat
    })
    df.to_csv(args.out, index=False)
if __name__ == "__main__":
    main()
