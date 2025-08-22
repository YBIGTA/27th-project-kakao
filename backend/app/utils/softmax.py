import math, numpy as np

def softmax_with_temp(values, T=1.2, eps=1e-12):
    arr = np.array(values, dtype=float)
    if arr.size == 0:
        return arr
    m = np.max(arr)
    ex = np.exp((arr - m) / max(T, eps))
    s = ex.sum() + eps
    return ex / s

def normalized_entropy(P):
    P = np.array(P, dtype=float)
    K = max(1, len(P))
    eps = 1e-12
    H = -(P * np.log(P + eps)).sum()
    return H / math.log(K) if K > 1 else 0.0

def auto_temperature(R_values, h_target, Tmin=0.6, Tmax=3.0, iters=20):
    def h(T):
        P = softmax_with_temp(R_values, T=T)
        return normalized_entropy(P)
    lo, hi = Tmin, Tmax
    for _ in range(iters):
        mid = 0.5*(lo+hi)
        hmid = h(mid)
        if hmid < h_target:
            lo = mid  # flatten more
        else:
            hi = mid  # sharpen
    return 0.5*(lo+hi)
