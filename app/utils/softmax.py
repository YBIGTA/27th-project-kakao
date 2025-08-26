
import math
from typing import List

def softmax_clamped(x: List[float], temperature: float = 1.0) -> List[float]:
    if temperature <= 0:
        temperature = 1e-6
    # numerical stability
    m = max(x) if x else 0.0
    exps = [math.exp((xi - m) / temperature) for xi in x]
    s = sum(exps) if exps else 1.0
    return [e / s for e in exps]
