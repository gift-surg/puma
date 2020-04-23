from time import sleep
from typing import Optional

DELAY_SLOW = 1
DELAY_FAST = DELAY_SLOW / 100


def method0() -> str:
    sleep(DELAY_SLOW)
    return f"method0"


def method1(a: int) -> str:
    sleep(DELAY_SLOW)
    return f"method1 - {a}"


def method2(a: int, b: int) -> str:
    sleep(DELAY_SLOW)
    return f"method2 - {a} - {b}"


def method3(a: int, b: int, c: int) -> str:
    sleep(DELAY_SLOW)
    return f"method3 - {a} - {b} - {c}"


def method4(a: int, b: int, c: int, d: int) -> str:
    sleep(DELAY_SLOW)
    return f"method4 - {a} - {b} - {c} - {d}"


def method5(a: int, b: int, c: int, d: int, e: int) -> str:
    sleep(DELAY_SLOW)
    return f"method5 - {a} - {b} - {c} - {d} - {e}"


def method6(a: int, b: int, c: int, d: int, e: int, f: int) -> str:
    sleep(DELAY_SLOW)
    return f"method6 - {a} - {b} - {c} - {d} - {e} - {f}"


def method7(a: int, b: int, c: int, d: int, e: int, f: int, g: int) -> str:
    sleep(DELAY_SLOW)
    return f"method7 - {a} - {b} - {c} - {d} - {e} - {f} - {g}"


def method8(a: int, b: int, c: int, d: int, e: int, f: int, g: int, h: int) -> str:
    sleep(DELAY_SLOW)
    return f"method8 - {a} - {b} - {c} - {d} - {e} - {f} - {g} - {h}"


def method9(a: int, b: int, c: int, d: int, e: int, f: int, g: int, h: int, i: int) -> str:
    sleep(DELAY_SLOW)
    return f"method9 - {a} - {b} - {c} - {d} - {e} - {f} - {g} - {h} - {i}"


def method10(a: int, b: int, c: int, d: int, e: int, f: int, g: int, h: int, i: int, j: int) -> str:
    sleep(DELAY_SLOW)
    return f"method10 - {a} - {b} - {c} - {d} - {e} - {f} - {g} - {h} - {i} - {j}"


def fast_method(a: Optional[int] = None, b: Optional[int] = None, c: Optional[int] = None) -> str:
    sleep(DELAY_FAST)
    return f"fast_method - {a} - {b} - {c}"
