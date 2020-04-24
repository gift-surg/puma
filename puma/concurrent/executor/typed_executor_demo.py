from typing import Optional

from puma.concurrent.executor.typed_executor import TypedThreadPoolExecutor


def my_method(a: Optional[int] = None, b: Optional[int] = None, c: Optional[int] = None) -> str:
    result = f"MyMethod got {a} - {b} - {c}"
    print(result)
    return result


if __name__ == '__main__':
    with TypedThreadPoolExecutor() as ex:
        print("submit")
        future1 = ex.submit(my_method)
        future2 = ex.submit(my_method, 1)
        future3 = ex.submit(my_method, 1, 2, 3)
        print("Submit results: ", future1.result(), future2.result(), future3.result())

        # Note: The following calls are equivalent
        print("map_tuple")
        results = ex.map_tuple(my_method, [(1,), (2, 3), (4, 5, 6)])
        print("Map_tuple results: ", list(results))

        print("map")
        results = ex.map(my_method, [1, 2, 4], [None, 3, 5], [None, None, 6])
        print("Map results: ", list(results))
