# Day 07 — Demo Guide
## Advanced Python: Decorators, Metaprogramming, Async, Parallel Processing, Testing & Logging

This document is a professional demo guide covering advanced Python topics used in realistic ETL/pipeline systems. Each section contains a clear objective, step-by-step implementation, runnable code blocks, explanations of key parts, and expected output behavior.

---

**Usage note:** Copy each code block into a `.py` file and run it to experiment. The examples aim to be production-lean while remaining simple to run locally.

---

## 1. Function Decorators (Logging, Timing, Retry)

Objective
- Demonstrate how to implement and compose function decorators for logging, timing, and retry behavior. Apply them to a simulated API fetch used in ingestion pipelines.

Step-by-step implementation
1. Implement a `logging_decorator` that adds structured logs using the `logging` module.
2. Implement a `timing_decorator` that measures execution time and logs it.
3. Implement a `retry_decorator` with configurable max attempts and configurable exceptions to retry.
4. Stack the decorators and demonstrate behavior on a flaky API fetch simulation.

Full code
```python
# demo_decorators.py
import logging
import time
import random
from functools import wraps

logger = logging.getLogger("demo.decorators")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def logging_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info("Calling %s with args=%s kwargs=%s", func.__name__, args, kwargs)
        result = func(*args, **kwargs)
        logger.info("%s returned %s", func.__name__, type(result))
        return result

    return wrapper


def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - start
            logger.info("%s took %.3f s", func.__name__, elapsed)

    return wrapper


def retry_decorator(max_attempts=3, exceptions=(Exception,)):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    logger.debug("Attempt %s for %s", attempt, func.__name__)
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    logger.warning("%s failed on attempt %s: %s", func.__name__, attempt, exc)
                    if attempt == max_attempts:
                        logger.error("Max attempts reached for %s. Re-raising.", func.__name__)
                        raise
                    time.sleep(0.1 * attempt)

        return wrapper

    return decorator


@logging_decorator
@timing_decorator
@retry_decorator(max_attempts=4, exceptions=(RuntimeError,))
def simulated_api_fetch(resource_id: int) -> dict:
    """Simulated flaky API function used in an ETL ingestion stage.

    - Randomly fails to simulate network/API instability.
    - Sleeps to simulate IO latency.
    """
    # Simulate network latency
    time.sleep(random.uniform(0.05, 0.2))
    # Simulate flakiness
    if random.random() < 0.4:
        raise RuntimeError(f"Transient error fetching resource {resource_id}")
    return {"id": resource_id, "payload": f"data-for-{resource_id}"}


if __name__ == "__main__":
    # Run several fetches to demonstrate retries and timing
    for i in range(1, 6):
        try:
            data = simulated_api_fetch(i)
            print("Fetched:", data)
        except Exception as e:
            print("Final failure for id", i, "->", e)
```

Explanation of key parts
- Decorator order: decorators closest to the function are applied first; at call time the outermost wrapper is executed first. In our stack, `retry_decorator` wraps `simulated_api_fetch` first, `timing_decorator` wraps the result, and `logging_decorator` wraps the timing wrapper. This results in logging wrapping timing, wrapping retry logic.
- Closures: Each decorator returns `wrapper` which captures `func` from the outer scope — that's a closure.
- `functools.wraps`: Preserves the wrapped function's `__name__`, `__doc__`, and metadata which helps debugging and introspection.

Expected output behavior
- INFO logs for each call and result type.
- Timing logs showing duration.
- Warnings on retry attempts and a final error if max attempts reached.
- Printed fetched data for successful requests.

Sample output (approximate):
```
2026-02-23 12:00:00 INFO demo.decorators: Calling simulated_api_fetch with args=(1,) kwargs={}
2026-02-23 12:00:00 INFO demo.decorators: simulated_api_fetch took 0.123 s
2026-02-23 12:00:00 INFO demo.decorators: simulated_api_fetch returned <class 'dict'>
Fetched: {'id': 1, 'payload': 'data-for-1'}
... (warnings for retries if failures occur)
```

---

## 2. Class-Based Decorators

Objective
- Build a stateful class-based decorator implementing retry behavior with counters and optional logging, and apply it to a data loading function.

Step-by-step implementation
1. Implement a `Retry` class that accepts `max_attempts` and optionally a logger.
2. Implement `__call__` to make instances usable as decorators.
3. Demonstrate on a data loader that simulates intermittent failures.

Full code
```python
# demo_class_decorator.py
import time
import random
import logging
from functools import wraps

logger = logging.getLogger("demo.class_decorator")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class Retry:
    def __init__(self, max_attempts=3, exceptions=(Exception,), backoff=0.1):
        self.max_attempts = max_attempts
        self.exceptions = exceptions
        self.backoff = backoff
        self.stats = {"calls": 0, "retries": 0}

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            self.stats["calls"] += 1
            last_exc = None
            for attempt in range(1, self.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except self.exceptions as exc:
                    last_exc = exc
                    self.stats["retries"] += 1
                    logger.warning("Attempt %s failed for %s: %s", attempt, func.__name__, exc)
                    if attempt == self.max_attempts:
                        logger.error("Raising after %s attempts", attempt)
                        raise
                    time.sleep(self.backoff * attempt)

        return wrapper


@Retry(max_attempts=4, backoff=0.05)
def load_data_chunk(chunk_id: int) -> list:
    """Simulated data loader for an ETL stage."""
    if random.random() < 0.5:
        raise ConnectionError(f"Failed to load chunk {chunk_id}")
    return [{"chunk": chunk_id, "value": i} for i in range(3)]


if __name__ == "__main__":
    for i in range(1, 6):
        try:
            print("Loaded:", load_data_chunk(i))
        except Exception as e:
            print("Chunk failed permanently:", i, e)

    # Inspect decorator stats
    # Note: To access `stats` you'd need to keep a reference to the Retry instance before decoration.
```

Explanation of key parts
- `__call__`: Instances of `Retry` are callable; when used as a decorator, `__call__` receives the function to wrap and returns a wrapper function.
- State: `self.stats` holds state across decorated calls, which is something function-based decorators cannot easily do without external storage.
- When to prefer class-based decorators: when decorator needs to maintain state or expose introspection API (counters, metrics, caches).

Expected output behavior
- INFO/WARNING logs for retry attempts.
- Successful `load_data_chunk` returns printed lists; permanent failures after retries will print the exception.

---

## 3. Dynamic Class Creation (Meta-programming)

Objective
- Demonstrate runtime generation of connector classes for an ETL platform using `type()` and a `BaseConnector` contract. Show how this pattern supports plugin-style connectors.

Step-by-step implementation
1. Define a `BaseConnector` that specifies `connect`, `fetch`, and `close`.
2. Use `type()` to generate connector classes with different defaults/behaviors.
3. Instantiate and use connectors the same way — demonstrating runtime extensibility.

Full code
```python
# demo_dynamic_connectors.py
from typing import Any


class BaseConnector:
    """Base contract for connectors used by the pipeline."""
    name = "base"

    def __init__(self, **cfg):
        self.cfg = cfg

    def connect(self) -> None:
        raise NotImplementedError

    def fetch(self) -> Any:
        raise NotImplementedError

    def close(self) -> None:
        pass


def make_connector_class(name: str, fetch_impl):
    """Create a connector class at runtime.

    - `name`: class name
    - `fetch_impl`: function implementing fetch(self)
    """

    attrs = {
        "name": name,
        "connect": lambda self: print(f"[{name}] connected with {self.cfg}"),
        "fetch": fetch_impl,
        "close": lambda self: print(f"[{name}] closed"),
    }

    return type(f"{name}Connector", (BaseConnector,), attrs)


def mysql_fetch(self):
    return [{"id": 1, "user": "alice"}]


def api_fetch(self):
    return [{"id": "a1", "value": "sample"}]


MySQLConnector = make_connector_class("MySQL", mysql_fetch)
APIConnector = make_connector_class("API", api_fetch)


if __name__ == "__main__":
    connectors = [MySQLConnector(host="db.prod"), APIConnector(base_url="https://api")]
    for c in connectors:
        c.connect()
        print("Fetched:", c.fetch())
        c.close()
```

Explanation of key parts
- `type(name, bases, dict)`: Creates a class object dynamically. `name` is the new class name, `bases` is a tuple of base classes, and `dict` contains attributes and methods.
- Runtime class generation is useful in plugin systems, ORMs (model factories), and adapter factories where connectors or models vary by configuration or external schemas.

Expected output behavior
- Connectors print connection message, fetched data, and closed message. All connectors behave consistently because they share `BaseConnector` semantics.

---

## 4. multiprocessing vs concurrent.futures

Objective
- Compare sequential CPU-bound processing to parallel processing via `ProcessPoolExecutor`. Measure time differences to explain the GIL and why multiprocessing helps CPU-bound tasks.

Step-by-step implementation
1. Implement a CPU-bound function (e.g., calculate large Fibonacci via iterative heavy loop or large prime check).
2. Run N tasks sequentially and measure time.
3. Run N tasks with `ProcessPoolExecutor` and measure time.

Full code
```python
# demo_multiprocessing_vs_concurrent.py
import time
from concurrent.futures import ProcessPoolExecutor


def heavy_work(n: int) -> int:
    # CPU-bound: sum of prime checks up to a limit based on n
    count = 0
    limit = 20000 + (n % 5) * 1000
    for i in range(2, limit):
        is_prime = True
        for j in range(2, int(i**0.5) + 1):
            if i % j == 0:
                is_prime = False
                break
        if is_prime:
            count += 1
    return count


def run_sequential(tasks):
    start = time.perf_counter()
    results = [heavy_work(t) for t in tasks]
    return time.perf_counter() - start, results


def run_process_pool(tasks, workers=4):
    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(heavy_work, tasks))
    return time.perf_counter() - start, results


if __name__ == "__main__":
    tasks = list(range(8))
    t_seq, _ = run_sequential(tasks)
    print(f"Sequential time: {t_seq:.2f}s")

    t_proc, _ = run_process_pool(tasks, workers=4)
    print(f"ProcessPool time (4 workers): {t_proc:.2f}s")

    print("Note: CPU-bound tasks benefit from multiprocessing because the GIL is bypassed by separate processes.")
```

Explanation of key parts
- GIL: In CPython, the Global Interpreter Lock prevents multiple native threads executing Python bytecode concurrently. For CPU-bound work, threads are limited — multiprocessing creates separate processes, each with its own Python interpreter and GIL.
- Serialization overhead: Process pools require pickling arguments and return values; for large objects this cost may dominate and reduce parallel benefits.

Expected output behavior
- Sequential time will be larger; ProcessPoolExecutor will usually be faster proportional to available CPU cores, minus serialization overhead.

---

## 5. asyncio for API-heavy Workloads

Objective
- Demonstrate using `asyncio` to concurrently run many I/O-bound tasks (simulated HTTP fetches), using `asyncio.gather()`.

Step-by-step implementation
1. Implement an async `fetch` that simulates network IO with `asyncio.sleep()`.
2. Launch many fetches concurrently with `gather()` and measure elapsed time.

Full code
```python
# demo_asyncio_fetch.py
import asyncio
import random
import time


async def fetch(url: str) -> dict:
    delay = random.uniform(0.05, 0.25)
    await asyncio.sleep(delay)
    return {"url": url, "status": 200, "delay": delay}


async def main(urls):
    start = time.perf_counter()
    results = await asyncio.gather(*(fetch(u) for u in urls))
    elapsed = time.perf_counter() - start
    print(f"Fetched {len(results)} urls in {elapsed:.3f}s")
    return results


if __name__ == "__main__":
    urls = [f"https://example.com/resource/{i}" for i in range(50)]
    results = asyncio.run(main(urls))
    print(results[:3])
```

Explanation of key parts
- Event loop: `asyncio.run()` creates and runs the event loop; `await` yields control, allowing other coroutines to run.
- Blocking code: If you run CPU-bound or blocking calls inside `fetch` (e.g., `time.sleep()`), the event loop is blocked and concurrency is lost. Use thread/process pools or move CPU work out of the event loop.

Expected output behavior
- Total elapsed time will be close to the maximum simulated delay, not the sum — demonstrating concurrency.

---

## 6. Hybrid Concurrency Strategy

Objective
- Build a small pipeline that performs asynchronous I/O-bound fetches, then applies CPU-heavy transformations in a `ProcessPoolExecutor`, then aggregates results.

Step-by-step implementation
1. Async fetch stage: concurrently fetch simulated items.
2. Offload CPU-bound transform to process pool using `loop.run_in_executor` or `ProcessPoolExecutor` with `asyncio`.
3. Aggregate transformed results.

Full code
```python
# demo_hybrid_pipeline.py
import asyncio
import random
import time
from concurrent.futures import ProcessPoolExecutor


async def fetch_item(i):
    await asyncio.sleep(random.uniform(0.01, 0.05))
    return {"id": i, "value": i * 100}


def cpu_transform(item):
    # Simulate heavy calculation
    s = 0
    for _ in range(20000):
        s += (_ * (_ % (item["id"] + 1))) % 7
    return {"id": item["id"], "transformed": item["value"] + s}


async def main():
    # Stage 1: async fetch
    items = await asyncio.gather(*(fetch_item(i) for i in range(20)))

    # Stage 2: CPU-bound transformation in a process pool
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=4) as pool:
        tasks = [loop.run_in_executor(pool, cpu_transform, it) for it in items]
        transformed = await asyncio.gather(*tasks)

    # Stage 3: aggregation
    total = sum(x["transformed"] for x in transformed)
    print("Aggregated total:", total)


if __name__ == "__main__":
    start = time.perf_counter()
    asyncio.run(main())
    print("Elapsed:", time.perf_counter() - start)
```

Explanation of key parts
- Clear separation of concerns: I/O-bound work stays async; CPU-bound work is offloaded to processes.
- Architecture: This hybrid model ensures efficient resource usage — event loop keeps IO tasks concurrent while CPU work uses multiple cores.

Expected output behavior
- Program prints aggregated result and elapsed time; CPU work benefits from multiprocessing, while fetches remain non-blocking.

---

## 7. Custom Exceptions & Structured Error Handling

Objective
- Demonstrate domain-specific exceptions for validation and show how orchestration code treats them differently from infrastructure errors.

Step-by-step implementation
1. Define `ValidationError`, `DependencyError` and `TransientError`.
2. Raise them in validation and fetching code.
3. Orchestrate handling: retry transient errors, abort on validation errors, and escalate dependency errors.

Full code
```python
# demo_exceptions.py
class PipelineError(Exception):
    pass


class ValidationError(PipelineError):
    pass


class DependencyError(PipelineError):
    pass


class TransientError(PipelineError):
    pass


def validate_record(record):
    if "id" not in record:
        raise ValidationError("Missing id")
    if record.get("value") is None:
        raise DependencyError("Dependent enrichment missing")
    if record.get("id") == "temp-fail":
        raise TransientError("Temporary validation hiccup")
    return True


def run_pipeline(records):
    for r in records:
        try:
            validate_record(r)
            print("Processed", r)
        except ValidationError as e:
            print("Validation failed, skipping record:", e)
        except TransientError as e:
            print("Transient; should retry but skipping in demo:", e)
        except DependencyError as e:
            print("External dependency issue — escalate:", e)


if __name__ == "__main__":
    inputs = [{"id": 1, "value": 10}, {"value": 2}, {"id": "temp-fail", "value": 3}, {"id": 4}]
    run_pipeline(inputs)
```

Explanation of key parts
- Avoid catching generic `Exception` — catching specific exceptions improves clarity and prevents swallowing bugs.
- Domain-driven exceptions map to business concepts (validation failure vs transient network error) and allow orchestration to react differently (retry, skip, escalate).

Expected output behavior
- Validation failures are logged and skipped; transient errors are identified for retry logic; dependency issues are escalated.

---

## 8. Logging Best Practices

Objective
- Show a production-style logging configuration with module-level loggers, structured messages, and different verbosity levels.

Step-by-step implementation
1. Configure root logger with appropriate handler and formatter.
2. Use module-level loggers (named by module) to control verbosity.
3. Show structured logging via key=value patterns.

Full code
```python
# demo_logging.py
import logging


def configure_logging():
    fmt = "%(asctime)s level=%(levelname)s name=%(name)s msg=\"%(message)s\""
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt))

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)


def run():
    configure_logging()
    logger = logging.getLogger("pipeline.ingest")
    logger.info("start run batch_id=%s", 42)
    try:
        logger.debug("debugging details that are verbose")
        raise ValueError("simulated error")
    except Exception:
        logger.exception("Error while processing batch")


if __name__ == "__main__":
    run()
```

Explanation of key parts
- Logger per module: `logging.getLogger("pipeline.ingest")` allows adjusting granular levels in production.
- Structured message: The format includes `level=`, `name=`, and `msg=`, making it easy to parse.
- `logger.exception`: Logs the exception with traceback at ERROR level.

Expected output behavior
- INFO log for batch start, ERROR log with traceback for the simulated error, debug logs shown only if level set to DEBUG.

---

## 9. Retry with Exponential Backoff

Objective
- Implement a retry decorator with exponential backoff and jitter; demonstrate on a flaky function.

Step-by-step implementation
1. Implement retry with `max_attempts`, `base_delay`, `max_delay`, and random jitter.
2. Demonstrate avoiding retries for specific errors (e.g., `ValidationError`).

Full code
```python
# demo_backoff_retry.py
import time
import random
from functools import wraps


class NonRetryableError(Exception):
    pass


def backoff_retry(max_attempts=5, base_delay=0.1, max_delay=2.0, jitter=0.1, retry_exceptions=(Exception,), non_retry_exceptions=()):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                attempt += 1
                try:
                    return func(*args, **kwargs)
                except non_retry_exceptions:
                    raise
                except retry_exceptions as exc:
                    if attempt >= max_attempts:
                        raise
                    backoff = min(max_delay, base_delay * (2 ** (attempt - 1)))
                    # apply jitter
                    sleep_time = backoff * (1 + random.uniform(-jitter, jitter))
                    time.sleep(sleep_time)

        return wrapper

    return decorator


@backoff_retry(max_attempts=4, base_delay=0.05, jitter=0.2, retry_exceptions=(RuntimeError,), non_retry_exceptions=(NonRetryableError,))
def flaky_op(i):
    if i == 99:
        raise NonRetryableError("bad payload")
    if random.random() < 0.6:
        raise RuntimeError("transient failure")
    return f"ok:{i}"


if __name__ == "__main__":
    for i in range(5):
        try:
            print(flaky_op(i))
        except Exception as e:
            print("Failed permanently:", e)
```

Explanation of key parts
- Exponential backoff: delay increases exponentially between retries to reduce load on failing services.
- Jitter: Randomizing sleep reduces synchronized retry storms across multiple clients (thundering herd).
- Non-retryable errors: Domain errors (e.g., malformed payload) should not be retried.

Expected output behavior
- Retries with increasing delays; permanent failure after exhausting attempts; immediate raise for non-retryable errors.

---

## 10. Unit Testing with Pytest

Objective
- Provide a small module with validation and transformation functions and a `pytest` test file demonstrating parametrized tests, exception tests, fixtures, and mocking an external dependency.

Step-by-step implementation
1. Create a module `transform.py` with `validate_record` and `transform_record`.
2. Create `test_transform.py` illustrating tests.

Full code (module)
```python
# transform.py
from typing import Dict


class ValidationError(Exception):
    pass


def validate_record(rec: Dict) -> bool:
    if "id" not in rec:
        raise ValidationError("id missing")
    if not isinstance(rec["id"], int):
        raise ValidationError("id must be int")
    return True


def transform_record(rec: Dict) -> Dict:
    validate_record(rec)
    return {"id": rec["id"], "value": rec.get("value", 0) * 10}
```

Full code (tests)
```python
# test_transform.py
import pytest
from transform import validate_record, transform_record, ValidationError


@pytest.mark.parametrize("rec,expected", [
    ({"id": 1, "value": 2}, {"id": 1, "value": 20}),
    ({"id": 2}, {"id": 2, "value": 0}),
])
def test_transform_valid(rec, expected):
    assert transform_record(rec) == expected


def test_validate_raises_on_missing_id():
    with pytest.raises(ValidationError):
        validate_record({})


def test_validate_raises_on_wrong_type():
    with pytest.raises(ValidationError):
        validate_record({"id": "x"})


@pytest.fixture
def sample_rec():
    return {"id": 5, "value": 3}


def test_transform_with_fixture(sample_rec):
    assert transform_record(sample_rec)["value"] == 30

```

Mock example (external dependency)
```python
# Suppose transform_record calls an enrichment service; we can mock it.
from unittest.mock import patch


def test_transform_with_enrichment(monkeypatch):
    import transform

    def fake_enrich(rec):
        rec["value"] = 7
        return rec

    with patch("transform.enrich", fake_enrich):
        assert transform.transform_record({"id": 1})["value"] == 70
```

Explanation of key parts
- Parametrized tests reduce duplication for multiple input-output pairs.
- `pytest.raises` verifies expected exceptions.
- Fixtures provide reusable setup data.
- `unittest.mock.patch` (or `monkeypatch` fixture) isolates external dependencies for deterministic tests.

How to run tests
```
pip install pytest
pytest -q
```

Expected output behavior
- Tests should pass and provide concise summary from `pytest`.

---

## Closing notes
- This demo guide focuses on practical patterns for ETL/pipeline systems: robust decorators, clear error handling, correct concurrency model choices, and testable code.
- For each code block, create small modules in a temporary project to run and iterate. The examples are intentionally compact and portable.

File: [lab/Day07_Demo_Guide.md](lab/Day07_Demo_Guide.md)
