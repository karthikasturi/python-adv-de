# Day07 Lab Guide: Advanced Python — Decorators, Async, Parallel Processing, Testing & Logging

This hands-on lab contains 10 progressive exercises focused on building production-oriented ETL/pipeline utilities using decorators, concurrency, error handling, and testing. Each lab lists objectives, implementation tasks, starter code (where useful), validation checkpoints, and expected behaviour. Do not paste full solutions — implement and iterate.

---

# Lab 1: Function Decorators (Logging, Timing, Retry)

## Objective
Build reusable function decorators to enhance ETL functions with logging, timing, and retry behaviour.

## Tasks
1. Create a `log_calls` decorator that prints the function name and arguments when called.
2. Create a `timeit` decorator that measures and prints execution time.
3. Create a `retry` decorator with a `max_attempts` parameter that retries on exception.
4. Apply all three decorators to a simulated `fetch_data()` function that sometimes fails.
5. Ensure `retry` re-raises the final exception after exhausting attempts.

## Starter Code
```python
import random
import time

def fetch_data():
    """Simulated API fetch: randomly raises RuntimeError to simulate failure."""
    if random.random() < 0.4:
        raise RuntimeError("simulated fetch failure")
    time.sleep(0.05)
    return {"rows": [1, 2, 3]}

# Apply your decorators here

if __name__ == '__main__':
    print(fetch_data())
```

## Validation Checklist
- Logging prints the function name and argument summary.
- Timing prints a human-readable execution time (ms or seconds).
- Retry attempts the configured number of times on failure.
- After final failed attempt, the exception propagates.

## Expected Behavior / Hints
- `log_calls` should show function name and abbreviated args (avoid printing huge data structures).
- `retry` should sleep a small interval between attempts (you can start with fixed sleep).

---

# Lab 2: Class-Based Decorators

## Objective
Implement a stateful retry mechanism as a class-based decorator for better configurability and observability.

## Tasks
1. Create a `Retry` class that accepts `max_attempts` in `__init__`.
2. Implement `__call__` to wrap the target function and perform retries.
3. Print or log the attempt number for each try.
4. Apply the decorator to a `load_data()` function which fails intermittently.

## Starter Code
```python
import random

class Retry:
    def __init__(self, max_attempts=3):
        self.max_attempts = max_attempts

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            # implement retry loop here
            return func(*args, **kwargs)
        return wrapper

@Retry(max_attempts=3)
def load_data():
    if random.random() < 0.5:
        raise ValueError("transient load error")
    return "ok"

if __name__ == '__main__':
    print(load_data())
```

## Validation Checklist
- Attempt count increments and is printed for each retry.
- Final exception propagates after `max_attempts`.
- The class decorator behaves the same as a function decorator from a call site view.

---

# Lab 3: Dynamic Class Creation

## Objective
Create connector classes dynamically at runtime using `type()` to support flexible ETL connector creation.

## Tasks
1. Define a `BaseConnector` with `connect()` and `read()` stubs.
2. Write a factory function `make_connector(name, source_type)` that uses `type()` to create a subclass.
3. Dynamically create `MySQLConnector`, `APIConnector`, and `FileConnector` with a `source_type` attribute and simple method implementations.
4. Instantiate each connector and call `connect()` and `read()`.

## Starter Code
```python
class BaseConnector:
    def __init__(self, config=None):
        self.config = config or {}

    def connect(self):
        raise NotImplementedError

    def read(self):
        raise NotImplementedError

def make_connector(name, source_type):
    # Use type() to construct a new class that inherits from BaseConnector
    return type(name, (BaseConnector,), {
        'source_type': source_type,
        'connect': lambda self: f"connected to {self.source_type}",
        'read': lambda self: [1,2,3]
    })

# Create and test connectors

```

## Validation Checklist
- Classes are created dynamically (inspect `type(conn_class)` or `conn_class.__name__`).
- Each connector has correct `source_type` attribute.
- Instances behave like normal classes (`connect()` returns expected stub text).

---

# Lab 4: multiprocessing vs ProcessPoolExecutor

## Objective
Parallelize a CPU-bound workload and compare sequential vs process-based execution.

## Tasks
1. Implement a CPU-heavy function (e.g., large prime test, or a heavy numeric loop).
2. Run it sequentially over N inputs and measure total time.
3. Use `concurrent.futures.ProcessPoolExecutor` to run jobs in parallel and measure time.
4. Compare and explain results; ensure no pickling errors occur.

## Starter Code
```python
import time
from math import sqrt

def is_prime(n):
    if n < 2:
        return False
    for i in range(2, int(sqrt(n))+1):
        if n % i == 0:
            return False
    return True

def heavy_task(x):
    count = 0
    for i in range(100_000 + x, 100_000 + x + 5000):
        if is_prime(i):
            count += 1
    return count

if __name__ == '__main__':
    inputs = list(range(0, 20))
    # measure sequential and parallel versions

```

## Validation Checklist
- Parallel execution using processes is faster for this CPU-heavy task on multi-core machines.
- No pickling errors (avoid local nested functions or lambda that can't be pickled).
- Document why threads (`ThreadPoolExecutor`) are less effective for CPU-bound work (GIL).

---

# Lab 5: asyncio for I/O-bound Workloads

## Objective
Use `asyncio` to concurrently run many I/O-bound simulated API calls.

## Tasks
1. Write an `async def fetch_sim(id)` that awaits `asyncio.sleep()` to simulate latency.
2. Run N calls sequentially and measure time.
3. Use `asyncio.gather()` to run calls concurrently and measure time.
4. Ensure no blocking synchronous calls are used inside coroutines.

## Starter Code
```python
import asyncio
import time

async def fetch_sim(id, delay=0.2):
    await asyncio.sleep(delay)
    return {'id': id, 'data': id * 2}

async def run_sequential(n):
    results = []
    for i in range(n):
        results.append(await fetch_sim(i))
    return results

async def run_concurrent(n):
    tasks = [fetch_sim(i) for i in range(n)]
    return await asyncio.gather(*tasks)

if __name__ == '__main__':
    n = 20
    # measure run_sequential vs run_concurrent

```

## Validation Checklist
- Concurrent execution via `gather` is significantly faster than sequential awaits.
- No blocking synchronous I/O inside coroutines.
- Proper use of `async`/`await` and event loop.

---

# Lab 6: Hybrid Pipeline (Async I/O + ProcessPoolExecutor CPU)

## Objective
Combine asynchronous I/O for fetching and a process pool for CPU-heavy transformations.

## Tasks
1. Implement an async stage that fetches simulated payloads (`asyncio` + `sleep`).
2. Implement a CPU-bound `transform(payload)` function.
3. Use `asyncio.get_running_loop().run_in_executor(ProcessPoolExecutor(), transform, payload)` to offload CPU work.
4. Aggregate and log results per item.

## Starter Code
```python
import asyncio
from concurrent.futures import ProcessPoolExecutor

def transform(payload):
    # CPU heavy transformation
    s = 0
    for i in range(100_000 + payload['id']):
        s += (i % 7)
    return {**payload, 'transformed': s}

async def fetch_and_transform(id, executor):
    payload = await fetch_sim(id, delay=0.05)
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(executor, transform, payload)
    return result

```

## Validation Checklist
- Async is used for I/O stage; process pool is used for CPU stage.
- Results for each item contain transformed data.
- Logging is present for both stages.

---

# Lab 7: Custom Exceptions

## Objective
Design domain-specific exceptions and use them for clearer error handling in pipeline orchestration.

## Tasks
1. Create `DataValidationError` and `ExternalServiceError` exception classes.
2. Raise `DataValidationError` when input validation fails; raise `ExternalServiceError` when external calls fail.
3. In the orchestrator, catch and handle these exceptions differently (e.g., skip, alert, or retry).

## Starter Code
```python
class DataValidationError(Exception):
    pass

class ExternalServiceError(Exception):
    pass

def validate_row(row):
    if 'id' not in row:
        raise DataValidationError('missing id')

def call_external():
    # simulate
    raise ExternalServiceError('service unavailable')

```

## Validation Checklist
- `DataValidationError` and `ExternalServiceError` are raised intentionally in test cases.
- Orchestrator catches them separately and applies different handling logic.
- Avoid catching bare `Exception` for flow control.

---

# Lab 8: Logging Best Practices

## Objective
Replace ad-hoc `print()` usage with structured `logging`, use levels sensibly, and include stack traces for errors.

## Tasks
1. Configure Python `logging` with a formatter that includes timestamp, level, module, and message.
2. Replace `print` statements with `logger.debug/info/error` as appropriate.
3. Log lifecycle events of the pipeline (start/end/stage boundaries).
4. Log exceptions using `logger.exception()` to capture traceback.

## Starter Code
```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger('day07_lab')

def pipeline_step():
    try:
        # do work
        logger.info('step completed')
    except Exception:
        logger.exception('step failed')

```

## Validation Checklist
- No `print()` statements remain in pipeline code.
- Logs include timestamp and level and are human-readable.
- Exceptions are logged with full stack trace.

---

# Lab 9: Retry with Exponential Backoff

## Objective
Implement production-grade retry logic with exponential backoff and jitter; allow skipping retries for permanent errors.

## Tasks
1. Create a `retry_backoff` decorator that accepts `max_attempts`, `base_delay`, and `jitter`.
2. On each retry, wait `base_delay * (2 ** (attempt-1))` plus a small random jitter.
3. Accept an argument `retry_on` (tuple of exception types) to restrict retryable exceptions.
4. Log retry attempts and final failure.

## Starter Code
```python
import time
import random

def retry_backoff(max_attempts=5, base_delay=0.1, jitter=0.05, retry_on=(Exception,)):
    def decorator(func):
        def wrapper(*args, **kwargs):
            # implement backoff with jitter here
            return func(*args, **kwargs)
        return wrapper
    return decorator

@retry_backoff(max_attempts=3, base_delay=0.2)
def fragile_call():
    # simulate
    pass

```

## Validation Checklist
- Delay increases roughly exponentially between attempts.
- Jitter adds small variability to the delay.
- Exceptions not in `retry_on` cause immediate propagation (no retry).

---

# Lab 10: Unit Testing with Pytest

## Objective
Write unit tests for pipeline components, including parametrized tests, fixtures, and mocking external services.

## Tasks
1. Write tests for validation logic (including edge cases).
2. Test that the correct exceptions are raised for bad inputs.
3. Use `pytest.mark.parametrize` to test multiple inputs succinctly.
4. Create a fixture that returns sample dataset(s) for reuse.
5. Use `unittest.mock` or `pytest-mock` to mock external API calls and assert behavior.

## Starter Code (tests/test_validation.py)
```python
import pytest

from mypipeline import validate_row

@pytest.fixture
def sample_row():
    return {'id': 1, 'value': 42}

def test_validate_ok(sample_row):
    assert validate_row(sample_row) is None

@pytest.mark.parametrize('bad_row', [{}, {'value': 1}])
def test_validate_raises(bad_row):
    with pytest.raises(Exception):
        validate_row(bad_row)

```

## Validation Checklist
- Tests run and pass using `pytest`.
- Edge cases covered via parametrization.
- External dependencies are mocked so tests run offline.

---

# Final Notes

- Each lab is intentionally procedural: implement one piece, run it, add logging and tests, then compose into the next lab.
- Keep functions small and testable; avoid large monolithic scripts.
- Where applicable, prefer configuration-driven behavior (timeouts, max attempts, pool sizes).

If you want, I can also:
- Add a minimal `requirements.txt` or `pyproject.toml` for running tests and examples.
- Create small starter modules in `temp/` that map to each lab for quick iteration.
