import asyncio
import functools
import time
import pytest
from pyflow.engine import TaskExecutionError, TaskStatus, async_run
from pyflow.models import Task, Workflow


def custom_decorator(fn):
    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        res = await fn(*args, **kwargs)
        return res + 1

    return wrapper


class CallableAsyncTask:
    async def __call__(self, inputs, params):
        return params["val"] * 10


def test_async_invalid_concurrency():
    wf = Workflow()
    with pytest.raises(ValueError, match="max_concurrency"):
        asyncio.run(async_run(wf, max_concurrency=0))


def test_async_mixed_sync_and_async_non_blocking():
    async def _scenario():
        event_loop_ticks = 0
        stop_ticker = False

        async def ticker():
            nonlocal event_loop_ticks
            while not stop_ticker:
                event_loop_ticks += 1
                await asyncio.sleep(0.005)

        def blocking_sync_task(inputs, params):
            time.sleep(0.04)
            return 10

        @custom_decorator
        async def decorated_async_task(inputs, params):
            await asyncio.sleep(0.01)
            return inputs["sync_step"]

        wf = Workflow()
        wf.add_task(Task("sync_step", blocking_sync_task))
        wf.add_task(Task("callable_step", CallableAsyncTask(), params={"val": 3}))
        wf.add_task(
            Task(
                "async_step",
                decorated_async_task,
                dependencies=frozenset(["sync_step"]),
            )
        )

        ticker_task = asyncio.create_task(ticker())
        result = await async_run(wf, max_concurrency=2)
        stop_ticker = True
        await ticker_task

        assert result.succeeded is True
        assert result.outputs == {
            "sync_step": 10,
            "callable_step": 30,
            "async_step": 11,
        }
        assert event_loop_ticks >= 3

    asyncio.run(_scenario())


def test_async_max_concurrency_and_layer_order():
    async def _scenario():
        active_count = 0
        max_observed = 0
        execution_events = []

        async def worker(inputs, params):
            nonlocal active_count, max_observed
            name = params["name"]
            active_count += 1
            if active_count > max_observed:
                max_observed = active_count
            execution_events.append(f"start:{name}")
            await asyncio.sleep(0.02)
            execution_events.append(f"end:{name}")
            active_count -= 1
            return name

        wf = Workflow()
        for i in range(5):
            wf.add_task(Task(f"w_{i}", worker, params={"name": f"w_{i}"}))
        wf.add_task(
            Task(
                "final",
                worker,
                dependencies=frozenset([f"w_{i}" for i in range(5)]),
                params={"name": "final"},
            )
        )

        result = await async_run(wf, max_concurrency=2)

        assert result.succeeded is True
        assert max_observed == 2
        assert execution_events[-2:] == ["start:final", "end:final"]

    asyncio.run(_scenario())


def test_async_retry_success_with_backoff():
    async def _scenario():
        calls = 0
        recorded_delays = []

        async def fake_sleep(delay):
            recorded_delays.append(delay)

        async def flaky_task(inputs, params):
            nonlocal calls
            calls += 1
            if calls < 3:
                raise RuntimeError("tymczasowy błąd")
            return "sukces po próbach"

        wf = Workflow()
        wf.add_task(Task("flaky", flaky_task, retries=3))

        result = await async_run(wf, base_delay=0.5, sleep_func=fake_sleep)

        assert result.succeeded is True
        assert result.outputs["flaky"] == "sukces po próbach"
        assert result.task_states["flaky"].attempts == 3
        assert recorded_delays == [0.5, 1.0]

    asyncio.run(_scenario())


def test_async_retry_exhaustion_and_skipped_dependents():
    async def _scenario():
        calls = 0

        async def always_failing(inputs, params):
            nonlocal calls
            calls += 1
            raise ValueError("krytyczny błąd")

        async def dependent(inputs, params):
            return "nigdy nie uruchomione"

        async def independent(inputs, params):
            return "niezależne ok"

        wf = Workflow()
        wf.add_task(Task("bad", always_failing, retries=2))
        wf.add_task(Task("child", dependent, dependencies=frozenset(["bad"])))
        wf.add_task(Task("good", independent))

        result = await async_run(wf, base_delay=0.0)

        assert result.succeeded is False
        assert calls == 3
        assert result.task_states["bad"].status == TaskStatus.FAILED
        assert result.task_states["bad"].attempts == 3
        assert isinstance(result.task_states["bad"].error, TaskExecutionError)
        assert isinstance(result.task_states["bad"].error.__cause__, ValueError)

        assert result.task_states["child"].status == TaskStatus.SKIPPED
        assert result.task_states["child"].attempts == 0

        assert result.task_states["good"].status == TaskStatus.SUCCEEDED
        assert result.outputs["good"] == "niezależne ok"

    asyncio.run(_scenario())


def test_async_timeout_triggers_retry_and_fails():
    async def _scenario():
        attempts_started = 0

        async def slow_task(inputs, params):
            nonlocal attempts_started
            attempts_started += 1
            await asyncio.sleep(0.2)
            return "za późno"

        wf = Workflow()
        wf.add_task(Task("slow", slow_task, timeout=0.02, retries=1))

        result = await async_run(wf, base_delay=0.0)

        assert result.succeeded is False
        assert attempts_started == 2
        assert result.task_states["slow"].status == TaskStatus.FAILED
        assert result.task_states["slow"].attempts == 2
        assert isinstance(result.task_states["slow"].error.__cause__, asyncio.TimeoutError)

    asyncio.run(_scenario())


def test_async_cancellation_propagates():
    async def _scenario():
        started_event = asyncio.Event()

        async def hanging_task(inputs, params):
            started_event.set()
            await asyncio.sleep(10)

        wf = Workflow()
        wf.add_task(Task("hang", hanging_task, retries=3))

        runner_task = asyncio.create_task(async_run(wf, base_delay=0.0))
        await started_event.wait()
        runner_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await runner_task

    asyncio.run(_scenario())