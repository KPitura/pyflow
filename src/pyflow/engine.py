import asyncio
import functools
import inspect
import time
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Mapping, Optional

from pyflow.graph import execution_layers, topological_order
from pyflow.models import Task, Workflow


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class TaskExecutionError(Exception):
    def __init__(self, task_name: str, original_exception: BaseException):
        self.task_name = task_name
        self.original_exception = original_exception
        self.__cause__ = original_exception
        super().__init__(
            f"Zadanie '{task_name}' zakończyło się błędem: {type(original_exception).__name__}: {original_exception}"
        )

@dataclass
class TaskState:
    status: TaskStatus = TaskStatus.PENDING
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    duration: float = 0.0
    error: Optional[TaskExecutionError] = None
    attempts: int = 0


@dataclass
class RunResult:
    outputs: dict[str, Any] = field(default_factory=dict)
    task_states: dict[str, TaskState] = field(default_factory=dict)
    total_duration: float = 0.0

    @property
    def succeeded(self) -> bool:
        return all(
            state.status == TaskStatus.SUCCEEDED for state in self.task_states.values()
        )


def _is_async_callable(obj: Any) -> bool:
    unwrapped = inspect.unwrap(obj)
    if inspect.iscoroutinefunction(unwrapped):
        return True
    if not inspect.isfunction(unwrapped) and not inspect.ismethod(unwrapped) and callable(unwrapped):
        call_method = getattr(unwrapped, "__call__", None)
        if call_method is not None and inspect.iscoroutinefunction(inspect.unwrap(call_method)):
            return True
    return False


async def _invoke_callable(
    func: Callable[[Mapping[str, Any], Mapping[str, Any]], Any],
    inputs: Mapping[str, Any],
    params: Mapping[str, Any],
) -> Any:
    if _is_async_callable(func):
        return await func(inputs, params)

    bound = functools.partial(func, inputs, params)
    res = await asyncio.to_thread(bound)
    if inspect.isawaitable(res):
        return await res
    return res


def _execute_sync_attempt(
    task: Task,
    inputs: Mapping[str, Any],
    params: Mapping[str, Any],
) -> Any:
    if task.timeout is None:
        return task.function(inputs, params)

    async def _run_with_timeout() -> Any:
        return await asyncio.wait_for(
            asyncio.to_thread(functools.partial(task.function, inputs, params)),
            timeout=task.timeout,
        )

    return asyncio.run(_run_with_timeout())


def run(
    workflow: Workflow,
    base_delay: float = 0.1,
    sleep_func: Callable[[float], None] = time.sleep,
) -> RunResult:
    workflow.validate()
    order = topological_order(workflow)

    task_states: dict[str, TaskState] = {
        name: TaskState(status=TaskStatus.PENDING) for name in order
    }
    outputs: dict[str, Any] = {}

    workflow_start = time.perf_counter()

    for task_name in order:
        task = workflow.tasks[task_name]
        state = task_states[task_name]

        blocked_by = [
            dep
            for dep in task.dependencies
            if task_states[dep].status in (TaskStatus.FAILED, TaskStatus.SKIPPED)
        ]

        if blocked_by:
            state.status = TaskStatus.SKIPPED
            continue

        task_inputs = MappingProxyType({dep: outputs[dep] for dep in task.dependencies})
        task_params = MappingProxyType(dict(task.params))

        state.status = TaskStatus.RUNNING
        state.started_at = time.perf_counter()

        max_attempts = task.retries + 1
        last_error: Optional[TaskExecutionError] = None

        for attempt in range(max_attempts):
            state.attempts = attempt + 1
            try:
                result = _execute_sync_attempt(task, task_inputs, task_params)
            except Exception as exc:
                last_error = TaskExecutionError(task_name, exc)
                if attempt < task.retries:
                    delay = base_delay * (2**attempt)
                    if delay > 0:
                        sleep_func(delay)
            else:
                state.ended_at = time.perf_counter()
                state.duration = state.ended_at - state.started_at
                state.status = TaskStatus.SUCCEEDED
                outputs[task_name] = result
                last_error = None
                break

        if last_error is not None:
            state.ended_at = time.perf_counter()
            state.duration = state.ended_at - state.started_at
            state.status = TaskStatus.FAILED
            state.error = last_error

    total_duration = time.perf_counter() - workflow_start

    return RunResult(
        outputs=outputs,
        task_states=task_states,
        total_duration=total_duration,
    )


async def _run_single_task_async(
    task: Task,
    task_inputs: Mapping[str, Any],
    task_params: Mapping[str, Any],
    semaphore: asyncio.Semaphore,
    base_delay: float,
    sleep_func: Callable[[float], Awaitable[None]],
) -> tuple[TaskState, Any, bool]:
    state = TaskState(status=TaskStatus.PENDING)
    async with semaphore:
        state.status = TaskStatus.RUNNING
        state.started_at = time.perf_counter()

        max_attempts = task.retries + 1
        last_error: Optional[TaskExecutionError] = None

        for attempt in range(max_attempts):
            state.attempts = attempt + 1
            try:
                coro = _invoke_callable(task.function, task_inputs, task_params)
                if task.timeout is not None:
                    result = await asyncio.wait_for(coro, timeout=task.timeout)
                else:
                    result = await coro
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_error = TaskExecutionError(task.name, exc)
                if attempt < task.retries:
                    delay = base_delay * (2**attempt)
                    if delay > 0:
                        await sleep_func(delay)
            else:
                state.ended_at = time.perf_counter()
                state.duration = state.ended_at - state.started_at
                state.status = TaskStatus.SUCCEEDED
                return state, result, True

        state.ended_at = time.perf_counter()
        state.duration = state.ended_at - state.started_at
        state.status = TaskStatus.FAILED
        state.error = last_error
        return state, None, False


async def async_run(
    workflow: Workflow,
    max_concurrency: int = 4,
    base_delay: float = 0.1,
    sleep_func: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> RunResult:
    if max_concurrency < 1:
        raise ValueError("Parametr max_concurrency musi być większy lub równy 1.")

    workflow.validate()
    layers = execution_layers(workflow)

    task_states: dict[str, TaskState] = {
        name: TaskState(status=TaskStatus.PENDING)
        for layer in layers
        for name in layer
    }
    outputs: dict[str, Any] = {}
    semaphore = asyncio.Semaphore(max_concurrency)

    workflow_start = time.perf_counter()

    for layer in layers:
        ready_tasks: list[tuple[Task, Mapping[str, Any], Mapping[str, Any]]] = []

        for task_name in layer:
            task = workflow.tasks[task_name]
            blocked_by = [
                dep
                for dep in task.dependencies
                if task_states[dep].status in (TaskStatus.FAILED, TaskStatus.SKIPPED)
            ]
            if blocked_by:
                task_states[task_name].status = TaskStatus.SKIPPED
            else:
                t_inputs = MappingProxyType({dep: outputs[dep] for dep in task.dependencies})
                t_params = MappingProxyType(dict(task.params))
                ready_tasks.append((task, t_inputs, t_params))

        if not ready_tasks:
            continue

        coroutines = [
            _run_single_task_async(
                task=task,
                task_inputs=t_inputs,
                task_params=t_params,
                semaphore=semaphore,
                base_delay=base_delay,
                sleep_func=sleep_func,
            )
            for task, t_inputs, t_params in ready_tasks
        ]

        gathered = await asyncio.gather(*coroutines, return_exceptions=True)

        for item in gathered:
            if isinstance(item, asyncio.CancelledError):
                raise item
            if isinstance(item, BaseException):
                raise item

        for (task, _, _), (state, value, success) in zip(ready_tasks, gathered):
            task_states[task.name] = state
            if success:
                outputs[task.name] = value

    total_duration = time.perf_counter() - workflow_start

    return RunResult(
        outputs=outputs,
        task_states=task_states,
        total_duration=total_duration,
    )


def format_report(result: RunResult) -> str:
    overall_status = "SUKCES" if result.succeeded else "BŁĄD"
    lines = [
        "=== RAPORT WYKONANIA WORKFLOW ===",
        f"Status ogólny: {overall_status}",
        f"Całkowity czas: {result.total_duration * 1000:.3f} ms",
        "-" * 33,
    ]

    if not result.task_states:
        lines.append("Brak zadań do wykonania (pusty workflow).")
        return "\n".join(lines)

    for name, state in result.task_states.items():
        time_str = f"{state.duration * 1000:.3f} ms"
        line = f"[{state.status.value:9}] {name:<20} | próby: {state.attempts} | czas: {time_str}"
        if state.error:
            line += f" | przyczyna: {state.error}"
        lines.append(line)

    return "\n".join(lines)