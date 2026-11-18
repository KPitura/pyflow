import time
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Optional

from pyflow.graph import topological_order
from pyflow.models import Workflow


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class TaskExecutionError(Exception):
    """Wyjątek opakowujący błąd wykonania konkretnego zadania."""

    def __init__(self, task_name: str, original_exception: Exception):
        self.task_name = task_name
        self.original_exception = original_exception
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


def _execute_task_safely(
    task_name: str,
    func: Any,
    inputs: Mapping[str, Any],
    params: Mapping[str, Any],
) -> Any:
    """Wywołuje funkcję zadania i opakowuje ewentualny wyjątek z zachowaniem przyczyny."""
    try:
        return func(inputs, params)
    except Exception as exc:
        raise TaskExecutionError(task_name, exc) from exc


def run(workflow: Workflow) -> RunResult:
    """
    Waliduje workflow i uruchamia zadania synchronicznie zgodnie z porządkiem topologicznym.
    """
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

        try:
            result = _execute_task_safely(
                task_name, task.function, task_inputs, task_params
            )
        except TaskExecutionError as exec_err:
            state.ended_at = time.perf_counter()
            state.duration = state.ended_at - state.started_at
            state.status = TaskStatus.FAILED
            state.error = exec_err
        else:
            state.ended_at = time.perf_counter()
            state.duration = state.ended_at - state.started_at
            state.status = TaskStatus.SUCCEEDED
            outputs[task_name] = result

    total_duration = time.perf_counter() - workflow_start

    return RunResult(
        outputs=outputs,
        task_states=task_states,
        total_duration=total_duration,
    )


def format_report(result: RunResult) -> str:
    """Generuje czytelny raport tekstowy z przebiegu wykonania workflow."""
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
        line = f"[{state.status.value:9}] {name:<20} | czas: {time_str}"
        if state.error:
            line += f" | przyczyna: {state.error}"
        lines.append(line)

    return "\n".join(lines)