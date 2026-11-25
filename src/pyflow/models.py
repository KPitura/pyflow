from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional


class WorkflowValidationError(Exception):
    pass


@dataclass(frozen=True)
class Task:
    name: str
    function: Callable[[Mapping[str, Any], Mapping[str, Any]], Any]
    dependencies: frozenset[str] = field(default_factory=frozenset)
    params: Mapping[str, Any] = field(default_factory=dict)
    description: Optional[str] = None
    retries: int = 0
    timeout: Optional[float] = None

    def __post_init__(self):
        if not self.name or not self.name.strip():
            raise ValueError("Nazwa zadania nie może być pusta.")
        if self.name in self.dependencies:
            raise ValueError(f"Zadanie '{self.name}' nie może zależeć samo od siebie.")
        if self.retries < 0:
            raise ValueError("Liczba ponowień (retries) nie może być ujemna.")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("Limit czasu (timeout) musi być dodatni.")


class Workflow:
    def __init__(self):
        self.tasks: dict[str, Task] = {}

    def add_task(self, task: Task) -> None:
        if task.name in self.tasks:
            raise ValueError(f"Zadanie o nazwie '{task.name}' już istnieje w workflow.")
        self.tasks[task.name] = task

    def validate(self) -> None:
        for task in self.tasks.values():
            for dep in task.dependencies:
                if dep not in self.tasks:
                    raise WorkflowValidationError(
                        f"Zadanie '{task.name}' ma niezarejestrowaną zależność: '{dep}'"
                    )

        from pyflow.graph import topological_order

        try:
            topological_order(self)
        except ValueError as e:
            raise WorkflowValidationError(f"Walidacja DAG nie powiodła się: {e}")