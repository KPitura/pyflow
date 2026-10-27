import copy
from dataclasses import dataclass, field
from typing import Callable, Any, Mapping

class WorkflowValidationError(Exception):
    pass

@dataclass
class Task:
    name: str
    function: Callable[[Mapping[str, Any], Mapping[str, Any]], Any]
    dependencies: frozenset[str] = field(default_factory=frozenset)
    params: Mapping[str, Any] = field(default_factory=dict)
    description: str | None = None

    def __post_init__(self):
        if not self.name:
            raise ValueError("Nazwa zadania nie może być pusta.")
        if self.name in self.dependencies:
            raise ValueError(f"Zadanie '{self.name}' nie może zależeć od samego siebie.")
        
        self.dependencies = frozenset(self.dependencies)
        self.params = copy.deepcopy(dict(self.params))

class Workflow:
    def __init__(self):
        self.tasks: dict[str, Task] = {}

    def add_task(self, task: Task) -> None:
        if task.name in self.tasks:
            raise ValueError(f"Zadanie '{task.name}' zostało już dodane.")
        self.tasks[task.name] = task

    def validate(self) -> None:
        # 1. Sprawdzenie, czy zadania nie odwołują się do nieistniejących zależności
        for task in self.tasks.values():
            for dep in task.dependencies:
                if dep not in self.tasks:
                    raise WorkflowValidationError(
                        f"Brakująca zależność: '{dep}' dla zadania '{task.name}'"
                    )
        
        from .graph import topological_order
        topological_order(self)