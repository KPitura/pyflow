import pytest
from pyflow import Task, Workflow, WorkflowValidationError

def dummy_func(inputs, params):
    return "ok"

def test_task_creation_immutability():
    deps = ["a", "b"]
    t = Task(name="test", function=dummy_func, dependencies=deps)
    deps.append("c") # Zmiana zewnętrznej listy nie powinna wpływać na zadanie
    assert "c" not in t.dependencies

def test_task_invalid_name_and_self_dependency():
    with pytest.raises(ValueError):
        Task(name="", function=dummy_func)
    with pytest.raises(ValueError):
        Task(name="A", function=dummy_func, dependencies=["A"])

def test_workflow_add_duplicate():
    w = Workflow()
    t = Task(name="A", function=dummy_func)
    w.add_task(t)
    with pytest.raises(ValueError):
        w.add_task(t)

def test_workflow_missing_dependency():
    w = Workflow()
    w.add_task(Task("A", dummy_func, dependencies=["MISSING"]))
    with pytest.raises(WorkflowValidationError):
        w.validate()