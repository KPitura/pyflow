import pytest
from pyflow.models import Task, Workflow, WorkflowValidationError


def dummy_func(inputs, params):
    return "ok"


def test_task_creation():
    t = Task(name="t1", function=dummy_func)
    assert t.name == "t1"
    assert t.dependencies == frozenset()
    assert t.retries == 0
    assert t.timeout is None


def test_task_invalid_retries_and_timeout():
    with pytest.raises(ValueError, match="retries"):
        Task(name="t1", function=dummy_func, retries=-1)
    with pytest.raises(ValueError, match="timeout"):
        Task(name="t1", function=dummy_func, timeout=0)
    with pytest.raises(ValueError, match="timeout"):
        Task(name="t1", function=dummy_func, timeout=-2.5)


def test_task_empty_name():
    with pytest.raises(ValueError, match="nie może być pusta"):
        Task(name="", function=dummy_func)


def test_task_self_dependency():
    with pytest.raises(ValueError, match="nie może zależeć samo od siebie"):
        Task(name="t1", function=dummy_func, dependencies=frozenset(["t1"]))


def test_workflow_add_duplicate():
    w = Workflow()
    t1 = Task(name="t1", function=dummy_func)
    t2 = Task(name="t1", function=dummy_func)
    w.add_task(t1)
    with pytest.raises(ValueError, match="już istnieje"):
        w.add_task(t2)


def test_workflow_missing_dependency():
    w = Workflow()
    t1 = Task(name="t1", function=dummy_func, dependencies=frozenset(["missing"]))
    w.add_task(t1)
    with pytest.raises(WorkflowValidationError, match="niezarejestrowaną zależność"):
        w.validate()