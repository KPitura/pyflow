import pytest
from pyflow import Task, Workflow, WorkflowValidationError, topological_order, execution_layers

def dummy(i, p): pass

def test_empty_and_single_graph():
    w = Workflow()
    assert topological_order(w) == []
    assert execution_layers(w) == []

    w.add_task(Task("A", dummy))
    assert topological_order(w) == ["A"]
    assert execution_layers(w) == [["A"]]

def test_chain_and_layers():
    w = Workflow()
    w.add_task(Task("extract", dummy))
    w.add_task(Task("clean_a", dummy, dependencies=["extract"]))
    w.add_task(Task("clean_b", dummy, dependencies=["extract"]))
    w.add_task(Task("report", dummy, dependencies=["clean_a", "clean_b"]))
    
    assert execution_layers(w) == [["extract"], ["clean_a", "clean_b"], ["report"]]
    
    order = topological_order(w)
    assert order == ["extract", "clean_a", "clean_b", "report"]

def test_cycle_detection():
    w = Workflow()
    w.add_task(Task("A", dummy, dependencies=["B"]))
    w.add_task(Task("B", dummy, dependencies=["A"]))
    
    with pytest.raises(WorkflowValidationError):
        topological_order(w)
    with pytest.raises(WorkflowValidationError):
        execution_layers(w)
    with pytest.raises(WorkflowValidationError):
        w.validate()

def test_independent_tasks():
    w = Workflow()
    w.add_task(Task("Z", dummy))
    w.add_task(Task("A", dummy))
    assert execution_layers(w) == [["A", "Z"]]