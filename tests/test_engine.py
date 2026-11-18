import pytest
from pyflow.engine import TaskExecutionError, TaskStatus, format_report, run
from pyflow.models import Task, Workflow


def test_empty_workflow():
    wf = Workflow()
    result = run(wf)
    assert result.succeeded is True
    assert result.outputs == {}
    assert result.task_states == {}
    assert result.total_duration >= 0.0


def test_data_flow_and_params():
    execution_log = []

    def step_a(inputs, params):
        execution_log.append("a")
        assert dict(inputs) == {}
        return params["base"] * 2

    def step_b(inputs, params):
        execution_log.append("b")
        assert dict(inputs) == {"a": 10}
        return inputs["a"] + params["offset"]

    def step_c(inputs, params):
        execution_log.append("c")
        # step_c zależy tylko od step_b, nie powinien widzieć step_a w inputs
        assert dict(inputs) == {"b": 15}
        return f"Wynik: {inputs['b']}"

    wf = Workflow()
    wf.add_task(Task("a", step_a, params={"base": 5}))
    wf.add_task(Task("b", step_b, dependencies=frozenset(["a"]), params={"offset": 5}))
    wf.add_task(Task("c", step_c, dependencies=frozenset(["b"])))

    result = run(wf)

    assert execution_log == ["a", "b", "c"]
    assert result.succeeded is True
    assert result.outputs == {"a": 10, "b": 15, "c": "Wynik: 15"}
    for state in result.task_states.values():
        assert state.status == TaskStatus.SUCCEEDED
        assert state.duration >= 0.0


def test_failure_policy_and_exception_chaining():
    def failing_task(inputs, params):
        raise ZeroDivisionError("dzielenie przez zero")

    def dependent_task(inputs, params):
        return "nie powinno się wykonać"

    def independent_task(inputs, params):
        return "niezależny sukces"

    wf = Workflow()
    wf.add_task(Task("fail_root", failing_task))
    wf.add_task(Task("child", dependent_task, dependencies=frozenset(["fail_root"])))
    wf.add_task(Task("grandchild", dependent_task, dependencies=frozenset(["child"])))
    wf.add_task(Task("independent", independent_task))

    result = run(wf)

    assert result.succeeded is False

    # Sprawdzenie statusów
    assert result.task_states["fail_root"].status == TaskStatus.FAILED
    assert result.task_states["child"].status == TaskStatus.SKIPPED
    assert result.task_states["grandchild"].status == TaskStatus.SKIPPED
    assert result.task_states["independent"].status == TaskStatus.SUCCEEDED

    # Sprawdzenie wyników
    assert "independent" in result.outputs
    assert result.outputs["independent"] == "niezależny sukces"
    assert "fail_root" not in result.outputs
    assert "child" not in result.outputs

    # Sprawdzenie wyjątku i łańcuchowania przyczyny (__cause__)
    err = result.task_states["fail_root"].error
    assert isinstance(err, TaskExecutionError)
    assert err.task_name == "fail_root"
    assert isinstance(err.__cause__, ZeroDivisionError)


def test_format_report():
    wf = Workflow()
    wf.add_task(Task("t1", lambda i, p: 42))
    result = run(wf)
    report = format_report(result)

    assert "RAPORT WYKONANIA WORKFLOW" in report
    assert "Status ogólny: SUKCES" in report
    assert "[SUCCEEDED] t1" in report