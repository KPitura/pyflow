import csv
import json
from pathlib import Path
from typing import Any, Mapping

from pyflow.models import Task, Workflow


def read_csv_task(inputs: Mapping[str, Any], params: Mapping[str, Any]) -> list[dict[str, str]]:
    file_path = Path(params["input_path"])
    with file_path.open(mode="r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def clean_records_task(inputs: Mapping[str, Any], params: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_rows = inputs["extract_csv"]
    cleaned = []
    for row in raw_rows:
        try:
            amount = float(row["amount"])
            if amount <= 0:
                continue
            cleaned.append(
                {
                    "category": row["category"].strip().lower(),
                    "amount": round(amount, 2),
                }
            )
        except (ValueError, KeyError, TypeError):
            continue
    return cleaned


def aggregate_task(inputs: Mapping[str, Any], params: Mapping[str, Any]) -> dict[str, Any]:
    records = inputs["clean_data"]
    totals: dict[str, float] = {}
    for rec in records:
        cat = rec["category"]
        totals[cat] = round(totals.get(cat, 0.0) + rec["amount"], 2)

    return {
        "valid_records_count": len(records),
        "total_amount": round(sum(totals.values()), 2),
        "by_category": dict(sorted(totals.items())),
    }


def write_json_task(inputs: Mapping[str, Any], params: Mapping[str, Any]) -> str:
    summary = inputs["aggregate_data"]
    output_path = Path(params["output_path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open(mode="w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    return str(output_path)


def build_etl_workflow(input_csv: str | Path, output_json: str | Path) -> Workflow:
    wf = Workflow()
    wf.add_task(
        Task(
            name="extract_csv",
            function=read_csv_task,
            params={"input_path": str(input_csv)},
            description="Odczyt surowych danych z pliku CSV",
        )
    )
    wf.add_task(
        Task(
            name="clean_data",
            function=clean_records_task,
            dependencies=frozenset(["extract_csv"]),
            description="Walidacja kwot i normalizacja kategorii",
        )
    )
    wf.add_task(
        Task(
            name="aggregate_data",
            function=aggregate_task,
            dependencies=frozenset(["clean_data"]),
            description="Agregacja sum według kategorii",
        )
    )
    wf.add_task(
        Task(
            name="export_json",
            function=write_json_task,
            dependencies=frozenset(["aggregate_data"]),
            params={"output_path": str(output_json)},
            description="Zapis raportu końcowego do JSON",
        )
    )
    return wf