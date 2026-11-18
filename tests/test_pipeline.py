import json
from pyflow.engine import format_report, run
from pyflow.pipeline import build_etl_workflow


def test_csv_to_json_pipeline(tmp_path):
    csv_file = tmp_path / "sales.csv"
    json_file = tmp_path / "report.json"

    csv_content = (
        "category,amount\n"
        "Books,100.50\n"
        " books ,49.50\n"
        "Electronics,300.00\n"
        "Books,-20.00\n"
        "Food,invalid_number\n"
    )
    csv_file.write_text(csv_content, encoding="utf-8")

    wf = build_etl_workflow(input_csv=csv_file, output_json=json_file)
    result = run(wf)

    assert result.succeeded is True
    assert json_file.exists()

    report_data = json.loads(json_file.read_text(encoding="utf-8"))
    assert report_data == {
        "valid_records_count": 3,
        "total_amount": 450.0,
        "by_category": {
            "books": 150.0,
            "electronics": 300.0,
        },
    }

    text_report = format_report(result)
    assert "extract_csv" in text_report
    assert "export_json" in text_report