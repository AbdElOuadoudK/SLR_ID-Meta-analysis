from slr_meta.shared.dependencies import format_missing_dependency_message


def test_format_missing_dependency_message_is_actionable_for_single_dependency():
    message = format_missing_dependency_message(["pandas"])

    assert "Missing required Python dependency: pandas." in message
    assert "python -m pip install -r requirements.txt" in message


def test_format_missing_dependency_message_pluralizes_dependencies():
    message = format_missing_dependency_message(["pandas", "openpyxl"])

    assert "Missing required Python dependencies: pandas, openpyxl." in message
