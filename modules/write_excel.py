from pathlib import Path
from datetime import datetime
import pandas as pd

def write_upload_results_to_excel(CONFIG, auto_result_df, test_case_df, test_case_step_df, test_runs):
    """Write test results, cases, steps, and runs to a timestamped Excel file."""

    # Ensure required configuration keys are present
    required_keys = ['suite_name', 'output_dir']
    missing = [key for key in required_keys if key not in CONFIG]
    if missing:
        raise KeyError(f"Missing required config keys: {missing}")

    suite_name = CONFIG['suite_name']
    output_dir = Path(CONFIG['output_dir'])
    logger = CONFIG.get("logger")

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate sanitized filename with timestamp
    timestamp_str = datetime.now().strftime('%Y_%m_%d_%H%M%S')
    safe_suite_name = "".join(c for c in suite_name if c.isalnum() or c in " _-").rstrip()
    filename = f"{safe_suite_name} - Upload Results - {timestamp_str}.xlsx"
    output_path = output_dir / filename

    # Write DataFrames to Excel
    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            auto_result_df.to_excel(writer, sheet_name="Targets", index=False)
            test_case_df.to_excel(writer, sheet_name="Test Cases", index=False)
            test_case_step_df.to_excel(writer, sheet_name="Test Steps", index=False)
            test_runs.to_excel(writer, sheet_name="Created Test Runs", index=False)
    except Exception as e:
        raise IOError(f"Failed to write Excel file: {e}")

    logger.info(f"Wrote upload results to: '{output_path}'")
    return str(output_path)
