import re
from pathlib import Path
import pandas as pd

def load_data_from_excel(CONFIG):
    """Load and validate data from an Excel file based on CONFIG settings."""

    def _validate_required_config_keys(CONFIG, required_keys):
        """Ensure all required keys exist in the config."""
        missing = [k for k in required_keys if k not in CONFIG]
        if missing:
            raise KeyError(f"Missing required config keys: {missing}")

    def _load_and_prepare_excel(CONFIG):
        """Load Excel sheet and apply column renaming based on mapping."""
        excel_path = Path(CONFIG["excel_path"])
        sheet_name = CONFIG["excel_tab_name"]
        column_map = CONFIG["excel_column_mapping"]

        if not excel_path.exists():
            raise FileNotFoundError(f"Excel file not found: {excel_path}")

        try:
            xls = pd.ExcelFile(excel_path)
        except Exception as e:
            raise ValueError(f"Failed to read Excel file '{excel_path}': {e}")

        if sheet_name not in xls.sheet_names:
            raise ValueError(f"Sheet '{sheet_name}' not found. Available sheets: {xls.sheet_names}")

        df_raw = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
        missing_cols = [col for col in column_map.keys() if col not in df_raw.columns]
        if missing_cols:
            raise ValueError(f"Missing expected columns in Excel sheet: {missing_cols}")

        return df_raw.rename(columns=column_map)

    def _add_upload_status_column(df):
        """Insert an empty 'Upload Status' column at the front of the DataFrame."""
        df["Upload Status"] = ""
        reordered_cols = ["Upload Status"] + [col for col in df.columns if col != "Upload Status"]
        return df[reordered_cols]

    def _append_status(df, idx, msg):
        """Append a status message to the Upload Status column."""
        current = str(df.at[idx, "Upload Status"]).strip()
        df.at[idx, "Upload Status"] = f"{current}\n{msg}" if current else msg

    def _validate_each_row(df):
        """Run all row-level validation and annotation checks."""
        pid_pattern = re.compile(r"^TC-[1-9][0-9]{0,9}$")
        for idx, row in df.iterrows():
            _check_pdf_path(row, df, idx)
            _check_test_case_pid(row, df, idx, pid_pattern)
            _parse_test_result(row, df, idx)
            _check_unapproved_version(row, df, idx)

    def _check_pdf_path(row, df, idx):
        """Validate the PDF file path exists; annotate upload status if missing or bad."""
        raw_path = row.get("pdf_file_path")
        if not raw_path:
            _append_status(df, idx, "Missing PDF path")
            return

        clean_path_str = re.sub(r'^[\'"]|[\'"]$', '', raw_path.strip().replace('\xa0', ''))
        path = Path(clean_path_str)
        if not path.exists():
            alt_path = Path(clean_path_str.replace("/", "\\"))
            if not alt_path.exists():
                _append_status(df, idx, f"PDF not found: {raw_path}")

    def _check_test_case_pid(row, df, idx, pid_pattern):
        """Validate test case PID format."""
        pid = row.get("test_case_pid", "").strip()
        if not pid_pattern.match(pid):
            _append_status(df, idx, f"Invalid PID format: {pid}")

    def _parse_test_result(row, df, idx):
        """Derive and annotate test result based on raw result string."""
        raw = str(row.get("raw_test_result", "")).strip().lower()
        has_passed = "passed" in raw
        has_failed = "failed" in raw

        if has_failed:
            result = "Failed"
        elif has_passed:
            result = "Passed"
        else:
            result = "N/A"
            _append_status(df, idx, "Result could not be determined from raw_test_result")

        df.at[idx, "test_result"] = result

    def _check_unapproved_version(row, df, idx):
        """Check if version is a pre-release (e.g., 0.x) and annotate."""
        version = str(row.get("version", ""))
        if version.startswith("0."):
            _append_status(df, idx, "Unapproved version (starts with 0)")

    logger = CONFIG.get("logger")

    _validate_required_config_keys(CONFIG, ["excel_path", "excel_tab_name", "excel_column_mapping"])
    df = _load_and_prepare_excel(CONFIG)
    df = _add_upload_status_column(df)
    _validate_each_row(df)

    logger.info(f"Loaded {len(df)} records from '{CONFIG['excel_path']}'")
    return df
