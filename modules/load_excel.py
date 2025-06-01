import re
from pathlib import Path
import pandas as pd

def load_data_from_excel(CONFIG):
    def _validate_required_config_keys(CONFIG, required_keys):
        missing = [k for k in required_keys if k not in CONFIG]
        if missing:
            raise KeyError(f"Missing required config keys: {missing}")

    def _load_and_prepare_excel(CONFIG):
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
        df["Upload Status"] = ""
        reordered_cols = ["Upload Status"] + [col for col in df.columns if col != "Upload Status"]
        return df[reordered_cols]



    def _validate_each_row(df):
        pid_pattern = re.compile(r"^TC-[1-9][0-9]{0,9}$")

        for idx, row in df.iterrows():
            _check_pdf_path(row, df, idx)
            _check_test_case_pid(row, df, idx, pid_pattern)
            _parse_test_result(row, df, idx)
            _check_unapproved_version(row, df, idx)


    def _check_pdf_path(row, df, idx):
        raw_path = row.get("pdf_file_path")
        if not raw_path:
            df.at[idx, "Upload Status"] = "Missing PDF path"
            return

        clean_path_str = re.sub(r'^[\'"]|[\'"]$', '', raw_path.strip().replace('\xa0', ''))
        path = Path(clean_path_str)
        if not path.exists():
            alt_path = Path(clean_path_str.replace("/", "\\"))
            if not alt_path.exists():
                df.at[idx, "Upload Status"] = f"PDF not found: {raw_path}"

    def _check_test_case_pid(row, df, idx, pid_pattern):
        pid = row.get("test_case_pid", "").strip()
        if not pid_pattern.match(pid):
            df.at[idx, "Upload Status"] = f"Invalid PID format: {pid}"

    def _parse_test_result(row, df, idx):
        raw = str(row.get("raw_test_result", "")).strip().lower()

        has_passed = "passed" in raw
        has_failed = "failed" in raw

        if has_failed:
            result = "Failed"
        elif has_passed:
            result = "Passed"
        else:
            result = "N/A"
            if df.at[idx, "Upload Status"]:
                df.at[idx, "Upload Status"] += "\nResult could not be determined from raw_test_result"
            else:
                df.at[idx, "Upload Status"] = "Result could not be determined from raw_test_result"


        df.at[idx, "test_result"] = result

    def _check_unapproved_version(row, df, idx):
        version = str(row.get("version", ""))
        if version.startswith("0."):
            current_status = str(df.at[idx, "Upload Status"]).strip()
            new_status = "Unapproved version (starts with 0)"
            if current_status:
                df.at[idx, "Upload Status"] = f"{current_status}\n{new_status}"
            else:
                df.at[idx, "Upload Status"] = new_status

    logger = CONFIG.get("logger")
    _validate_required_config_keys(CONFIG, ["excel_path", "excel_tab_name", "excel_column_mapping"])

    df = _load_and_prepare_excel(CONFIG)
    df = _add_upload_status_column(df)

    _validate_each_row(df)

    logger.info(f"Loaded {len(df)} records from '{CONFIG['excel_path']}'")
    return df
