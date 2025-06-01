import os
from datetime import datetime
from pathlib import Path
import pandas as pd
import re
import ast


import pandas as pd
from pathlib import Path
from pprint import pprint
from config_env_initializer.config_loader import ConfigLoader
from modules.qtest_extract import search_qTest_for_test_cases, create_test_suite, create_test_runs, get_case_versions, get_steps_by_case_version, execute_test_runs


def load_config():
    config_path = r"configs\apitryout.yaml"

    loader = ConfigLoader(config_path)
    CONFIG = loader.config
    logger = CONFIG['logger']

    print("\nLoaded and validated config:")
    pprint(CONFIG, indent=4)
    return CONFIG


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
    _add_upload_status_column(df)
    _validate_each_row(df)

    logger.info(f"Loaded {len(df)} records from '{CONFIG['excel_path']}'")
    return df


def update_upload_status_for_missing_cases(CONFIG, auto_result_df, test_case_df):
    existing_pids = set(test_case_df["pid"].astype(str))
    project_id = CONFIG["qtest_project_id"]

    for idx, row in auto_result_df.iterrows():
        pid = str(row.get("test_case_pid", "")).strip()
        if pid and pid not in existing_pids:
            message = f"Test case '{pid}' not found in project {project_id}"
            if auto_result_df.at[idx, "Upload Status"]:
                auto_result_df.at[idx, "Upload Status"] += f"\n{message}"
            else:
                auto_result_df.at[idx, "Upload Status"] = message


def update_upload_status_for_cases_step_count(CONFIG, auto_result_df, test_case_df, test_case_step_df):
    logger = CONFIG.get("logger")

    # Count steps per PID
    step_counts = test_case_step_df["pid"].value_counts()

    for _, case_row in test_case_df.iterrows():
        pid = case_row.get("pid")
        if not pid:
            continue

        count = step_counts.get(pid, 0)

        if count == 0:
            message = "No test steps found"
        elif count > 1:
            message = f"Multiple test steps found ({count})"
        else:
            continue  # 1 step is expected, no message needed

        # Update Upload Status for all matching rows in auto_result_df
        matching_rows = auto_result_df[auto_result_df["test_case_pid"] == pid]
        for idx in matching_rows.index:
            existing = auto_result_df.at[idx, "Upload Status"]
            auto_result_df.at[idx, "Upload Status"] = f"{existing}\n{message}" if existing else message

    logger.info("Upload statuses updated based on step count.")


def add_case_id(CONFIG, test_case_df, auto_result_df):
    logger = CONFIG.get("logger")

    # Build lookup dictionaries
    pid_to_id = dict(zip(test_case_df["pid"], test_case_df["id"]))
    pid_to_version_id = dict(zip(test_case_df["pid"], test_case_df["test_case_version_id"]))

    # Apply lookups to auto_result_df
    auto_result_df["test_case_id"] = auto_result_df["test_case_pid"].map(pid_to_id)
    auto_result_df["test_case_version_id"] = auto_result_df["test_case_pid"].map(pid_to_version_id)

    # Log how many were successfully mapped
    matched = auto_result_df["test_case_id"].notna().sum()
    total = len(auto_result_df)
    logger.info(f"Mapped test_case_id and test_case_version_id for {matched} of {total} records.")



def unpack_case_steps(CONFIG, case_df):
    logger = CONFIG.get("logger")

    if "test_steps" not in case_df.columns:
        raise ValueError("Column 'test_steps' not found in case_df")

    step_records = []

    for _, row in case_df.iterrows():
        test_case_pid = row.get("pid")
        test_case_id = row.get("id")
        test_case_version_id = row.get("test_case_version_id")
        project_id = row.get("project_id")

        test_steps = row.get("test_steps")

        # 🔁 Attempt to convert stringified list if needed
        if isinstance(test_steps, str):
            try:
                test_steps = ast.literal_eval(test_steps)
            except Exception:
                logger.warning(f"Skipping case '{test_case_pid}' — 'test_steps' could not be parsed")
                continue

        if not isinstance(test_steps, list):
            logger.warning(f"Skipping case '{test_case_pid}' — 'test_steps' not a list")
            continue

        for step in test_steps:
            step_id = step.get("id")
            order = step.get("order")
            description = step.get("description", "")
            expected = step.get("expected", "")
            plain_text = step.get("plain_value_text", "")

            step_records.append({
                "pid": test_case_pid,
                "test_case_id": test_case_id,
                "test_case_version_id": test_case_version_id,
                "project_id": project_id,
                "step_id": step_id,
                "step_order": order,
                "description": description,
                "expected": expected,
                "plain_value_text": plain_text,
            })

    if not step_records:
        logger.warning("No test steps unpacked. Resulting DataFrame will be empty.")

    return pd.DataFrame(step_records)


def update_case_steps(CONFIG, updated_cases_df, test_case_step_df):


    logger = CONFIG["logger"]

    # Ensure 'pid' columns are strings for matching
    updated_cases_df["pid"] = updated_cases_df["pid"].astype(str)
    test_case_step_df["pid"] = test_case_step_df["pid"].astype(str)

    # Remove outdated steps for updated cases
    step_df_filtered = test_case_step_df[
        ~test_case_step_df["pid"].isin(updated_cases_df["pid"])
    ]

    logger.info(f"Removed {len(test_case_step_df) - len(step_df_filtered)} outdated step records.")

    new_step_rows = []

    for _, row in updated_cases_df.iterrows():
        pid = row["pid"]
        case_id = row["id"]
        version_id = row["test_case_version_id"]

        logger.info(f"Fetching updated steps for PID {pid} (ID: {case_id}, Version ID: {version_id})")
        steps = get_steps_by_case_version(CONFIG, case_id, version_id)

        for step in steps:
            new_step_rows.append({
                "pid": pid,
                "test_case_id": case_id,
                "test_case_version_id": version_id,
                "project_id": CONFIG.get("qtest_project_id"),
                "step_id": step.get("id"), 
                "step_order": step.get("order"),
                "description": step.get("description", ""),
                "expected": step.get("expected", ""),
                "plain_value_text": step.get("plain_value_text", ""),
                # "raw": step
            })




    new_step_df = pd.DataFrame(new_step_rows)
    logger.info(f"Retrieved {len(new_step_df)} new step records.")

    # Combine the cleaned old steps and the new ones
    combined_df = pd.concat([step_df_filtered, new_step_df], ignore_index=True)

    return combined_df


def get_latest_approved_versions(CONFIG, test_case_df):
    logger = CONFIG["logger"]
    updated_rows = []

    # Check if test_case_df is empty and raise an error if it is
    if test_case_df.empty:
        raise ValueError("The test_case_df is empty. Cannot proceed with updates.")

    # Extract all rows where version does NOT end in .0
    needs_update = test_case_df[~test_case_df["version"].astype(str).str.endswith(".0")]

    logger.info(f"Found {len(needs_update)} test cases that do not end with .0 - checking for latest approved versions.")

    for _, row in needs_update.iterrows():
        case_id = row["id"]
        pid = row["pid"]

        versions = get_case_versions(CONFIG, case_id)
        approved_versions = [v for v in versions if str(v.get("version", "")).endswith(".0")]

        if not approved_versions:
            logger.warning(f"No approved (.0) versions found for test case ID {case_id} (PID: {pid})")
            continue

        def parse_version(vstr):
            match = re.match(r"(\d+)\.0", str(vstr))
            return int(match.group(1)) if match else -1

        approved_versions.sort(key=lambda v: parse_version(v["version"]), reverse=True)
        latest = approved_versions[0]

        logger.info(f"Replacing case ID {case_id} (v{row['version']}) with v{latest['version']}")

        new_row = row.copy()
        new_row["id"] = latest["id"]
        new_row["version"] = latest["version"]
        new_row["test_case_version_id"] = latest["test_case_version_id"]
        updated_rows.append(new_row)

    if updated_rows:
        updated_df = test_case_df.copy()
        updated_pids = []

        for row in updated_rows:
            match = updated_df[updated_df["pid"] == row["pid"]]
            if not match.empty:
                idx = match.index[0]
                updated_df.loc[idx, ["id", "version", "test_case_version_id"]] = row[["id", "version", "test_case_version_id"]]
                updated_pids.append(row["pid"])

        updated_cases_df = updated_df[updated_df["pid"].isin(updated_pids)].copy()
        return updated_df, updated_cases_df

    else:
        return test_case_df, pd.DataFrame(columns=test_case_df.columns)




def write_upload_results_to_excel(df, CONFIG):
    # Validate necessary CONFIG keys
    required_keys = ['suite_name', 'output_dir']
    missing = [key for key in required_keys if key not in CONFIG]
    if missing:
        raise KeyError(f"Missing required config keys: {missing}")

    suite_name = CONFIG['suite_name']
    output_dir = Path(CONFIG['output_dir'])

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Format file name
    timestamp_str = datetime.now().strftime('%Y_%m_%d_%H%M%S')
    safe_suite_name = "".join(c for c in suite_name if c.isalnum() or c in " _-").rstrip()
    base_filename = f"{safe_suite_name} - Upload Results - {timestamp_str}.xlsx"
    output_path = output_dir / base_filename

    # Write to Excel with specified sheet name
    try:
        df.to_excel(output_path, index=False, sheet_name="AUTO Upload Results")
    except Exception as e:
        raise IOError(f"Failed to write Excel file: {e}")

    CONFIG['logger'].info(f"Wrote upload results to: {output_path}")
    return str(output_path)


if __name__ == "__main__":
    CONFIG = load_config()

    import pdb; pdb.set_trace()


    auto_result_df = load_data_from_excel(CONFIG)
    

    test_case_pids = list(
        auto_result_df.loc[auto_result_df["Upload Status"].str.strip() == "", "test_case_pid"]
        .astype(str)
        .unique()
    )

    test_case_df = search_qTest_for_test_cases(CONFIG, test_case_pids)
    print(test_case_df)
    test_case_df, updated_cases_df = get_latest_approved_versions(CONFIG, test_case_df)
    test_case_step_df = unpack_case_steps(CONFIG, test_case_df)
    test_case_step_df = update_case_steps(CONFIG, updated_cases_df, test_case_step_df)


    update_upload_status_for_missing_cases(CONFIG, auto_result_df, test_case_df)
    update_upload_status_for_cases_step_count(CONFIG, auto_result_df, test_case_df, test_case_step_df)
    add_case_id(CONFIG, test_case_df, auto_result_df)
    

    # Create test-suite
    suite_id = create_test_suite(CONFIG)

    # # Create test-runs
    valid_case_df = auto_result_df[auto_result_df["Upload Status"].str.strip() == ""]
    test_runs = create_test_runs(CONFIG, suite_id, valid_case_df)

    with pd.ExcelWriter("debug_qtest_results.xlsx", engine="openpyxl") as writer:
        auto_result_df.to_excel(writer, sheet_name="Targets", index=False)
        test_case_df.to_excel(writer, sheet_name="Test Cases", index=False)
        test_case_step_df.to_excel(writer, sheet_name="Test Steps", index=False)
        test_runs.to_excel(writer, sheet_name="Created Test Runs", index=False)

    # Execute test-runs
    test_logs = execute_test_runs(CONFIG, valid_case_df, test_runs, test_case_step_df)

    write_upload_results_to_excel(auto_result_df, CONFIG)


