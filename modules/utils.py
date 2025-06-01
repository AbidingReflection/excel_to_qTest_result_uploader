import pandas as pd
import ast
from modules.qtest_extract import get_steps_by_case_version

def update_upload_status_for_missing_cases(CONFIG, auto_result_df, test_case_df):
    """Mark cases in auto_result_df as missing if not found in test_case_df."""
    existing_pids = set(test_case_df["pid"].astype(str))
    project_id = CONFIG["qtest_project_id"]

    for idx, row in auto_result_df.iterrows():
        pid = str(row.get("test_case_pid", "")).strip()
        if pid and pid not in existing_pids:
            message = f"Test case '{pid}' not found in project {project_id}"
            current = auto_result_df.at[idx, "Upload Status"]
            auto_result_df.at[idx, "Upload Status"] = f"{current}\n{message}" if current else message

def update_upload_status_for_cases_step_count(CONFIG, auto_result_df, test_case_df, test_case_step_df):
    """Update Upload Status for test cases with 0 or multiple steps."""
    logger = CONFIG.get("logger")
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
            continue  # Expected case, do nothing

        matching_rows = auto_result_df[auto_result_df["test_case_pid"] == pid]
        for idx in matching_rows.index:
            current = auto_result_df.at[idx, "Upload Status"]
            auto_result_df.at[idx, "Upload Status"] = f"{current}\n{message}" if current else message

    logger.info("Upload statuses updated based on step count.")

def add_case_id(CONFIG, test_case_df, auto_result_df):
    """Map test_case_id and version_id into auto_result_df using PID keys."""
    logger = CONFIG.get("logger")

    pid_to_id = dict(zip(test_case_df["pid"], test_case_df["id"]))
    pid_to_version_id = dict(zip(test_case_df["pid"], test_case_df["test_case_version_id"]))

    auto_result_df["test_case_id"] = auto_result_df["test_case_pid"].map(pid_to_id)
    auto_result_df["test_case_version_id"] = auto_result_df["test_case_pid"].map(pid_to_version_id)

    matched = auto_result_df["test_case_id"].notna().sum()
    logger.info(f"Mapped test_case_id and test_case_version_id for {matched} of {len(auto_result_df)} records.")

def unpack_case_steps(CONFIG, case_df):
    """Convert test step structures in case_df into a flattened step DataFrame."""
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
            step_records.append({
                "pid": test_case_pid,
                "test_case_id": test_case_id,
                "test_case_version_id": test_case_version_id,
                "project_id": project_id,
                "step_id": step.get("id"),
                "step_order": step.get("order"),
                "description": step.get("description", ""),
                "expected": step.get("expected", ""),
                "plain_value_text": step.get("plain_value_text", ""),
            })

    if not step_records:
        logger.warning("No test steps unpacked. Resulting DataFrame will be empty.")

    return pd.DataFrame(step_records)

def update_case_steps(CONFIG, updated_cases_df, test_case_step_df):
    """Fetch updated test steps and return a combined DataFrame with latest info."""
    logger = CONFIG["logger"]

    updated_cases_df["pid"] = updated_cases_df["pid"].astype(str)
    test_case_step_df["pid"] = test_case_step_df["pid"].astype(str)

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
            })

    new_step_df = pd.DataFrame(new_step_rows)
    logger.info(f"Retrieved {len(new_step_df)} new step records.")

    return pd.concat([step_df_filtered, new_step_df], ignore_index=True)
