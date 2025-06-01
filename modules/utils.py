import pandas as pd
import ast

from modules.qtest_extract import get_steps_by_case_version

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
