import aiohttp
import asyncio
from datetime import datetime, timezone
import json
import requests
import re
import time

import pandas as pd

class RequestFailureException(Exception):
    """Raised when all retry attempts for a request fail."""
    pass


async def make_search_requests(CONFIG, project_id: int, query_object_type: str, query: str, return_fields: list[str] = None):
    """Perform concurrent, paginated qTest search requests with retries and return results as a DataFrame."""

    if return_fields is None:
        return_fields = ["*"]  # Default to all fields

    logger = CONFIG["logger"]
    base_url = f"{CONFIG['qtest_domain']}api/v3/projects/{project_id}/search"
    
    headers = {
        "Authorization": CONFIG['auth']['qtest']['qTest_bearer_token'].get(),
        "Content-Type": "application/json"
    }

    
    params = {
        "includeExternalProperties": "false",
        "includeTestLogProperties": "false",
        "pageSize": CONFIG.get("request_page_size", 100),
        "page": 1
    }

    request_body = {
        "object_type": query_object_type,
        "fields": return_fields,
        "query": query
    }
    
    max_retries = CONFIG.get("request_retries", 3)

    async def fetch_with_retries(url, headers, json_data, params):
        """Perform a POST request with retry logic for transient server errors."""

        for attempt in range(1, max_retries + 1):
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=json_data, params=params) as response:
                    response_text = await response.text()
                    
                    if response.status == 200:
                        return await response.json()
                    
                    elif 500 <= response.status < 600:
                        logger.warning(f"Server error {response.status} on attempt {attempt}/{max_retries}. Retrying...")
                        if attempt == max_retries:
                            error_message = (
                                f"CRITICAL ERROR: Max retries reached for {url}.\n"
                                f"Response Code: {response.status}\n"
                                f"Response Text: {response_text}\n"
                                f"Request Body: {json.dumps(json_data, indent=4)}\n"
                                f"Parameters: {json.dumps(params, indent=4)}"
                            )
                            logger.critical(error_message)
                            raise RequestFailureException(error_message)
                        await asyncio.sleep(2 ** attempt)
                    
                    else:
                        logger.error(f"Request failed with status {response.status}: {response_text}")
                        raise RequestFailureException(f"Request failed with status {response.status}: {response_text}")

    logger.info(f"Making initial search request to {base_url}")
    logger.info(f"Request body:\n{json.dumps(request_body, indent=4)}")

    first_page_data = await fetch_with_retries(base_url, headers, request_body, params)
    if not first_page_data:
        raise RequestFailureException(f"Failed to fetch initial page from {base_url}. No data returned.")

    total_records = first_page_data.get("total", 0)
    page_size = params["pageSize"]
    total_pages = (total_records + page_size - 1) // page_size

    results = first_page_data.get("items", [])
    logger.info(f"Received {len(results)} records from page 1. Total records expected: {total_records}")

    if total_pages > 1:
        max_concurrent_requests = CONFIG.get("max_concurrent_requests", 5)
        logger.info(f"Making {total_pages-1} additional requests with concurrency limit of {max_concurrent_requests}")
        semaphore = asyncio.Semaphore(max_concurrent_requests)

        async def fetch_page(page):
            async with semaphore:
                params["page"] = page
                logger.info(f"Fetching page {page}")
                page_data = await fetch_with_retries(base_url, headers, request_body, params)
                
                if page_data:
                    num_records = len(page_data.get("items", []))
                    logger.info(f"Successfully fetched page {page}, records retrieved: {num_records}")
                    return page_data.get("items", [])
                return []

        tasks = [fetch_page(p) for p in range(2, total_pages + 1)]
        additional_results = await asyncio.gather(*tasks, return_exceptions=True)

        for page_data in additional_results:
            if isinstance(page_data, Exception):  
                raise page_data
            results.extend(page_data)

    logger.info(f"Total search records retrieved: {len(results)}")
    return pd.DataFrame(results)


async def extract_test_cases(CONFIG, test_case_pids):
    """Chunk and asynchronously fetch test case details from qTest using search API."""

    if not isinstance(test_case_pids, list) or not all(isinstance(pid, str) for pid in test_case_pids):
        raise ValueError("test_case_pids must be a list of strings.")

    chunk_size = CONFIG.get("case_search_chunk_size", 50)
    logger = CONFIG.get("logger")
    project_id = CONFIG.get("qtest_project_id")

    search_queries = [f"'id' = '{pid}'" for pid in test_case_pids]
    search_queries = [f" or ".join(search_queries[i:i + chunk_size]) for i in range(0, len(search_queries), chunk_size)]

    logger.info(f"Attempting {len(search_queries)} test-case search requests with chunk_size of {chunk_size}")

    async def fetch_search_results(search_query):
        search_results_df = await make_search_requests(CONFIG, project_id, 'test-cases', search_query)
        search_results_df = search_results_df.astype(str)

        if not search_results_df.empty:
            # Drop unwanted columns if they exist
            search_results_df = search_results_df.drop(
                columns=["links", "properties", "web_url", "attachments"], errors="ignore"
            )

            search_results_df['test_case_version_id'] = search_results_df['test_case_version_id'].astype(int)
            search_results_df["project_id"] = project_id
            logger.info(f"Test-case Search API returned {len(search_results_df)} records.")
            return search_results_df

        return None

    tasks = [fetch_search_results(search_query) for search_query in search_queries]
    results = await asyncio.gather(*tasks)

    dfs = [df for df in results if df is not None]

    if not dfs:
        logger.warning("No test cases found.")
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def search_qTest_for_test_cases(CONFIG, test_case_pids):
    """Sync wrapper for `extract_test_cases` to return test cases as DataFrame."""
    if not test_case_pids:
        CONFIG["logger"].info("No test case PIDs provided.")
        return pd.DataFrame()

    return asyncio.run(extract_test_cases(CONFIG, test_case_pids))


def create_test_suite(CONFIG):
    """Create a test suite in qTest under the specified parent."""
    project_id = CONFIG.get("qtest_project_id")
    parent_id = CONFIG.get("suite_parent_id")
    parent_type = CONFIG.get("suite_parent_type", "test-cycle")
    suite_name = CONFIG.get("suite_name")

    logger = CONFIG["logger"]
    base_url = f"{CONFIG['qtest_domain']}api/v3/projects/{project_id}/test-suites"
    url = f"{base_url}?parentId={parent_id}&parentType={parent_type}"

    headers = {
        "Authorization": CONFIG['auth']['qtest']['qTest_bearer_token'].get(),
        "Content-Type": "application/json"
    }

    request_body = {"name": suite_name}
    max_retries = CONFIG.get("request_retries", 3)
    delay_seconds = 1

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"[Attempt {attempt}/{max_retries}] Creating test suite: '{suite_name}' under parent ID {parent_id} ({parent_type})")
            response = requests.post(url, headers=headers, json=request_body, verify=False)

            if response.status_code == 200:
                suite_data = response.json()
                suite_id = suite_data.get("id")
                if suite_id:
                    logger.info(f"Successfully created test suite '{suite_name}' with ID: {suite_id}")
                    return suite_id
                else:
                    logger.warning(f"Attempt {attempt} succeeded but response did not contain suite ID: {suite_data}")
            else:
                logger.warning(f"Attempt {attempt} failed: {response.status_code} - {response.text}")

        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt} exception during suite creation: {e}")

        if attempt < max_retries:
            time.sleep(delay_seconds)
            delay_seconds *= 2
        else:
            logger.error(f"Final failure creating suite '{suite_name}' after {max_retries} attempts.")
            logger.error(f"Last response: {getattr(response, 'text', 'No response')}")

    raise Exception(f"Failed to create suite '{suite_name}' after {max_retries} attempts.")


def create_test_runs(CONFIG, suite_id, valid_case_df):
    """Create test runs in qTest for each test case in the DataFrame."""

    project_id = CONFIG.get("qtest_project_id")
    parent_id = suite_id
    parent_type = "test-suite"

    logger = CONFIG["logger"]
    base_url = f"{CONFIG['qtest_domain']}api/v3/projects/{project_id}/test-runs"
    url = f"{base_url}?parentId={parent_id}&parentType={parent_type}"

    headers = {
        "Authorization": CONFIG['auth']['qtest']['qTest_bearer_token'].get(),
        "Content-Type": "application/json"
    }

    created_runs = []

    for _, record in valid_case_df.iterrows():
        test_run_name = record.get("test_run_name")
        test_case_id = record.get("test_case_id")
        test_case_version_id = record.get("test_case_version_id")

        if not test_run_name or pd.isna(test_case_id):
            logger.warning(f"Skipping record — missing test_run_name or test_case_id (PID: {record.get('test_case_pid')})")
            continue

        request_body = {
            "name": test_run_name,
            "test_case": {
                "id": int(test_case_id),
                "test_case_version_id": int(test_case_version_id)
            }
        }

        try:
            response = requests.post(url, headers=headers, json=request_body, verify=False)

            if response.status_code == 201:
                run_data = response.json()
                logger.info(f"Created test run '{test_run_name}' for test_case_id {test_case_id} (run_id: {run_data['id']})")
                created_runs.append({
                    "test_case_pid": record.get("test_case_pid"),
                    "test_case_id": test_case_id,
                    "test_run_name": test_run_name,
                    "test_run_id": run_data["id"]
                })
            else:
                logger.error(f"Failed to create test run '{test_run_name}': {response.status_code} - {response.text}")

        except Exception as e:
            logger.exception(f"Exception while creating test run for case {test_case_id}: {e}")

    return pd.DataFrame(created_runs)


def get_case_versions(CONFIG, case_id):
    """Get all versions of a test case from qTest, retrying on failure."""

    project_id = CONFIG["qtest_project_id"]
    logger = CONFIG["logger"]

    url = f"{CONFIG['qtest_domain']}api/v3/projects/{project_id}/test-cases/{case_id}/versions?showParamIdentifier=false"

    headers = {
        "Authorization": CONFIG['auth']['qtest']['qTest_bearer_token'].get(),
        "Content-Type": "application/json"
    }

    max_retries = CONFIG.get("request_retries", 3)

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"[Attempt {attempt}/{max_retries}] Querying versions for test case ID: {case_id}")
            response = requests.get(url, headers=headers, verify=False)

            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Attempt {attempt} failed for case ID {case_id}: {response.status_code} - {response.text}")
                if attempt == max_retries:
                    logger.error(f"Final failure fetching versions for case ID {case_id} after {max_retries} attempts.")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt} exception for case ID {case_id}: {e}")
            if attempt == max_retries:
                logger.exception(f"Final exception fetching versions for case ID {case_id} after {max_retries} attempts.")

    return []  # Fallback if all retries fail


def get_steps_by_case_version(CONFIG, case_id, version_id):
    """Retrieve test steps for a specific test case version."""

    project_id = CONFIG["qtest_project_id"]
    logger = CONFIG["logger"]

    url = (
        f"{CONFIG['qtest_domain']}api/v3/projects/{project_id}/test-cases/"
        f"{case_id}/versions/{version_id}/test-steps?showParamIdentifier=false"
    )

    headers = {
        "Authorization": CONFIG['auth']['qtest']['qTest_bearer_token'].get(),
        "Content-Type": "application/json"
    }

    max_retries = CONFIG.get("request_retries", 3)

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"[Attempt {attempt}/{max_retries}] Querying steps for case ID {case_id}, version ID {version_id}")
            response = requests.get(url, headers=headers, verify=False)

            if response.status_code == 200:
                steps = response.json()
                if not isinstance(steps, list):
                    logger.warning(f"Unexpected format for steps response (case ID: {case_id}, version ID: {version_id}): {steps}")
                    return []
                return steps
            else:
                logger.warning(f"Attempt {attempt} failed for steps (case ID {case_id}, version ID {version_id}): {response.status_code} - {response.text}")
                if attempt == max_retries:
                    logger.error(f"Final failure fetching steps for case ID {case_id}, version ID {version_id} after {max_retries} attempts.")

        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt} exception for steps (case ID {case_id}, version ID {version_id}): {e}")
            if attempt == max_retries:
                logger.exception(f"Final exception fetching steps for case ID {case_id}, version ID {version_id} after {max_retries} attempts.")

    return []  # Fallback if all retries fail


def execute_test_runs(CONFIG, valid_case_df, test_runs, test_case_step_df):
    """Log execution results for each test case run with step-level logs."""

    logger = CONFIG.get("logger")
    project_id = CONFIG["qtest_project_id"]
    result_code_mapping = CONFIG.get("execution_status_mapping", {})

    required_cols = {"test_case_id", "test_case_version_id", "test_case_pid", "test_result"}
    missing_cols = required_cols - set(valid_case_df.columns)
    if missing_cols:
        raise KeyError(f"Missing required columns in valid_case_df: {missing_cols}")

    for _, record in valid_case_df.iterrows():
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        pid = record["test_case_pid"]
        test_result = record["test_result"]

        result_code = result_code_mapping.get(test_result)
        if result_code is None:
            logger.warning(f"No result code mapping for result '{test_result}'. Skipping PID {pid}")
            continue

        # Get test_run_id
        run_row = test_runs[test_runs["test_case_pid"] == pid]
        if run_row.empty:
            logger.warning(f"No test run found for PID {pid}")
            continue

        test_run_id = run_row.iloc[0]["test_run_id"]

        # Get single test_step_id
        step_row = test_case_step_df[test_case_step_df["pid"] == pid]
        if step_row.empty:
            logger.warning(f"No test step found for PID {pid}")
            continue

        step_id = int(step_row.iloc[0]["step_id"])

        # Build body
        body = {
            "exe_start_date": timestamp,
            "exe_end_date": timestamp,
            "status": {
                "id": result_code,
                "name": test_result
            },
            "test_step_logs": [
                {
                    "test_step_id": step_id,
                    "status": {
                        "id": result_code,
                        "name": test_result
                    },
                    "actual_result": f"Automated test script has passed, and the location of the test result is included below:\n\n{record["pdf_file_path"]}"
                }
            ]
        }

        url = f"{CONFIG['qtest_domain']}api/v3/projects/{project_id}/test-runs/{test_run_id}/test-logs"
        headers = {
            "Authorization": CONFIG['auth']['qtest']['qTest_bearer_token'].get(),
            "Content-Type": "application/json"
        }

        max_retries = CONFIG.get("request_retries", 3)
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"[Attempt {attempt}/{max_retries}] Posting result for PID {pid} to test run {test_run_id}")
                response = requests.post(url, headers=headers, json=body, verify=False)

                if response.status_code == 201:
                    logger.info(f"Successfully posted result for PID {pid}")
                    break  # Exit retry loop on success
                else:
                    logger.warning(f"Attempt {attempt} failed for PID {pid}: {response.status_code} - {response.text}")
                    if attempt == max_retries:
                        logger.error(f"Final failure posting result for PID {pid} after {max_retries} attempts.")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt} exception for PID {pid}: {e}")
                if attempt == max_retries:
                    logger.exception(f"Final exception posting result for PID {pid} after {max_retries} attempts.")


def get_latest_approved_versions(CONFIG, test_case_df):
    """Replace version in test_case_df with latest approved (.0) version, if available."""

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
