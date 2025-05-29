import aiohttp
import asyncio
import json
import pandas as pd
import requests
from datetime import datetime, timezone


class RequestFailureException(Exception):
    """Custom exception for request failures after all retries."""
    pass


async def make_search_requests(CONFIG, project_id: int, query_object_type: str, query: str, return_fields: list[str] = None):
    """Executes paginated search requests with retries and concurrency, returning results as a DataFrame."""

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
    """Synchronous wrapper to fetch test cases."""
    if not test_case_pids:
        CONFIG["logger"].info("No test case PIDs provided.")
        return pd.DataFrame()

    return asyncio.run(extract_test_cases(CONFIG, test_case_pids))


def create_test_suite(CONFIG):
    project_id = CONFIG.get("qtest_project_id")
    parent_id = CONFIG.get("suite_parent_id")
    parent_type = CONFIG.get("suite_parent_type", "test-cycle")  # fallback to "test-cycle"
    suite_name = CONFIG.get("suite_name")

    logger = CONFIG["logger"]
    base_url = f"{CONFIG['qtest_domain']}api/v3/projects/{project_id}/test-suites"
    url = f"{base_url}?parentId={parent_id}&parentType={parent_type}"

    headers = {
        "Authorization": CONFIG['auth']['qtest']['qTest_bearer_token'].get(),
        "Content-Type": "application/json"
    }

    request_body = {
        "name": suite_name
    }

    logger.info(f"Creating test suite: '{suite_name}' under parent ID {parent_id} ({parent_type})")

    response = requests.post(url, headers=headers, json=request_body)

    if response.status_code == 200:
        suite_data = response.json()
        suite_id = suite_data.get("id")
        logger.info(f"Created test suite '{suite_name}' with ID: {suite_id}")
        return suite_id
    else:
        logger.error(f"Failed to create test suite. Status {response.status_code}: {response.text}")
        raise Exception(f"Create suite failed ({response.status_code}) — {response.text}")


import requests
import pandas as pd

def create_test_runs(CONFIG, suite_id, valid_case_df):
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
            response = requests.post(url, headers=headers, json=request_body)

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
    project_id = CONFIG["qtest_project_id"]
    logger = CONFIG["logger"]
    
    url = f"{CONFIG['qtest_domain']}api/v3/projects/{project_id}/test-cases/{case_id}/versions?showParamIdentifier=false"
    
    headers = {
        "Authorization": CONFIG['auth']['qtest']['qTest_bearer_token'].get(),
        "Content-Type": "application/json"
    }

    logger.info(f"Querying versions for test case ID: {case_id}")
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()  # Should be a list of version objects
    else:
        logger.error(f"Failed to fetch versions for test case {case_id}: {response.status_code} - {response.text}")
        return []



def get_steps_by_case_version(CONFIG, case_id, version_id):
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

    logger.info(f"Querying steps for case ID {case_id}, version ID {version_id}")

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        steps = response.json()

        if not isinstance(steps, list):
            logger.warning(f"Unexpected format for steps response (case ID: {case_id}, version ID: {version_id}): {steps}")
            return []

        return steps

    except requests.RequestException as e:
        logger.error(f"Failed to retrieve steps for case ID {case_id}, version ID {version_id}: {e}")
        return []


def execute_test_runs(CONFIG, valid_case_df, test_runs, test_case_step_df):
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
                    "actual_result": f"Automated testing has {test_result}."
                }
            ]
        }

        url = f"{CONFIG['qtest_domain']}api/v3/projects/{project_id}/test-runs/{test_run_id}/test-logs"
        headers = {
            "Authorization": CONFIG['auth']['qtest']['qTest_bearer_token'].get(),
            "Content-Type": "application/json"
        }

        try:
            logger.info(f"Posting result for PID {pid} to test run {test_run_id}")
            response = requests.post(url, headers=headers, json=body)

            if response.status_code == 201:
                logger.info(f"Successfully posted result for PID {pid}")
            else:
                logger.error(f"Failed to post result for PID {pid}: {response.status_code} - {response.text}")

        except Exception as e:
            logger.exception(f"Exception while posting result for PID {pid}: {e}")
