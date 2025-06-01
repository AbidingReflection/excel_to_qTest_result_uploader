from pathlib import Path

from modules.load_config import load_config
from modules.load_excel import load_data_from_excel
from modules.write_excel import write_upload_results_to_excel

from modules.utils import update_upload_status_for_missing_cases, update_upload_status_for_cases_step_count, \
                          add_case_id, unpack_case_steps, update_case_steps
from modules.qtest_extract import search_qTest_for_test_cases, create_test_suite, create_test_runs, \
                                  execute_test_runs, get_latest_approved_versions

def run_pipeline(CONFIG):    

    auto_result_df = load_data_from_excel(CONFIG)

    print(auto_result_df["Upload Status"])

    test_case_pids = list(
        auto_result_df.loc[auto_result_df["Upload Status"].str.strip() == "", "test_case_pid"]
        .astype(str)
        .unique()
    )

    test_case_df = search_qTest_for_test_cases(CONFIG, test_case_pids)
    test_case_df, updated_cases_df = get_latest_approved_versions(CONFIG, test_case_df)
    test_case_step_df = unpack_case_steps(CONFIG, test_case_df)
    test_case_step_df = update_case_steps(CONFIG, updated_cases_df, test_case_step_df)

    update_upload_status_for_missing_cases(CONFIG, auto_result_df, test_case_df)
    update_upload_status_for_cases_step_count(CONFIG, auto_result_df, test_case_df, test_case_step_df)
    add_case_id(CONFIG, test_case_df, auto_result_df)

    suite_id = create_test_suite(CONFIG)

    valid_case_df = auto_result_df[auto_result_df["Upload Status"].str.strip() == ""]
    test_runs = create_test_runs(CONFIG, suite_id, valid_case_df)
    execute_test_runs(CONFIG, valid_case_df, test_runs, test_case_step_df)

    write_upload_results_to_excel(CONFIG, auto_result_df, test_case_df, test_case_step_df, test_runs)

if __name__ == "__main__":
    try:
        CONFIG = load_config(Path("configs") / "apitryout.yaml")
        run_pipeline(CONFIG)

    except Exception as e:
        logger = None
        try:
            if 'CONFIG' in locals():
                logger = CONFIG.get('logger')
        except Exception:
            pass

        if logger:
            logger.exception("Fatal error during apitryout pipeline")
        else:
            print(f"Fatal error during apitryout pipeline: {e}")
