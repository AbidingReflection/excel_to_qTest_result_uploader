from config_env_initializer.config_validator import CustomValidator

auth_systems = ["qTest"]
project_dirs = ["configs", "auth"]

sub_project_dirs = ["logs", "input", "output"]
sub_projects = ["AUTO_result_uploads"]


schema = {

    "excel_path": {
        "type": str,
        "required": True,
        "validators": ["valid_path_string"],
        "default": r"input\<REQUIRED>"
    },
    "excel_tab_name": {
        "type": str,
        "required": True,
        "validators": ["valid_excel_tab_name"],
        "default": "Sheet1"
    },
    "excel_column_mapping": {
        "type": dict,
        "required": True,
        "validators": [
            {
                "name": "dict_must_have_values",
                "required_values": [
                    "test_case_pid",
                    "test_run_name",
                    "pdf_file_path",
                    "raw_test_result"
                ]
            }
        ],
        "default": None
    },

    "suite_name": {
        "type": str,
        "required": True,
        "validators": [],
        "default": None
    },
    "suite_parent_id": {
        "type": int,
        "required": True,
        "validators": [{"name": "int_no_leading_zero"}],
        "default": None
    },


    "qtest_domain": {
        "type": str,
        "required": True,
        "validators": ["https_url_with_trailing_slash"],
        "default": None
    },
    "qtest_project_id": {
        "type": int,
        "required": True,
        "validators": [{"name": "int_no_leading_zero", "digits": 6}],
        "default": None
    },
    "qtest_auth_path": {
        "type": str,
        "required": False,
        "validators": ["valid_path_string"],
        "default": r"auth\qTest\example.yaml"
    },
    "output_dir": {
        "type": str,
        "required": False,
        "validators": ["valid_path_string"],
        "default": r"output\AUTO_result_uploads"
    },




    "log_dir": {
        "type": str,
        "required": False,
        "validators": ["valid_path_string"],
        "default": r"logs\AUTO_result_uploads"
    },
    "log_level": {
        "type": str,
        "required": False,
        "validators": ["log_level_valid"],
        "default": "INFO"
    },
    "log_microseconds": {
        "type": bool,
        "required": False,
        "validators": [],  
        "default": False
    },


    "request_timeout": {
        "type": int,
        "required": True,
        "validators": [{"name": "int_in_range", "min_value": 5, "max_value": 60}],
        "default": 10
    },
    "request_retries": {
        "type": int,
        "required": False,
        "validators": [{"name": "int_in_range", "min_value": 0, "max_value": 10}],
        "default": 3
    },
}
