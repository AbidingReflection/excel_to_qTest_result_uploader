from config_env_initializer.config_validator import CustomValidator

@CustomValidator.register()
def string_in_string(value, *, input_str, key=None):
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string.")
    if input_str not in value:
        raise ValueError(f"{key} must contain the substring '{input_str}'. Got: '{value}'")


project_dirs = ["configs", "auth"]
sub_project_dirs = ["logs", "input", "output"]
sub_projects = ["AUTO_result_uploads"]
auth_systems = ["qTest"]

schema = {

    "excel_path": {
        "type": str,
        "required": True,
        "validators": [],
        "default": r"input\<REQUIRED>"
    },
    "excel_tab_name": {
        "type": str,
        "required": True,
        "validators": [],
        "default": "sheet1"
    },
    "excel_column_mapping": {
        "type": dict,
        "required": True,
        "validators": [],
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
        "validators": [],
        "default": None
    },


    "qtest_domain": {
        "type": str,
        "required": True,
        "validators": [],
        "default": None
    },
    "qtest_project_id": {
        "type": int,
        "required": True,
        "validators": [],
        "default": None
    },
    "qtest_auth_path": {
        "type": str,
        "required": False,
        "validators": [],
        "default": r"auth\qTest\example.yaml"
    },


    "output_dir": {
        "type": str,
        "required": False,
        "validators": [],  
        "default": r"output\AUTO_result_uploads"
    },




    "log_dir": {
        "type": str,
        "required": False,
        "validators": [],
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
