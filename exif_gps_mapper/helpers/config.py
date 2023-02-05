import os

from yaml import safe_load, safe_dump


def read(config_path: str = "config/config.yaml") -> dict:

    # Read YAML file
    with open(config_path, 'r') as stream:
        config = safe_load(stream)

    return config


def add_config_filenames(config: dict) -> dict:

    # Create Data directories
    os.makedirs(config["db"]["dir"], exist_ok=True)

    data_tables = ["exif", "route", "exercise"]

    for table in data_tables:
        # File name and path
        file_name = f"{table}.parquet"
        table_path = os.path.join(os.getcwd(), config["db"]["dir"], file_name)

        # Add to config
        config["db"][table] = table_path

    return config


def write(config: dict, filename: str):
    with open(filename, "w+") as f:
        safe_dump(config, f, default_flow_style=False)
