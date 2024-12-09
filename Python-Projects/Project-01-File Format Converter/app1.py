import os
import re
import sys
import glob
import json
import logging
import pandas as pd
from typing import List, Dict, Optional


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def validate_environment_variables():
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    tgt_base_dir = os.environ.get('TGT_BASE_DIR')
    if not src_base_dir or not tgt_base_dir:
        raise EnvironmentError("SRC_BASE_DIR and TGT_BASE_DIR must be set as environment variables.")
    return src_base_dir, tgt_base_dir


def get_column_names(schemas: Dict, ds_name: str, sorting_key: str = 'column_position') -> List[str]:
    if ds_name not in schemas:
        raise KeyError(f"Dataset name '{ds_name}' not found in schemas.")
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]


def read_csv(file: str, schemas: Dict) -> pd.DataFrame:
    try:
        file_path_list = re.split(r'[/\\]', file)
        ds_name = file_path_list[-2]
        columns = get_column_names(schemas, ds_name)
        return pd.read_csv(file, names=columns)
    except Exception as e:
        logging.error(f"Error reading CSV file {file}: {e}")
        raise


def to_json(df: pd.DataFrame, tgt_base_dir: str, ds_name: str, file_name: str):
    try:
        json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
        os.makedirs(f'{tgt_base_dir}/{ds_name}', exist_ok=True)
        df.to_json(json_file_path, orient='records', lines=True)
    except Exception as e:
        logging.error(f"Error converting DataFrame to JSON: {e}")
        raise


def file_converter(src_base_dir: str, tgt_base_dir: str, ds_name: str):
    try:
        schemas_path = os.path.join(src_base_dir, 'schemas.json')
        schemas = json.load(open(schemas_path))
        files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')

        if not files:
            raise FileNotFoundError(f"No files found for dataset '{ds_name}'.")

        for file in files:
            df = read_csv(file, schemas)
            file_name = os.path.basename(file)
            to_json(df, tgt_base_dir, ds_name, file_name)
    except Exception as e:
        logging.error(f"Error in file conversion for dataset '{ds_name}': {e}")
        raise


def process_files(ds_names: Optional[List[str]] = None):
    try:
        src_base_dir, tgt_base_dir = validate_environment_variables()
        schemas_path = os.path.join(src_base_dir, 'schemas.json')
        schemas = json.load(open(schemas_path))

        if not ds_names:
            ds_names = list(schemas.keys())

        for ds_name in ds_names:
            try:
                logging.info(f"Processing dataset: {ds_name}")
                file_converter(src_base_dir, tgt_base_dir, ds_name)
            except Exception as e:
                logging.warning(f"Error processing dataset '{ds_name}': {e}")
                continue
    except Exception as e:
        logging.critical(f"Critical failure: {e}")
        sys.exit(1)


if __name__ == '__main__':
    try:
        if len(sys.argv) == 2:
            ds_names = json.loads(sys.argv[1])
            process_files(ds_names)
        else:
            process_files()
    except Exception as e:
        logging.critical(f"Unhandled exception in main: {e}")
        sys.exit(1)
