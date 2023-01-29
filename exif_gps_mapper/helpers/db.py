import os

import pandas as pd
from pandas import DataFrame


def read(path: str) -> DataFrame | None:
    if os.path.exists(path):
        return pd.read_parquet(path)
    else:
        return None
