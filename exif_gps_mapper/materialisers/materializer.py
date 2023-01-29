import os
from abc import ABC, abstractmethod

import pandas as pd


class Materializer(ABC):

    SCHEMA = None
    INDEX = None

    def __init__(self, path: str):
        # Settings
        self.path = path

        # Container
        self.db = self._read()
        self.rows = []

    def _read(self) -> pd.DataFrame | None:
        if os.path.exists(self.path):
            return pd.read_parquet(self.path)
        else:
            return None

    def generate_dataframe(self) -> pd.DataFrame:
        # Apply schema to list<tuples>
        df_batch = pd.DataFrame(self.rows, columns=self.SCHEMA)
        df_batch = df_batch.astype(self.SCHEMA)
        df_batch = df_batch.set_index(self.INDEX)

        if self.db is None:
            # Full Load
            return df_batch
        else:
            # Join using the index as key
            df_joined = pd.concat(
                [
                    self.db,
                    df_batch[~df_batch.index.isin(self.db.index)]
                ]
            )
        return df_joined

    def close(self):

        if len(self.rows):
            # Generate
            df = self.generate_dataframe()

            # Write
            if len(df):
                df.to_parquet(self.path)

                # Clear staging
                self.rows = []

    def __len__(self):
        return len(self.rows)

    @abstractmethod
    def add(self, *args):
        pass
