import os
import pandas as pd


class ExifDatabase:
    def __init__(self, db_path: str, lookup_path: str, ignore_dirs: list, file_extensions: list,
                 case_sensitive: bool = False):
        # Settings
        self.db_path = db_path
        self.lookup_path = lookup_path
        self.ignore_dirs = ignore_dirs
        self.file_extensions = file_extensions
        self.case_sensitive = case_sensitive

        self.db = self._read(self.db_path)

    def _read(self, path: str) -> pd.DataFrame | None:
        if os.path.exists(path):
            return pd.read_parquet(path)
        else:
            return None

    def scan_images(self) -> set:
        if not self.case_sensitive:
            ignore_dirs = set(x.lower() for x in self.ignore_dirs)
            file_extensions = [x.lower() for x in self.file_extensions]

        if not os.path.exists(self.lookup_path):
            raise OSError(f"Path ({self.lookup_path}) does not exist.")

        # Container
        found_images = []

        for root, dirs, file_names in os.walk(self.lookup_path, topdown=True):

            # Ignore directories in-place using the topdown method.
            if not self.case_sensitive:
                dirs[:] = [d for d in dirs if d.lower() not in self.ignore_dirs]
            else:
                dirs[:] = set(dirs) - set(self.ignore_dirs)

            for file_name in file_names:

                if not self.case_sensitive:
                    file_name_case = file_name.lower()
                else:
                    file_name_case = file_name

                if file_name_case.endswith(tuple(self.file_extensions)):
                    # Combine full path
                    full_path = os.path.join(os.path.abspath(root), file_name)
                    found_images.append(full_path)

        # Normalize paths
        found_images = [os.path.normpath(x) for x in found_images]

        return set(found_images)
