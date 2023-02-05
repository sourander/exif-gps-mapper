import os

import exiftool
import pandas as pd


class ExifDatabase:
    # Should we get these from Config?
    chosen_exif_fields = [
        "EXIF:CreateDate",
        "EXIF:GPSLatitude",
        "EXIF:GPSLongitude",
        "EXIF:LensModel",
        # "EXIF:JpgFromRaw" # requires -b arg
    ]

    schema = ["created", "lat", "long", "lens"]

    def __init__(self, db_path: str, lookup_path: str, ignore_dirs: list, file_extensions: list,
                 case_sensitive_extensions=False):
        # Settings

        self.db_path = db_path
        self.lookup_path = lookup_path
        self.ignore_dirs = ignore_dirs
        self.file_extensions = file_extensions
        self.case_sensitive_extensions = case_sensitive_extensions

        # In-Memory DataBase
        self._db: pd.DataFrame | None = None

        # Validate that schema is doable
        assert len(self.chosen_exif_fields) == len(self.schema)

    @property
    def as_df(self):
        return self._db

    def upsert(self):
        # Read DB
        self._db = self.read()

        if self._db is None:
            self.full_load()
        else:
            self.incremental_load()

    def read(self):
        if os.path.exists(self.db_path):
            return pd.read_parquet(self.db_path)
        else:
            return None

    def apply_deletes(self):

        # Read DB
        self._db = self.read()

        assert self._db is not None, "You have no database. Deletes do not make sense."

        # Set of deleted images (exist only in DB but not in look-up directory)
        deleted_images = self._get_deleted_images()

        # Apply filter
        df_filtered = self._db[~self._db["filepath"].isin(deleted_images)]

        # Write
        df_filtered.to_parquet(self.db_path)

        # Reload the In-Memory DB to be the newly written DB
        self._db = self.read()

    def full_load(self):

        all_files = self.scan_images()

        assert len(all_files), f"You are trying to perform a full load from path ({self.lookup_path}) that does not " \
                               f"contain any images into db {self.db_path}. No need to proceed!"

        # Load Exif for all files in lookup path
        df_full_load = self.get_exif_dataframe(all_files)

        # Materialize
        df_full_load.to_parquet(self.db_path)

        # Reload the In-Memory DB to be the newly written DB
        self._db = self.read()

    def _get_new_images(self) -> set:
        return self.scan_images() - set(self._db.filepath)

    def _get_deleted_images(self) -> set:
        return set(self._db.filepath) - self.scan_images()

    def incremental_load(self):

        # New files only
        new_images = self._get_new_images()

        # New batch of data. Can be None.
        df_batch = self.get_exif_dataframe(new_images)

        df_union = pd.concat([self._db, df_batch], axis="rows", ignore_index=True)

        # If new rows were added
        if len(df_union) > len(self._db):
            df_union.to_parquet(self.db_path)

        # Reload the In-Memory DB to be the newly written DB
        self._db = self.read()

    def get_exif_dataframe(self, image_paths: set) -> pd.DataFrame | None:
        if not len(image_paths):
            return None

        # Map names to custom names
        col_name_map = dict(zip(self.chosen_exif_fields, self.schema))
        col_name_map["SourceFile"] = "filepath"  # ExifToolHelper adds SourceFile

        # If JpgFromRaw, pass this ExifToolHelper(custom_args=exif_tool_args)
        # exif_tool_args = ['-G', '-n', '-b']

        # Container
        collected = []

        with exiftool.ExifToolHelper() as et:
            for d in et.get_tags(image_paths, tags=self.chosen_exif_fields):
                collected.append(d)

        # Convert to DataFrame and rename columns using the map
        df = pd.DataFrame(collected).rename(columns=col_name_map)

        # Normalize paths to make sure that forward/backward slashes are correct for OS
        df["filepath"] = df["filepath"].apply(os.path.normpath)
        return df

    def scan_images(self) -> set:

        if not os.path.exists(self.lookup_path):
            raise OSError(f"Path ({self.lookup_path}) does not exist.")

        # Container
        found_images = []

        for root, dirs, file_names in os.walk(self.lookup_path, topdown=True):

            dirs[:] = set(dirs) - set(self.ignore_dirs)

            for file_name in file_names:

                if not self.case_sensitive_extensions:
                    e = [x.lower() for x in self.file_extensions]
                    fn = file_name.lower()
                else:
                    e = self.file_extensions
                    fn = file_name

                if fn.endswith(tuple(e)):
                    # Combine full path
                    full_path = os.path.join(os.path.abspath(root), file_name)
                    found_images.append(full_path)

        # Normalize paths
        found_images = [os.path.normpath(x) for x in found_images]

        return set(found_images)

    def __str__(self):
        return f"ExifDatabase({self.db_path})"

    def __repr__(self):
        return f"ExifDatabase({self.db_path})"
