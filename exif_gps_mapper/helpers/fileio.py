import os

import exiftool
import pandas as pd
from pandas import DataFrame


def scan_images(path: str, ignore_dirs: list, file_extensions: list, case_sensitive: bool = False) -> set:
    if not case_sensitive:
        ignore_dirs = set(x.lower() for x in ignore_dirs)
        file_extensions = [x.lower() for x in file_extensions]

    if not os.path.exists(path):
        raise OSError(f"Path ({path}) does not exist.")

    # Container
    found_images = []

    for root, dirs, file_names in os.walk(path, topdown=True):

        # Ignore directories in-place using the topdown method.
        if not case_sensitive:
            dirs[:] = [d for d in dirs if d.lower() not in ignore_dirs]
        else:
            print("Pre:", dirs)
            dirs[:] = set(dirs) - set(ignore_dirs)
            print("Post:", dirs)

        for file_name in file_names:

            if not case_sensitive:
                file_name_case = file_name.lower()
            else:
                file_name_case = file_name

            if file_name_case.endswith(tuple(file_extensions)):
                # Combine full path
                full_path = os.path.join(os.path.abspath(root), file_name)
                found_images.append(full_path)

    # Normalize paths
    found_images = [os.path.normpath(x) for x in found_images]

    return set(found_images)


def get_new_exif_data(image_paths: list) -> DataFrame | None:

    if not len(image_paths):
        return None

    # Schema
    wanted = [
        "EXIF:CreateDate",
        "EXIF:GPSLatitude",
        "EXIF:GPSLongitude",
        "EXIF:LensModel",
        # "EXIF:JpgFromRaw" # requires -b arg
    ]

    # Map names to custom names
    column_names = ["created", "lat", "long", "lens"]
    col_name_map = dict(zip(wanted, column_names))
    col_name_map["SourceFile"] = "filepath"  # ExifToolHelper adds SourceFile

    # If JpgFromRaw, pass this ExifToolHelper(custom_args=exiftoolargs)
    # exiftoolargs=['-G', '-n', '-b']

    # Container
    collected = []

    with exiftool.ExifToolHelper() as et:
        for d in et.get_tags(image_paths, tags=wanted):
            collected.append(d)

    # Convert to DataFrame and rename columns using the map
    df = pd.DataFrame(collected).rename(columns=col_name_map)

    # Normalize paths to make sure that forward/backward slashes are correct for OS
    df["filepath"] = df["filepath"].apply(os.path.normpath)
    return df
