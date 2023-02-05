import os
import unittest
import pandas as pd
import shutil

from unittest import mock, TestCase
from exif_gps_mapper import ExifDatabase

base_dir_files = [
    (r"BaseDir", ["FolderA", "FolderB", "FolderC"], []),
    (r"BaseDir\FolderA", [], ["FileA1.TXT", "filea2.txt"]),
    (r"BaseDir\FolderB", [], ["FileB1.TXT", "fileb2.txt"]),
    (r"BaseDir\FolderC", ["FolderCInner"], []),
    (r"BaseDir\FolderC\FolderCInner", [], ["FileC1.TXT", "filec2.txt"])
]


class TestExifDatabaseScanFolder(TestCase):

    def setUp(self):
        self.exif_db = ExifDatabase(
            db_path="not_exists.parquet",  # Does not need to exist
            lookup_path="fake_dir",  # Does not need to exist
            ignore_dirs=[],
            file_extensions=[".txt"],
            case_sensitive_extensions=False
        )

    @mock.patch("exif_gps_mapper.exifdatabase.os.path.exists")
    @mock.patch("exif_gps_mapper.exifdatabase.os.walk")
    def test_scan_folder(self, mock_os_walk, mock_os_exists):
        self.exif_db.case_sensitive_extensions = False

        mock_os_exists.return_value = True
        mock_os_walk.return_value = base_dir_files

        # Perform scan and convert full path to set of filenames (e.g. {"FileA1.TXT", "filea2.txt"}
        returned = self.exif_db.scan_images()
        returned_filenames = {os.path.split(x)[-1] for x in returned}

        self.assertEqual(len(returned_filenames), 6)

    @mock.patch("exif_gps_mapper.exifdatabase.os.path.exists")
    @mock.patch("exif_gps_mapper.exifdatabase.os.walk")
    def test_scan_folder_case_sensitive(self, mock_os_walk, mock_os_exists):
        # Set on
        self.exif_db.case_sensitive_extensions = True

        mock_os_exists.return_value = True
        mock_os_walk.return_value = base_dir_files

        # Perform scan and convert full path to set of filenames (e.g. {"FileA1.TXT", "filea2.txt"}
        returned = self.exif_db.scan_images()
        returned_filenames = {os.path.split(x)[-1] for x in returned}

        # With case-sensitive, we should only get those files that end .txt and not .TXT
        self.assertEqual(len(returned_filenames), 3)


class TestExifDatabase(unittest.TestCase):

    def create_fake_images(self, n: int, prefix: str):

        for i in range(n):
            # Generate the target path
            target = os.path.join(self.under_testing_dir, f"{prefix}_{i:02d}.jpg")

            # Copy the image
            shutil.copyfile(self.original, target)

    def delete_fake_images(self, n:int):
        # Find jpeg files in the directory
        jpegs = [x for x in os.listdir(self.under_testing_dir) if x.endswith(".jpg")]

        # Sort the list of jpegs
        jpegs = sorted(jpegs)

        # Check if there are any images in the directory
        assert jpegs, "There are no images in the folder. Check the code."

        # Check if we are trying to delete more images than exist
        assert n <= len(jpegs), "You cannot delete more images than exist in the directory"

        # Get the last n images
        jpegs_to_del = jpegs[-n:]

        # Delete the images
        for jpeg in jpegs_to_del:
            os.remove(os.path.join(self.under_testing_dir, jpeg))

    def setUp(self):
        # Define the path to the database file
        self.db_path = os.path.join(
            "tests", "test_data", "TestExifDatabase", "test_db.parquet"
        )

        # Lookup path. This will ba scanned for images
        self.lookup_path = os.path.join("tests", "test_images")

        # Other parameters
        self.ignore_dirs = ['stash']
        self.file_extensions = ['.jpg', '.jpeg']
        self.case_sensitive_extensions = False

        # Instantiate the ExifDatabase object
        self.exif_database = ExifDatabase(
            self.db_path,
            self.lookup_path,
            self.ignore_dirs,
            self.file_extensions,
            self.case_sensitive_extensions
        )

        # Small 50x50px image we will copy and modify while testing
        self.stash_dir = os.path.join(self.lookup_path, "stash")
        self.original = os.path.join(self.stash_dir, "test_image.jpg")

        # The directory that will be written, read and deleted during testing
        self.under_testing_dir = os.path.join(self.lookup_path, "under_testing")

        # Make directory for the parquet database and the under_testing directory
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        os.makedirs(self.under_testing_dir, exist_ok=True)

        # Ensure that the database is empty before starting the next test
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

        # Create images that will always be part of full load
        self.create_fake_images(3, "existing")

    def test_full_load(self):
        # Perform a full load
        self.exif_database.full_load()
        db = pd.read_parquet(self.db_path)

        # Check if the full load was successful and the number of rows in the database is equal to the number of
        # images in the lookup path
        self.assertTrue(os.path.exists(self.db_path))
        self.assertEqual(len(db), 3)

    def test_incremental_load(self):
        # Perform a full load first
        self.exif_database.full_load()
        initial_count = len(self.exif_database.as_df)

        # Add a new image to the lookup path
        self.create_fake_images(1, "new_image")

        # Assume the new image has the path 'new_image_00.jpg'
        self.exif_database.incremental_load()
        final_count = len(self.exif_database.as_df)

        # Check if the incremental load was successful and that the number of rows in the database has increased by 1
        self.assertEqual(final_count, initial_count + 1)

    def test_apply_deletes(self):
        # Perform a full load first
        self.exif_database.full_load()
        initial_count = len(self.exif_database.as_df)

        # Delete an image from the lookup path
        self.delete_fake_images(1)

        # Assume the deleted image has the path 'deleted_image.jpg'
        self.exif_database.apply_deletes()
        final_count = len(self.exif_database.as_df)

        # Check if the apply_deletes method was successful and that the number of rows
        # in the database has decreased by 1
        self.assertEqual(final_count, initial_count - 1)

    def tearDown(self) -> None:

        # Remove test images from under_testing directory
        test_dir = os.path.join(self.lookup_path, "under_testing")

        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
