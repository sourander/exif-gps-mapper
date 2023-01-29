import os

from unittest import mock, TestCase
from exif_gps_mapper import ExifDatabase

base_dir_files = [
    ("BaseDir", ["FolderA", "FolderB", "FolderC"], []),
    ("BaseDir\\FolderA", [], ["FileA1.TXT", "filea2.txt"]),
    ("BaseDir\\FolderB", [], ["FileB1.TXT", "fileb2.txt"]),
    ("BaseDir\\FolderC", ["FolderCInner"], []),
    ("BaseDir\\FolderC\\FolderCInner", [], ["FileC1.TXT", "filec2.txt"])
]


class TestIterator(TestCase):

    def setUp(self):
        self.exif_db = ExifDatabase(
            db_path="not_exists.parquet",  # Does not need to exist
            lookup_path="data",            # Needs to exist
            ignore_dirs=[],
            file_extensions=[".txt"],
            case_sensitive=False
        )

    @mock.patch("exif_gps_mapper.exifdatabase.os.walk")
    def test_scan_folder(self, mock_os_walk):
        # Fake data
        self.exif_db.case_sensitive = False

        mock_os_walk.return_value = base_dir_files

        # Perform scan and convert full path to set of filenames (e.g. {"FileA1.TXT", "filea2.txt"}
        returned = self.exif_db.scan_images()
        returned_filenames = {os.path.split(x)[-1] for x in returned}

        self.assertEqual(len(returned_filenames), 6)
