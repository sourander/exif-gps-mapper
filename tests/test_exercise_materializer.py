import os
import pandas as pd

from unittest import TestCase
from exif_gps_mapper import ExerciseMaterializer

EXERCISE_DICT_A = {'upload-time': '2023-01-22T13:18:03.000Z',
                   'id': 1,
                   'polar-user': 'https://www.polaraccesslink.com/v3/users/12345678',
                   'transaction-id': 123,
                   'device': 'Polar Pacer Pro',
                   'device-id': '123FFFFF',
                   'start-time': '2023-01-22T12:00:00',
                   'start-time-utc-offset': 120,
                   'duration': 'PT37M12.200S',
                   'calories': 301,
                   'distance': 1470.0,
                   'heart-rate': {'average': 80, 'maximum': 100},
                   'sport': 'OTHER',
                   'has-route': True,
                   'detailed-sport-info': 'WALKING',
                   'fat-percentage': 50,
                   'carbohydrate-percentage': 50,
                   'protein-percentage': 0}

EXERCISE_DICT_B = {'upload-time': '2023-01-22T13:18:03.000Z',
                   'id': 2,
                   'polar-user': 'https://www.polaraccesslink.com/v3/users/12345678',
                   'transaction-id': 123,
                   'device': 'Polar Pacer Pro',
                   'device-id': '123FFFFF',
                   'start-time': '2023-01-22T14:00:00',
                   'start-time-utc-offset': 120,
                   'duration': 'PT10M46.781S',
                   'calories': 65,
                   'distance': 416.0,
                   'heart-rate': {'average': 80, 'maximum': 100},
                   'sport': 'OTHER',
                   'has-route': True,
                   'detailed-sport-info': 'WALKING',
                   'fat-percentage': 60,
                   'carbohydrate-percentage': 40,
                   'protein-percentage': 0}


class TestExerciseMaterializer(TestCase):

    def setUp(self):
        # Dir
        self.test_dir = "tests/test_data/TestExerciseMaterializer"
        os.makedirs(self.test_dir, exist_ok=True)

        self.test_file = "TestExerciseMaterializer.parquet"
        self.test_file_path = os.path.join(self.test_dir, self.test_file)

        if os.path.exists(self.test_file_path):
            os.remove(self.test_file_path)

    def test_can_handle_empty_db(self):
        exercise_materializer = ExerciseMaterializer(self.test_file_path)

        self.assertIsNone(exercise_materializer.db)
        self.assertEqual(len(exercise_materializer.generate_dataframe()), 0)
        self.assertIsInstance(exercise_materializer.generate_dataframe(), pd.DataFrame)

    def test_can_write(self):
        exercise_materializer = ExerciseMaterializer(self.test_file_path)
        exercise_materializer.add(EXERCISE_DICT_A)
        exercise_materializer.add(EXERCISE_DICT_B)
        self.assertEqual(len(exercise_materializer.generate_dataframe()), 2)

        exercise_materializer.close()
        self.assertEqual(len(pd.read_parquet(self.test_file_path)), 2)

    def test_does_not_add_duplicates(self):
        # Try to add same rows twice
        for i in range(2):
            exercise_materializer = ExerciseMaterializer(self.test_file_path)
            exercise_materializer.add(EXERCISE_DICT_A)
            exercise_materializer.add(EXERCISE_DICT_B)
            exercise_materializer.close()

        self.assertEqual(len(pd.read_parquet(self.test_file_path)), 2)
