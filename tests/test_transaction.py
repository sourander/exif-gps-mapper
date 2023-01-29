from unittest import mock, TestCase

from exif_gps_mapper import TransactionPool
from exif_gps_mapper.transactionpool import Singleton


class TestIterator(TestCase):

    # Remember reverse-order between decorators and parameters
    @mock.patch.object(TransactionPool, "_get_exercise")
    @mock.patch.object(TransactionPool, "_get_gpx")
    def test_iterator(self, mock_get_gpx, mock_get_exercise):
        # Fake data
        exercise_urls = ["a.com", "b.com"]
        exercise_summaries = [{"id": 1, "sport": "test"}, {"id": 2, "sport": "test"}]
        gpx_datafiles = ["<?xml?>a", "<?xml?>b"]

        # Init and set exercise urls to match length of two
        transaction = TransactionPool("abc123", "def456")
        transaction._exercise_urls = exercise_urls

        # Mock to return the elements in handmade lists instead of API calls
        mock_get_exercise.side_effect = exercise_summaries
        mock_get_gpx.side_effect = gpx_datafiles

        expected = [(a, b) for a, b in zip(exercise_summaries, gpx_datafiles)]
        returned = [t for t in transaction]

        self.assertEqual(returned, expected)

    def tearDown(self):
        # Reset singleton instances so that we can create new
        Singleton._instances = {}
