from unittest import mock, TestCase
from exif_gps_mapper.accesslink.transaction_pool import TransactionPool, Singleton


class TestTransactionPoolIterator(TestCase):

    @mock.patch('exif_gps_mapper.accesslink.transaction_pool.Transaction')
    def test_iterator_stops_if_populate_false(self, mock_transaction):

        # Return values of API
        populate_responses = [True, True, False]

        # Mock the Transaction in a situation where we would get
        # two pages of values. Third is empty transaction.
        a = mock.MagicMock()
        a.populate.side_effect = populate_responses
        a.__iter__.return_value = []
        mock_transaction.return_value = a

        # Instantiate
        pool = TransactionPool("access_token", "test_user")

        # Loop
        for transaction in pool:
            for _ in transaction:
                # This is where you should process exercise summary and gpx
                # and write those to file.
                pass

        # Count of Trues
        expected_loop_count = sum(populate_responses)
        actual_loop_count = len(pool.previous_transactions)

        self.assertEqual(actual_loop_count, expected_loop_count)

    @mock.patch('exif_gps_mapper.accesslink.transaction_pool.Transaction')
    def test_iterator_stops_if_commit_is_stuck(self, mock_transaction):

        # Mock the Transaction in a situation where we get
        # never-ending amount of transactions.
        a = mock.MagicMock()
        a.populate.return_value = True
        a.__iter__.return_value = []
        mock_transaction.return_value = a

        # Instantiate
        pool = TransactionPool("access_token", "test_user")

        # One-indexing
        actual_loop_count = 0

        # Loop
        for transaction in pool:
            actual_loop_count += 1
            for _ in transaction:
                pass

        excepted = pool.max_round_trips
        self.assertEqual(actual_loop_count, excepted)

    def tearDown(self) -> None:
        Singleton._instances = {}