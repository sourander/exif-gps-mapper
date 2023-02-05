# Transaction Pool should be a singleton object. It needs a shared state for the current transactions transaction id,
# which is only given once per 10 minutes unless a transaction has been committed.
from exif_gps_mapper.accesslink.transaction import Transaction


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class TransactionPool(metaclass=Singleton):
    # Class Variables
    # Maximum number of transactions created in this pool. Each can contain up to 50 exercises.
    # This protects our tool from infinite loop if Accesslink API would not register our commit.
    max_round_trips = 10

    def __init__(self, access_token: str, user_id: str):
        # API elements
        self._access_token = access_token
        self._user_id = user_id

        self.current_transaction = None
        self.previous_transactions = []

    def get_transaction(self) -> Transaction | None:
        # Otherwise, create new
        transaction = Transaction(self._access_token, self._user_id)
        self.current_transaction = transaction
        return transaction

    def commit_current(self):
        if self.current_transaction is not None:
            # Commit
            self.current_transaction.commit()

            # Mark it previous
            self.previous_transactions.append(self.current_transaction)
            self.current_transaction = None

    def __iter__(self):
        return self

    def __next__(self) -> Transaction:

        # Commit current Transaction if can
        self.commit_current()

        # Avoid infinite loop
        if len(self.previous_transactions) >= self.max_round_trips:
            raise StopIteration

        # Get new Transaction
        transaction = self.get_transaction()

        # Try to fetch a list of exercises
        success = transaction.populate()

        if success:
            return transaction

        raise StopIteration

    def __str__(self):
        return f"TransactionPool({self.current_transaction})"

    def __repr__(self):
        return f"TransactionPool({self.current_transaction})"
