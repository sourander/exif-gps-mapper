import requests

from exif_gps_mapper.exceptions import MaxRetryCountExceeded


# Transaction Pool should be a singleton object. It needs a shared state for the current transactions transaction id,
# which is only given once per 10 minutes unless a transaction has been committed.
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
        self.API_URL = f"https://www.polaraccesslink.com/v3/users/{user_id}/exercise-transactions"

        # Iterator
        self._exercise_urls: list = []
        self._i = 0

        # Run new() to create a transaction with an id
        self.transaction_id: int | None = None
        self.transaction_retry_count = 0

    def reset_state(self):
        self.transaction_id = None
        self._exercise_urls = []
        self._i = 0

    def new(self):

        # Pretend there is no data if max round trips has been met
        if self.transaction_retry_count >= self.max_round_trips:
            raise MaxRetryCountExceeded

        # Start a new transaction if it has not already been started
        # or if the previous was committed and reset
        if self.transaction_id is None:
            self.transaction_id = self._start_transaction()
            self.transaction_retry_count += 1

        # Fetch new exercises
        self._exercise_urls = self._get_exercise_list()

    def commit(self):
        uri = self.form_url(self.API_URL, self.transaction_id)
        r = requests.put(uri, headers=self._get_headers(None))
        r.raise_for_status()
        if r.status_code == 200:
            self.reset_state()

    @staticmethod
    def form_url(*args) -> str:
        return '/'.join(str(s).strip('/') for s in args)

    def _get_headers(self, content_type: str | None) -> dict:
        x = {
            "json": "application/json",
            "gpx": "application/gpx+xml"
        }

        # PUT operations do not need the accept parameter
        if content_type is None:
            return {"Authorization": f"Bearer {self._access_token}"}

        return {"Accept": x[content_type], "Authorization": f"Bearer {self._access_token}"}

    def _start_transaction(self) -> int | None:
        # Call
        r = requests.post(self.API_URL, headers=self._get_headers("json"))
        r.raise_for_status()

        if r.status_code == 204:
            # No new training session data available
            return None

        return r.json()["transaction-id"]

    def _get_exercise_list(self) -> list:

        # We cannot fetch data if we don't have a transaction open
        if self.transaction_id is None:
            return []

        # Call
        uri = self.form_url(self.API_URL, self.transaction_id)
        r = requests.get(uri, headers=self._get_headers("json"))
        r.raise_for_status()

        return r.json().get("exercises", [])

    def _get_exercise(self, exercise_url) -> dict:
        r = requests.get(exercise_url, headers=self._get_headers("json"))
        r.raise_for_status()

        return r.json()

    def _get_gpx(self, exercise_url) -> str | None:
        uri = self.form_url(exercise_url, "gpx")
        r = requests.get(uri, headers=self._get_headers("gpx"))
        r.raise_for_status()

        # Some sports do not contain GPX data
        if r.status_code == 204:
            return None

        return r.text

    def __iter__(self):
        return self

    def __next__(self) -> tuple:
        if self._i < len(self._exercise_urls):
            # Fetch the nth url
            exercise_url = self._exercise_urls[self._i]

            # Call exercise and exercise/gpx APIs
            exercise = self._get_exercise(exercise_url)
            gpx = self._get_gpx(exercise_url)

            # Increment
            self._i += 1
            return exercise, gpx

        raise StopIteration

    def __len__(self):
        return len(self._exercise_urls)

    def __str__(self):
        i = self._i
        n = self.__len__()
        return f"Transaction({self.transaction_id}) with {i}/{n} exercises fetched."

    def __repr__(self):
        return f"Transaction(transaction_id={self.transaction_id})"
