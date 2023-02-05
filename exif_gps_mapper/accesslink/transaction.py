import requests


class Transaction:
    # Class variables

    def __init__(self, access_token: str, user_id: str):
        self._user_id = user_id
        self._access_token = access_token

        # Base uri for API
        self.API_URL = f"https://www.polaraccesslink.com/v3/users/{user_id}/exercise-transactions"

        # Iterator
        self._exercise_urls: list = []
        self._i = 0

        # Run new() to create a transaction with an id
        self.transaction_id: int | None = None

    @staticmethod
    def _form_url(*args) -> str:
        return '/'.join(str(s).strip('/') for s in args)

    def populate(self) -> bool:
        # Get new transaction
        r = requests.post(self.API_URL, headers=self._get_headers("json"))
        r.raise_for_status()

        # Get transaction id
        transaction_id = self._get_transaction()

        # If we cannot get id, there are no data to fetch
        if transaction_id is None:
            return False

        # Otherwise, fetch the data
        exercises = self._get_exercise_list()

        # Set as instance variables
        self.transaction_id = transaction_id
        self._exercise_urls = exercises

        return True

    def _get_transaction(self) -> int | None:
        r = requests.post(self.API_URL, headers=self._get_headers("json"))
        r.raise_for_status()

        if r.status_code == 204:
            return None

        return int(r.json()["transaction-id"])

    def commit(self):
        # Tell Accesslink API that we are done
        uri = self._form_url(self.API_URL, self.transaction_id)
        r = requests.put(uri, headers=self._get_headers(None))
        r.raise_for_status()

    def _get_headers(self, content_type: str | None) -> dict:
        x = {
            "json": "application/json",
            "gpx": "application/gpx+xml"
        }

        # PUT operations do not need the accept parameter
        if content_type is None:
            return {"Authorization": f"Bearer {self._access_token}"}

        return {"Accept": x[content_type], "Authorization": f"Bearer {self._access_token}"}

    def _get_exercise_list(self) -> list:

        # Call
        uri = self._form_url(self.API_URL, self.transaction_id)
        r = requests.get(uri, headers=self._get_headers("json"))
        r.raise_for_status()

        return r.json().get("exercises", [])

    def _get_exercise(self, exercise_url) -> dict:
        r = requests.get(exercise_url, headers=self._get_headers("json"))
        r.raise_for_status()

        return r.json()

    def _get_gpx(self, exercise_url) -> str | None:
        uri = self._form_url(exercise_url, "gpx")
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
        return f"Transaction({self.transaction_id})"

    def __repr__(self):
        return f"Transaction({self.transaction_id})"
