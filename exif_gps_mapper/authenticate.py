import requests
import uuid
from flask import Flask, request, redirect
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta

# from utils import load_config, save_config
from exif_gps_mapper.helpers import config
from urllib.parse import urlencode

# Flask
CALLBACK_PORT = 5000
CALLBACK_ENDPOINT = "/oauth2_callback"
REDIRECT_URL = "http://localhost:{}{}".format(CALLBACK_PORT, CALLBACK_ENDPOINT)
app = Flask(__name__)

# Polar endpoints
AUTHORIZATION_URL = "https://flow.polar.com/oauth2/authorization"
ACCESS_TOKEN_URL = "https://polarremote.com/v2/oauth2/token"
ACCESSLINK_URL = "https://www.polaraccesslink.com/v3"

# Config
c = config.read()
secrets = {}


@app.route("/")
def authorize():
    params = {
        "client_id": c["accesslink"]['client_id'],
        "response_type": "code"
    }
    return redirect(f"{AUTHORIZATION_URL}?{urlencode(params)}")


@app.route(CALLBACK_ENDPOINT)
def callback():
    """Callback for OAuth2 authorization request

    Saves the user's id, expiration date and access token to a file.
    """

    # Get authorization from the callback request parameters
    authorization_code = request.args.get("code")

    # Get an access token for the user using the authorization code.
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json;charset=UTF-8"
    }

    data = {
        "grant_type": "authorization_code",
        "code": authorization_code
    }

    response = requests.post(
        ACCESS_TOKEN_URL,
        headers=headers,
        data=data,
        auth=HTTPBasicAuth(
            c["accesslink"]['client_id'],
            c["accesslink"]['client_secret']
        )
    )

    # Raise
    response.raise_for_status()

    # Get response as dictionary
    r = response.json()

    # Calculate expiration date
    expiration = (datetime.now() + timedelta(seconds=r["expires_in"])).date()

    # Save the user's id and access token to the configuration file.
    secrets["user_id"] = r["x_user_id"]
    secrets["access_token"] = r["access_token"]
    secrets["expires"] = expiration
    config.write(secrets, c["accesslink"]["secrets_path"])

    # Register the user as a user of the application.
    try:
        # url = "https://www.polaraccesslink.com/v3" + "/users"
        url = ACCESSLINK_URL + "/users"

        # Define headers
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {secrets['access_token']}"
        }

        # Define data
        data = {
            "member-id": uuid.uuid4().hex
        }
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:

        if err.response.status_code == 409:
            message = "Client has already been registered! Token has been refreshed. " \
                      "You can now close the page."
        else:
            raise
    else:
        message = "User registered to ExifGPSMapper! You can now close this page."
    finally:
        print("[INFO] Shutting down Flask server")
        shutdown()

    return message


def shutdown():
    shutdown_func = request.environ.get('werkzeug.server.shutdown')
    if shutdown_func is not None:
        shutdown_func()


def main():
    print("Navigate to http://localhost:{port}/ for authorization.\n".format(port=CALLBACK_PORT))
    app.run(host='localhost', port=CALLBACK_PORT)


if __name__ == "__main__":
    main()
