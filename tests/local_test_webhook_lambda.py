import json
import os
from pathlib import Path

from src.lambda_webhook_receiver.app import lambda_handler


EVENT_PATH = Path("events/calendly_invitee_created_api_gateway_event.json")


def main():
    os.environ.setdefault("RAW_BUCKET_NAME", "your-local-test-bucket-name")

    with open(EVENT_PATH, "r", encoding="utf-8") as file:
        event = json.load(file)

    response = lambda_handler(event, context=None)

    print(json.dumps(response, indent=2))

    try:
        print("\nParsed response body:")
        print(json.dumps(json.loads(response["body"]), indent=2))
    except json.JSONDecodeError:
        print(response["body"])


if __name__ == "__main__":
    main()