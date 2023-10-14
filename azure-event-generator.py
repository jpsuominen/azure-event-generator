import argparse
from pathlib import Path
from dotenv import dotenv_values
from azure.eventhub import EventHubProducerClient, EventData
import jsonlines, json


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="A tool to read events from a jsonl file and post them to an Azure Event Hub with a definable interval."
    )
    parser.add_argument("event_file", help="File to read events from.")
    parser.add_argument(
        "-v", "--verbose", help="increase output verbosity", action="store_true"
    )
    parser.add_argument(
        "-i",
        "--interval",
        help="Interval between events in milliseconds.",
        type=int,
        action="store",
    )
    parser.add_argument(
        "-c",
        "--count",
        help="Maximal number of events to send.",
        type=int,
        action="store",
        default=1000,
    )

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    config = dotenv_values(".env")
    connection_string = config["AZURE_EVENTHUB_CONNECTION_STRING"]
    event_hub = config["AZURE_EVENTHUB_NAME"]

    producer_client = EventHubProducerClient.from_connection_string(
        connection_string, eventhub_name=event_hub
    )

    events_processed = 0

    with jsonlines.open(args.event_file) as reader:
        for event in reader:
            # Now send the event instead of printing it
            # print(type(event))
            event_data = EventData(json.dumps(event))
            producer_client.send_event(event_data)

            events_processed = events_processed + 1

            if events_processed >= args.count:
                if args.verbose:
                    print(f"Max number of {args.count} events processed, exiting.")
                break

        if args.verbose:
            print(f"Processed {events_processed} events. Reached end of file, exiting.")
    # Close the Event Hub connection
    producer_client.close()


if __name__ == "__main__":
    main()
