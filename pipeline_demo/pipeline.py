import json
import time
from state_client import StateClient

DATA_FILE = "orders.log"
PIPELINE_NAME = "orders_pipeline"


def read_new_records(last_checkpoint: int | None):
    """
    Reads records with updated_at > last_checkpoint
    """
    records = []

    try:
        with open(DATA_FILE, "r") as f:
            for line in f:
                record = json.loads(line)
                ts = record["updated_at"]

                if last_checkpoint is None or ts > last_checkpoint:
                    records.append(record)
    except FileNotFoundError:
        pass

    return records


def process_records(records):
    """
    Simulate processing.
    """
    for r in records:
        print("Processing order:", r)
        time.sleep(0.2)  # simulate work


def main():
    client = StateClient()

    # 1. Read last checkpoint
    last_cp = client.get_checkpoint(PIPELINE_NAME)
    last_cp = int(last_cp) if last_cp is not None else None

    print("Last checkpoint:", last_cp)

    # 2. Read incremental data
    records = read_new_records(last_cp)

    if not records:
        print("No new records to process.")
        return

    # 3. Process
    process_records(records)

    # 4. Commit checkpoint (max updated_at)
    new_checkpoint = max(r["updated_at"] for r in records)
    client.set_checkpoint(PIPELINE_NAME, str(new_checkpoint))

    print("Checkpoint updated to:", new_checkpoint)


if __name__ == "__main__":
    main()
