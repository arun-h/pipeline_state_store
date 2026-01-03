import json
import time
import random

DATA_FILE = "orders.log"


def generate_order(order_id: int):
    return {
        "order_id": order_id,
        "updated_at": int(time.time()),
        "amount": random.randint(100, 1000),
    }


def main():
    print("Starting order producer. Ctrl+C to stop.")
    order_id = 1

    while True:
        order = generate_order(order_id)

        with open(DATA_FILE, "a") as f:
            f.write(json.dumps(order) + "\n")

        print("Produced:", order)

        order_id += 1
        time.sleep(2)  # new data every 2 seconds


if __name__ == "__main__":
    main()
