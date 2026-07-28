from confluent_kafka import Producer
import time
import random
import csv

# Kafka producer configuration
producer_config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "python-producer",
}
producer = Producer(producer_config)

csv_file_path = (
    "/home/phuonn/XLDLTT/HW9,10/credit_card/User0_credit_card_transactions.csv"
)


# Callback to report delivery errors and successful sends
def delivery_report(err, msg, line_number):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] - Line {line_number} sent successfully"
        )


# Stream CSV rows to the Kafka topic
if __name__ == "__main__":
    topic_name = "test"
    try:
        with open(csv_file_path, "r") as file:
            reader = csv.reader(file)
            line_number = 0
            for row in reader:
                line_number += 1

                message = ",".join(row)
                producer.produce(
                    topic_name,
                    value=message.encode("utf-8"),
                    callback=lambda err, msg: delivery_report(err, msg, line_number),
                )

                time.sleep(random.uniform(1, 3))
                producer.poll(1)

        producer.flush()

    except KeyboardInterrupt:
        print("Producer stopped.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.flush()
