"""Generic Kafka callback functions."""


def delivery_report(err, msg):
    """Delivery report."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
