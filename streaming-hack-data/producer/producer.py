"""Send every attack of hack_data.csv to Kafka, one JSON message per row.

Usage: python producer/producer.py [--csv data/hack_data.csv] [--topic hack-attacks]
                                   [--bootstrap localhost:29092] [--delay 0.05] [--loops 1]
"""
import argparse
import csv
import json
import os
import time

from kafka import KafkaProducer

NUMERIC = {
    "Session_Connection_Time": float,
    "Bytes Transferred": float,
    "Kali_Trace_Used": int,
    "Servers_Corrupted": float,
    "Pages_Corrupted": float,
    "WPM_Typing_Speed": float,
}


def to_record(row, attack_id):
    rec = {"attack_id": attack_id, "Location": row["Location"]}
    for col, cast in NUMERIC.items():
        rec[col] = cast(float(row[col])) if cast is int else cast(row[col])
    return rec


def main():
    p = argparse.ArgumentParser()
    default_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "hack_data.csv")
    p.add_argument("--csv", default=os.path.normpath(default_csv))
    p.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "hack-attacks"))
    p.add_argument("--bootstrap", default=os.environ.get(
        "KAFKA_BOOTSTRAP", f"localhost:{os.environ.get('KAFKA_EXTERNAL_PORT', '29092')}"))
    p.add_argument("--delay", type=float, default=0.05, help="seconds between two messages")
    p.add_argument("--loops", type=int, default=1, help="how many times to replay the file")
    args = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sent = 0
    for _ in range(args.loops):
        # utf-8-sig strips the BOM Excel adds; the delimiter is "," or ";" (French Excel)
        with open(args.csv, newline="", encoding="utf-8-sig") as f:
            delimiter = ";" if ";" in f.readline() else ","
            f.seek(0)
            for row in csv.DictReader(f, delimiter=delimiter):
                rec = to_record(row, sent)
                producer.send(args.topic, key=rec["Location"], value=rec)
                sent += 1
                if sent % 50 == 0:
                    print(f"{sent} attacks sent")
                time.sleep(args.delay)

    producer.flush()
    producer.close()
    print(f"Done: {sent} attacks sent to topic '{args.topic}'")


if __name__ == "__main__":
    main()
