# file: src/streaming/attack_flow_consumer.py

from kafka import KafkaConsumer
import json
import time

def consume_attack_flows():
    print("[AttackConsumer] Starting consumer setup...")
    consumer = KafkaConsumer(
        'attack_flow',
        bootstrap_servers=['localhost:9092'],
        group_id='attack_dashboard_group',
        auto_offset_reset='earliest',    # Read all existing attacks on first run
        enable_auto_commit=True
    )

    total_flows = 0
    unsafe_flows = 0  # Only attacks are sent here

    print("[AttackConsumer] Listening to 'attack_flow' topic...")
    try:
        for message in consumer:
            try:
                data = json.loads(message.value.decode('utf-8'))
                total_flows += 1
                unsafe_flows += 1  # All messages here are attacks
                print(f"[AttackConsumer] Attack Flow: {data.get('src_ip', 'N/A')} → {data.get('dst_ip', 'N/A')}")
                print(f"[AttackConsumer] Stats: Processed={total_flows}, Attacks={unsafe_flows}")
            except json.JSONDecodeError:
                print("[AttackConsumer] ⚠️ Invalid JSON in message")
    except KeyboardInterrupt:
        print("[AttackConsumer] Consumer interrupted by user")
    finally:
        consumer.close()
        print("[AttackConsumer] Consumer stopped.")

if __name__ == "__main__":
    consume_attack_flows()
