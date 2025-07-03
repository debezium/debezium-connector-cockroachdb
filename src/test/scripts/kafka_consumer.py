#!/usr/bin/env python3
"""
Kafka Consumer for CockroachDB Changefeed Events

@author Virag Tripathi
@version 1.0
@description Python consumer to read and display CockroachDB changefeed events from Kafka topics
"""

import json
from kafka import KafkaConsumer
import sys

def consume_messages():
    # Create consumer
    consumer = KafkaConsumer(
        'cockroachdb.public.products',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
        key_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
    )
    
    print("🎯 Starting Kafka consumer for topic: cockroachdb.public.products")
    print("📡 Waiting for messages...")
    print("=" * 80)
    
    try:
        for message in consumer:
            print(f"📨 Message received at {message.timestamp}")
            print(f"   Partition: {message.partition}")
            print(f"   Offset: {message.offset}")
            print(f"   Key: {message.key}")
            print(f"   Value: {json.dumps(message.value, indent=2)}")
            print("-" * 80)
            
    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages() 