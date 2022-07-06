# sfaas-dataflow


To run benchmark:

1. Start Kafka: `docker compose -f docker-compose-kafka.yml up`
2. Start Universalis: `docker compose up --build --scale worker=3`
3. Run Kafka Consumer: `demo/kafka_output_consumer.py`
4. Run your benchmark file