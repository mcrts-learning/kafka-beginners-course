List all topics
```console
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Create a topic
```console
kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --create --partitions 3 --replication-factor 1
```

