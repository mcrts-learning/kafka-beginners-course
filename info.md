List all topics
```console
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Create a topic
```console
kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --create --partitions 3 --replication-factor 1
```

Console consumer
```console
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter_tweets --from-beginning
```

Consumer describe
```console
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group bonsai-app-2 --describe
```

Consumer reset offset
```console
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group bonsai-app-2 --reset-offsets --execute --to-earliest --topic twitter_tweets
```


Twitter connect
```console
kafka-topics.sh --bootstrap-server localhost:9092 --topic twitter_status_connect --create --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --topic twitter_deletes_connect --create --partitions 3 --replication-factor 1
```

Twitter streams
```console
kafka-topics.sh --bootstrap-server localhost:9092 --topic important_tweets --create --partitions 3 --replication-factor 1
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic important_tweets --from-beginning
```