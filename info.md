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