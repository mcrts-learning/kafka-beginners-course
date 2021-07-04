package com.github.mcrts.streams.ElasticConsumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumerBulk {
    String hostname = "";
    String username = "";
    String password = "";

    public ElasticSearchConsumerBulk() {}

    public void setConfig(String configPath) throws IOException {
        Properties conf = new Properties();
        FileInputStream in = new FileInputStream(configPath);
        conf.load(in);
        in.close();
        username = conf.getProperty("bonsai.username");
        password = conf.getProperty("bonsai.password");
        hostname = conf.getProperty("bonsai.hostname");
    }

    public static Properties getProperties() {
        String groupId = "bonsai-app-2";
        String bootstrapServers = "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        return properties;
    }

    public static KafkaConsumer<String, String> createConsumer() {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerBulk.class.getName());
        String topic = "twitter_tweets";

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(getProperties());
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public RestHighLevelClient createClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(username, password)
        );
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public void run() throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerBulk.class.getName());

        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createConsumer();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            logger.info("Received " + records.count() + " records");
            Integer recordCount = records.count();

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord record : records) {
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                String id = extractIdFromTweet((String) record.value());
                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweets",
                        id
                ).source((String) record.value(), XContentType.JSON);
                bulkRequest.add(indexRequest);
            }

            if (recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing the offsets...");
                consumer.commitSync();
                logger.info("Offsets committed");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //client.close();
    }

    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String tweetJSON) {
        return jsonParser.parse(tweetJSON).getAsJsonObject().get("id_str").getAsString();
    }

    public static void main(String[] args) throws IOException {
        ElasticSearchConsumerBulk consumer = new ElasticSearchConsumerBulk();
        consumer.setConfig("./app.config");
        consumer.run();
    }
}
