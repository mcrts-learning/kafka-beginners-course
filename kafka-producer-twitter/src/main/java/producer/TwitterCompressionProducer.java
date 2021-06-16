package producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterCompressionProducer {
    String consumerKey = "";
    String consumerSecret = "";
    String token = "";
    String secret = "";

    public TwitterCompressionProducer() {}

    public void setConfig(String configPath) throws IOException {
        Properties conf = new Properties();
        FileInputStream in = new FileInputStream(configPath);
        conf.load(in);
        in.close();
        consumerKey = conf.getProperty("twitter.consumer.key");
        consumerSecret = conf.getProperty("twitter.consumer.secret");
        token = conf.getProperty("twitter.auth.token");
        secret = conf.getProperty("twitter.auth.secret");
    }

    public static Properties getProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        return properties;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin", "astrophysics", "formula 1");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(TwitterCompressionProducer.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "twitter_tweets";
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // create a kafka producer
        Properties properties = getProperties(bootstrapServers);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down application...");
            logger.info("Closing Twitter client...");
            client.stop();
            logger.info("Closing Kafka producer...");
            producer.close();
            logger.info("Done!");
        }));
        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>(topic, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }

    public static void main(String[] args) throws IOException {
        TwitterCompressionProducer producer = new TwitterCompressionProducer();
        producer.setConfig("./app.config");
        producer.run();
    }
}
