package ai.practice;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileProducer {

    private static final Logger logger = LoggerFactory.getLogger(
        FileProducer.class.getName());

    public static void main(String[] args) {

        String topicName = "file-topic";

        // Kafka Producer Configuration Setting
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "15.164.90.78:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        final String filePath = "/Users/apple/Documents/02.Dev/spring_test/LogReceiver/practice/src/main/resources/pizza_sample.txt";
        sendFileMessages(kafkaProducer, topicName, filePath);

        kafkaProducer.close();
    }

    private static void sendFileMessages(
        final KafkaProducer<String, String> kafkaProducer,
        final String topicName,
        final String filePath) {

        String line = "";
        final String delimiter = ",";

        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader reader = new BufferedReader(fileReader);

            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                for (int i = 1; i < tokens.length; i++) {
                    if (i != (tokens.length - 1)) {
                        value.append(tokens[i] + delimiter);
                    } else {
                        value.append(tokens[i]);
                    }
                }

                sendMessage(kafkaProducer, topicName, key, value.toString());
            }


        } catch (IOException e) {
            logger.info(e.getMessage());
        }

    }

    private static void sendMessage(
        final KafkaProducer<String, String> kafkaProducer,
        final String topicName,
        final String key,
        final String value) {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
            topicName,
            key,
            value);

        logger.info("key : {}, value : {}", key, value);

        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info("\n ######### record metadata received ######### \n" +
                    "partition: " + metadata.partition() + "\n" +
                    "offset: " + metadata.offset() + "\n" +
                    "timestamp: " + metadata.timestamp());
            } else {
                logger.error("exception error from broker " + exception.getMessage());
            }
        });
    }
}
