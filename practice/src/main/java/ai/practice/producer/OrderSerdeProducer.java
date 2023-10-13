package ai.practice.producer;

import ai.practice.model.OrderModel;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderSerdeProducer {

    private static final Logger logger = LoggerFactory.getLogger(
        OrderSerdeProducer.class.getName());

    public static void main(String[] args) {

        String topicName = "order-serde-topic";

        // Kafka Producer Configuration Setting
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "15.164.90.78:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            OrderSerializer.class.getName());

        KafkaProducer<String, OrderModel> kafkaProducer = new KafkaProducer<>(props);

        final String filePath = "/Users/apple/Documents/02.Dev/spring_test/LogReceiver/practice/src/main/resources/pizza_sample.txt";
        sendFileMessages(kafkaProducer, topicName, filePath);

        kafkaProducer.close();
    }

    private static void sendFileMessages(
        final KafkaProducer<String, OrderModel> kafkaProducer,
        final String topicName,
        final String filePath) {

        String line = "";
        final String delimiter = ",";

        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader reader = new BufferedReader(fileReader);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                OrderModel orderModel = new OrderModel(
                    tokens[1], tokens[2], tokens[3], tokens[4], tokens[5], tokens[6],
                    LocalDateTime.parse(tokens[7].trim(), formatter)
                );

                sendMessage(kafkaProducer, topicName, key, orderModel);
            }


        } catch (IOException e) {
            logger.info(e.getMessage());
        }

    }

    private static void sendMessage(
        final KafkaProducer<String, OrderModel> kafkaProducer,
        final String topicName,
        final String key,
        final OrderModel value) {

        ProducerRecord<String, OrderModel> producerRecord = new ProducerRecord<>(
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
