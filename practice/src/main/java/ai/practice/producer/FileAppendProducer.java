package ai.practice.producer;

import ai.practice.event.EventHandler;
import ai.practice.event.FileEventHandler;
import ai.practice.event.FileEventSource;
import java.io.File;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileAppendProducer {

    private static final Logger logger = LoggerFactory.getLogger(
        FileAppendProducer.class.getName());

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

        File file = new File(
            "/Users/apple/Documents/02.Dev/spring_test/LogReceiver/practice/src/main/resources/pizza_append.txt");

        EventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName, true);

        FileEventSource fileEventSource = new FileEventSource(1000, file, 0L, eventHandler);

        Thread fileEventSourceThread = new Thread(fileEventSource);
        fileEventSourceThread.start();

        try {
            fileEventSourceThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }

}
