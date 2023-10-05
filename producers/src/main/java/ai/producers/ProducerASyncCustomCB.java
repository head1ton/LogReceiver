package ai.producers;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerASyncCustomCB {

    private static final Logger logger = LoggerFactory.getLogger(
        ProducerASyncCustomCB.class.getName());

    public static void main(String[] args) {

        // Kafka Producer Configuration Setting
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "15.164.90.78:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        String topicName = "multipart-topic";

        for (int seq = 0; seq < 20; seq++) {
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(
                topicName,
                seq,
                "hello word " + seq);

            Callback callback = new CustomCallback(seq);

//            logger.info("seq : " + seq);

            kafkaProducer.send(producerRecord, callback);
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
