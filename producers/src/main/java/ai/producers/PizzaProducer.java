package ai.producers;

import com.github.javafaker.Faker;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PizzaProducer {

    private static final Logger logger = LoggerFactory.getLogger(
        PizzaProducer.class.getName());

    public static void sendPizzaMessage(
        KafkaProducer<String, String> kafkaProducer,
        String topicName,
        int iterCount,      // 몇 번 돌릴건지 (-1은 무한대)
        int interIntervalMillis,    // 1건 처리 후 쉬는 시간
        int intervalMillis,             // 1000(intervalCount) 건  돌리고 쉬는 시간
        int intervalCount,      // intervalMills 건수
        boolean sync)   // 동기: true 비동기: false
    {
        PizzaMessage pizzaMessage = new PizzaMessage();

        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        long startTime = System.currentTimeMillis();

        while (iterSeq++ != iterCount) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,
                pMessage.get("key"), pMessage.get("message"));

            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            // 쉬는 로직 (중간중간)
            if ((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("####### IntervalCount : " + intervalCount + " intervalMillis : "
                        + intervalMillis + " #######");
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
            // 쉬는 로직 (한건한건)
            if (interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis : " + interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }

        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        logger.info("{} millisecond elapsed for {} iterations", timeElapsed, iterCount);
    }

    public static void sendMessage(
        KafkaProducer<String, String> kafkaProducer,
        ProducerRecord<String, String> producerRecord,
        HashMap<String, String> pMessage,
        boolean sync) {
        if (!sync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("async message : " + pMessage.get("key") +
                        " partition: " + metadata.partition() +
                        " offset: " + metadata.offset());
                } else {
                    logger.error("exception error from broker " + exception.getMessage());
                }
            });
        } else {
            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                logger.info(
                    "sync message : " + pMessage.get("key") +
                        " partition: " + metadata.partition() +
                        " offset: " + metadata.offset());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {

        // Kafka Producer Configuration Setting
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "15.164.90.78:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
//        props.setProperty(ProducerConfig.ACKS_CONFIG,  "all"); // 0이면 안 기다림(async) ,  0 일 때 동기로 하면 안됨(offset 안나옴).

//        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32000");
//        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // millisecond

//        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "50000");

//        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6");
//        props.setProperty(ProducerConfig.ACKS_CONFIG, "0");
//        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");    // acks_config : all or -1 로 해야함.

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        String topicName = "pizza-topic";

        sendPizzaMessage(kafkaProducer, topicName, -1, 1000, 0, 0, true);

        kafkaProducer.close();
    }
}
