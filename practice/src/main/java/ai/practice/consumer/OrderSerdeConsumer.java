package ai.practice.consumer;


import ai.practice.model.OrderModel;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderSerdeConsumer<K extends Serializable, V extends Serializable> {

    public static final Logger logger = LoggerFactory.getLogger(OrderSerdeConsumer.class.getName());

    private final KafkaConsumer<K, V> kafkaConsumer;
    private final List<String> topics;

    public OrderSerdeConsumer(Properties consumerProps, final List<String> topics) {
        this.kafkaConsumer = new KafkaConsumer<K, V>(consumerProps);
        this.topics = topics;
    }

    public static void main(String[] args) {
        String topicName = "order-serde-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "15.164.90.78:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            OrderDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "order-serde-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        OrderSerdeConsumer<String, OrderModel> baseConsumer = new OrderSerdeConsumer<>(props,
            List.of(topicName));

        baseConsumer.initConsumer();

        String commitMode = "async";

        baseConsumer.pollConsumers(100, commitMode);
        baseConsumer.closeConsumer();
    }

    public void initConsumer() {
        kafkaConsumer.subscribe(topics);
        shutdownHookToRuntime(kafkaConsumer);
    }

    private void shutdownHookToRuntime(final KafkaConsumer<K, V> kafkaConsumer) {
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void closeConsumer() {
        kafkaConsumer.close();
    }

    private void pollConsumers(final long durationMillis, final String commitMode) {
        try {
            while (true) {
                if (commitMode.equals("sync")) {
                    pollCommitSync(durationMillis);
                } else {
                    pollCommitAsync(durationMillis);
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            logger.info("###### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            closeConsumer();
        }
    }

    private void pollCommitAsync(final long durationMillis) {
        ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(
            Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        kafkaConsumer.commitAsync(((offsets, exception) -> {
            if (exception != null) {
                logger.error("offsets {} is not completed, error: {}", offsets,
                    exception.getMessage());
            }
        }));
    }

    private void pollCommitSync(final long durationMillis) {
        ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(
            Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        try {
            if (consumerRecords.count() > 0) {
                kafkaConsumer.commitSync();
                logger.info("commit sync has been called");
            }
        } catch (CommitFailedException e) {
            logger.error(e.getMessage());
        }
    }

    private void processRecords(final ConsumerRecords<K, V> records) {
        records.forEach(this::processRecord);
    }

    private void processRecord(final ConsumerRecord<K, V> record) {
        logger.info("record key : {}, partition : {}, record offset : {} record value : {}",
            record.key(), record.partition(), record.offset(), record.value());
    }
}
