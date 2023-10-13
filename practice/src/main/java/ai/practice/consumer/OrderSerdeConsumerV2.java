package ai.practice.consumer;


import ai.practice.model.OrderModel;
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

public class OrderSerdeConsumerV2 {

    public static final Logger logger = LoggerFactory.getLogger(
        OrderSerdeConsumerV2.class.getName());

    private final KafkaConsumer<String, OrderModel> kafkaConsumer;
    private final List<java.lang.String> topics;

    public OrderSerdeConsumerV2(Properties consumerProps, final List<java.lang.String> topics) {
        this.kafkaConsumer = new KafkaConsumer<String, OrderModel>(consumerProps);
        this.topics = topics;
    }

    public static void main(java.lang.String[] args) {
        java.lang.String topicName = "order-serde-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "15.164.90.78:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            OrderDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "order-serde-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        OrderSerdeConsumerV2 orderSerdeConsumerV2 = new OrderSerdeConsumerV2(props,
            List.of(topicName));

        orderSerdeConsumerV2.initConsumer();

        java.lang.String commitMode = "async";

        orderSerdeConsumerV2.pollConsumers(100, commitMode);
        orderSerdeConsumerV2.closeConsumer();
    }

    public void initConsumer() {
        kafkaConsumer.subscribe(topics);
        shutdownHookToRuntime(kafkaConsumer);
    }

    private void shutdownHookToRuntime(final KafkaConsumer<String, OrderModel> kafkaConsumer) {
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

    private void pollConsumers(final long durationMillis, final java.lang.String commitMode) {
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
        ConsumerRecords<String, OrderModel> consumerRecords = kafkaConsumer.poll(
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
        ConsumerRecords<String, OrderModel> consumerRecords = kafkaConsumer.poll(
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

    private void processRecords(final ConsumerRecords<String, OrderModel> records) {
        records.forEach(this::processRecord);
    }

    private void processRecord(final ConsumerRecord<String, OrderModel> record) {
        logger.info("record key : {}, partition : {}, record offset : {} record value : {}",
            record.key(), record.partition(), record.offset(), record.value());
    }
}
