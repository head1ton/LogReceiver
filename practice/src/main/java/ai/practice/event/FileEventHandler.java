package ai.practice.event;

import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileEventHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class.getName());

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topicName;
    private final boolean sync;

    public FileEventHandler(
        final KafkaProducer<String, String> kafkaProducer,
        final String topicName,
        final boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(final MessageEvent messageEvent)
        throws InterruptedException, ExecutionException {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
            topicName, messageEvent.key, messageEvent.value);

        if (sync) {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("\n ######### record metadata received ######### \n" +
                "partition: " + recordMetadata.partition() + "\n" +
                "offset: " + recordMetadata.offset() + "\n" +
                "timestamp: " + recordMetadata.timestamp());
        } else {
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

//    public static void main(String[] args) throws Exception {
//
//        String topicName = "file-topic";
//
//        Properties props = new Properties();
//        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "15.164.90.78:9092");
//        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
//        boolean sync = true;
//
//        FileEventHandler fileEventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
//        MessageEvent messageEvent = new MessageEvent("key00001", "this is test message");
//        fileEventHandler.onMessage(messageEvent);
//    }

}
