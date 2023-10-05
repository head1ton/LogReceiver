package ai.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {

    private static final Logger logger = LoggerFactory.getLogger(
        CustomCallback.class.getName());

    private final int seq;

    public CustomCallback(int seq) {
        this.seq = seq;
    }

    @Override
    public void onCompletion(final RecordMetadata metadata, final Exception exception) {
        if (exception == null) {
            logger.info("seq: {} parition: {} offset: {}", this.seq, metadata.partition(),
                metadata.offset());
        } else {
            logger.error("exception error from broker " + exception.getMessage());
        }
    }
}
