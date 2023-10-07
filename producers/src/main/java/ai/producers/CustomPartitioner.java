package ai.producers;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomPartitioner implements Partitioner {

    public static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class.getName());
    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();
    private String specialKeyName;

    @Override
    public int partition(
        final String topic,
        final Object key,
        final byte[] keyBytes,
        final Object value,
        final byte[] valueBytes,
        final Cluster cluster) {

        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfoList.size();
        int numSpecialPartitions = (int) (numPartitions * 0.5);
        int partitionIndex = 0;

        if (keyBytes == null) {
//            return stickyPartitionCache.partition(topic, cluster);
            throw new InvalidRecordException("key should not be null");
        }

        if (key.equals(specialKeyName)) {
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        } else {
            partitionIndex =
                Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions)
                    + numSpecialPartitions;
        }

        logger.info("key : {} is sent to partition : {}", key, partitionIndex);

        return partitionIndex;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        specialKeyName = configs.get("custom.specialKey").toString();
    }

    @Override
    public void close() {

    }
}
