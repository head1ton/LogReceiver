package ai.practice.consumer;

import ai.practice.model.OrderModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderDeserializer implements Deserializer<OrderModel> {

    public static final Logger logger = LoggerFactory.getLogger(OrderDeserializer.class.getName());

    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public OrderModel deserialize(final String topic, final byte[] data) {
        OrderModel orderModel = null;

        try {
            orderModel = objectMapper.readValue(data, OrderModel.class);
        } catch (IOException e) {
            logger.error("Object mapper deserialization failed " + e.getMessage());
        }

        return orderModel;
    }
}
