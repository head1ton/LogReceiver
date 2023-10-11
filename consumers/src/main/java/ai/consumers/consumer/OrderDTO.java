package ai.consumers.consumer;

import java.time.LocalDateTime;


public class OrderDTO {

    public String orderId;
    public String shopId;
    public String menuName;
    public String userName;
    public String phoneNumber;
    public String address;
    public LocalDateTime orderTime;

    public OrderDTO(final String orderId, final String shopId, final String menuName,
        final String userName, final String phoneNumber,
        final String address, final LocalDateTime orderTime) {

        this.orderId = orderId;
        this.shopId = shopId;
        this.menuName = menuName;
        this.userName = userName;
        this.phoneNumber = phoneNumber;
        this.address = address;
        this.orderTime = orderTime;
    }
}
