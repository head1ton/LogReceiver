package ai.practice.event;

public class MessageEvent {

    public String key;
    public String value;

    public MessageEvent(final String key, final String value) {
        this.key = key;
        this.value = value;
    }
}
