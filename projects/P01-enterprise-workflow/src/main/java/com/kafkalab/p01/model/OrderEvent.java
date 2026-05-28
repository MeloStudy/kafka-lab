package com.kafkalab.p01.model;

public class OrderEvent {
    private String eventId;
    private OrderRequest payload;
    private String status; // CREATED, CONFIRMED, FAILED

    public OrderEvent() {}

    public OrderEvent(String eventId, OrderRequest payload, String status) {
        this.eventId = eventId;
        this.payload = payload;
        this.status = status;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public OrderRequest getPayload() { return payload; }
    public void setPayload(OrderRequest payload) { this.payload = payload; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}
