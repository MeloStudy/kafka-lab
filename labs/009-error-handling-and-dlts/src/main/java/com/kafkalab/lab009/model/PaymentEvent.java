package com.kafkalab.lab009.model;

import java.math.BigDecimal;

public class PaymentEvent {
    private String id;
    private BigDecimal amount;
    private String status;

    // Constructors
    public PaymentEvent() {}

    public PaymentEvent(String id, BigDecimal amount, String status) {
        this.id = id;
        this.amount = amount;
        this.status = status;
    }

    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    @Override
    public String toString() {
        return "PaymentEvent{" +
                "id='" + id + '\'' +
                ", amount=" + amount +
                ", status='" + status + '\'' +
                '}';
    }
}
