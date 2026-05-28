package com.kafkalab.p01.model;

public class OrderRequest {
    private String orderId;
    private String userId;
    private double amount;

    public OrderRequest() {}

    public OrderRequest(String orderId, String userId, double amount) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
}
