package com.kafkalab.springboot;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Payment {
    private String id;
    private String userId;
    private BigDecimal amount;
    private String currency;
}
