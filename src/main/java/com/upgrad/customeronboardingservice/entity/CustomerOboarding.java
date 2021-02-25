package com.upgrad.customeronboardingservice.entity;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDate;
import java.util.Date;

@Data
@Builder
@Document
public class CustomerOboarding {

    @Id
    private String customerId;
    private String coreCustomerId;
    private String customerName;
    private String customerMobileNumber;
    private LocalDate customerDob;
    private String customerStatus;
}
