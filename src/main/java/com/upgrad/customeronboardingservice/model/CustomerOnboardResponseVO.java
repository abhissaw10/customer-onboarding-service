package com.upgrad.customeronboardingservice.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomerOnboardResponseVO {
    private String customerId;
    private String coreBankingCustomerId;
    private String onBoardingStatus;
    private String customerMobileNumber;
    private LocalDate customerDob;
}
