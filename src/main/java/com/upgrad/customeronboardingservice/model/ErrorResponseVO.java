package com.upgrad.customeronboardingservice.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ErrorResponseVO {
    private String errorCode;
    private String errorMessage;
}
