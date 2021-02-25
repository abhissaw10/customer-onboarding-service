package com.upgrad.customeronboardingservice.controller.advice;

import com.upgrad.customeronboardingservice.exception.CoreBankingException;
import com.upgrad.customeronboardingservice.exception.CustomerDoesNotExistCoreBankingException;
import com.upgrad.customeronboardingservice.exception.CustomerDoesNotExistException;
import com.upgrad.customeronboardingservice.model.ErrorResponseVO;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import static com.upgrad.customeronboardingservice.constants.CustomerOnboardingConstants.*;

@ControllerAdvice
public class CustomerOboardingControllerAdvice {

    @ExceptionHandler(CoreBankingException.class)
    @ResponseStatus(HttpStatus.GATEWAY_TIMEOUT)
    public @ResponseBody ErrorResponseVO handleCoreBankingException(){
        return ErrorResponseVO.builder().errorCode(CORE_BANKING_ERROR_CODE).build();
    }

    @ExceptionHandler(CustomerDoesNotExistCoreBankingException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public @ResponseBody ErrorResponseVO handleCustomerDoesNotExistCoreException(){
        return ErrorResponseVO.builder().errorCode(CUSTOMER_NOT_FOUND_CORE_BANKING_ERROR_CODE).build();
    }

    @ExceptionHandler(CustomerDoesNotExistException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public @ResponseBody ErrorResponseVO handleCustomerDoesNotExistException(){
        return ErrorResponseVO.builder().errorCode(CUSTOMER_NOT_FOUND_ERROR_CODE).build();
    }

}
