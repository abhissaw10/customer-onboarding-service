package com.upgrad.customeronboardingservice.controller;

import com.upgrad.customeronboardingservice.exception.CoreBankingException;
import com.upgrad.customeronboardingservice.exception.CustomerDoesNotExistCoreBankingException;
import com.upgrad.customeronboardingservice.exception.CustomerDoesNotExistException;
import com.upgrad.customeronboardingservice.model.CustomerOnboardRequestVO;
import com.upgrad.customeronboardingservice.model.CustomerOnboardResponseVO;
import com.upgrad.customeronboardingservice.repository.CustomerOnboardingRepository;
import com.upgrad.customeronboardingservice.service.CustomerOnboardingService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class CustomerOnboardingController {

    private CustomerOnboardingService service;

    public CustomerOnboardingController(CustomerOnboardingService service) {
        this.service = service;
    }

    @PostMapping("/customer/v1/onboard")
    public ResponseEntity<CustomerOnboardResponseVO> onboardCustomer(@RequestBody CustomerOnboardRequestVO customerOnboardVO){
        return ResponseEntity.ok(service.register(customerOnboardVO));
    }

    @PostMapping("/customer/v1/validate")
    public ResponseEntity<CustomerOnboardResponseVO> validateCoreCustomer(@RequestBody CustomerOnboardRequestVO customerOnboardVO) throws CoreBankingException, CustomerDoesNotExistCoreBankingException {
        return ResponseEntity.ok(service.validate(customerOnboardVO));
    }

    @GetMapping("/v1/customer/{customerId}")
    public ResponseEntity<CustomerOnboardResponseVO> getCustomer(@PathVariable String customerId) throws CustomerDoesNotExistException {
        return ResponseEntity.ok(service.getCustomer(customerId));
    }
}
