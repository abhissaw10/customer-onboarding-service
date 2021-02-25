package com.upgrad.customeronboardingservice.service;

import com.upgrad.customeronboardingservice.entity.CustomerOboarding;
import com.upgrad.customeronboardingservice.exception.CoreBankingException;
import com.upgrad.customeronboardingservice.exception.CustomerDoesNotExistCoreBankingException;
import com.upgrad.customeronboardingservice.exception.CustomerDoesNotExistException;
import com.upgrad.customeronboardingservice.feign.CoreBankingACLClient;
import com.upgrad.customeronboardingservice.model.CoreBankingResponseVO;
import com.upgrad.customeronboardingservice.model.CustomerOnboardRequestVO;
import com.upgrad.customeronboardingservice.model.CustomerOnboardResponseVO;
import com.upgrad.customeronboardingservice.repository.CustomerOnboardingRepository;
import feign.FeignException;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

import static com.upgrad.customeronboardingservice.constants.CustomerOnboardingConstants.ONBOARDING_STATUS;

@Service
@Log4j2
public class CustomerOnboardingService {

    private CustomerOnboardingRepository repository;
    private CoreBankingACLClient coreBankingACLClient;

    @Value("${notification-topic}")
    private String notificationTopic;

    @Autowired
    KafkaTemplate kafkaTemplate;

    public CustomerOnboardingService(CustomerOnboardingRepository repository, CoreBankingACLClient coreBankingACLClient) {
        this.repository = repository;
        this.coreBankingACLClient = coreBankingACLClient;
    }

    public CustomerOnboardResponseVO register(CustomerOnboardRequestVO customerOnboardVO){
        String customerId = UUID.randomUUID().toString();
        CustomerOboarding customerOboarding = repository.save(CustomerOboarding
                .builder()
                .customerId(customerId)
                .customerMobileNumber(customerOnboardVO.getCustomerMobileNumber())
                .customerDob(customerOnboardVO.getCustomerDob())
                .build());
        notify(customerOnboardVO);
        return CustomerOnboardResponseVO
                .builder()
                .customerId(customerId)
                .onBoardingStatus(ONBOARDING_STATUS)
                .build();
    }

    public CustomerOnboardResponseVO validate(CustomerOnboardRequestVO customerOnboardVO) throws CoreBankingException, CustomerDoesNotExistCoreBankingException {
        CoreBankingResponseVO coreBankingResponseVO = null;
        try {
            coreBankingResponseVO = coreBankingACLClient.validateCustomerInfo(customerOnboardVO);
        }catch (FeignException e){
            throw new CoreBankingException();
        }
        if(coreBankingACLClient == null){
            throw new CustomerDoesNotExistCoreBankingException();
        }
        //validation logic goes here

        return CustomerOnboardResponseVO
                .builder()
                .coreBankingCustomerId(coreBankingResponseVO.getCoreBankingCustomerId())
                .build();
    }

    private void notify(CustomerOnboardRequestVO customerOnboardRequestVO){
        kafkaTemplate.send(MessageBuilder
                .withPayload(customerOnboardRequestVO)
                .setHeader(KafkaHeaders.TOPIC,notificationTopic)
                .build());
    }

    @KafkaListener(topics = "${notification-topic}", groupId = "1")
    private void listen(CustomerOnboardRequestVO customerOnboardRequestVO){
        log.info("Received Customer Info "+customerOnboardRequestVO);
    }

    public CustomerOnboardResponseVO getCustomer(String customerId) throws CustomerDoesNotExistException {
        CustomerOboarding customer = repository.findById(customerId)
               .map(cust -> {
                   return CustomerOboarding
                           .builder()
                           .customerName(cust.getCustomerName())
                           .coreCustomerId(cust.getCoreCustomerId())
                           .customerMobileNumber(cust.getCustomerMobileNumber())
                           .customerId(cust.getCustomerId())
                           .customerDob(cust.getCustomerDob())
                           .build();
               })
                .orElseThrow(CustomerDoesNotExistException::new);
    return CustomerOnboardResponseVO
            .builder()
            .coreBankingCustomerId(customer.getCoreCustomerId())
            .customerId(customer.getCustomerId())
            .customerDob(customer.getCustomerDob())
            .customerMobileNumber(customer.getCustomerMobileNumber())
            .onBoardingStatus(customer.getCustomerStatus())
            .build();
    }
}
