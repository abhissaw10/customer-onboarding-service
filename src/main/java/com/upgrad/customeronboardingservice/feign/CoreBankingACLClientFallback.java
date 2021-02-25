package com.upgrad.customeronboardingservice.feign;

import com.upgrad.customeronboardingservice.exception.CoreBankingException;
import com.upgrad.customeronboardingservice.model.CoreBankingResponseVO;
import com.upgrad.customeronboardingservice.model.CustomerOnboardRequestVO;
import org.springframework.stereotype.Component;

@Component
public class CoreBankingACLClientFallback implements CoreBankingACLClient{
    @Override
    public CoreBankingResponseVO validateCustomerInfo(CustomerOnboardRequestVO customerOnboardRequestVO) throws CoreBankingException {
        throw new CoreBankingException();
    }
}
