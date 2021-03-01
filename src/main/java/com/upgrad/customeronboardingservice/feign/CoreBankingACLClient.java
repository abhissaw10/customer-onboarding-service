package com.upgrad.customeronboardingservice.feign;

import com.upgrad.customeronboardingservice.model.CoreBankingResponseVO;
import com.upgrad.customeronboardingservice.model.CustomerOnboardRequestVO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.context.annotation.Primary;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@Primary
@FeignClient(name = "coreBankingAcl", url = "${coreBankingAclURL}")
public interface CoreBankingACLClient {

    @PostMapping("/customer/v1/validate")
    public CoreBankingResponseVO validateCustomerInfo(@RequestBody CustomerOnboardRequestVO customerOnboardRequestVO);
}
