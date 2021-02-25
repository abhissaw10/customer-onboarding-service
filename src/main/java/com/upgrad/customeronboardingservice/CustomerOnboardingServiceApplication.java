package com.upgrad.customeronboardingservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients
public class CustomerOnboardingServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(CustomerOnboardingServiceApplication.class, args);
	}

}
