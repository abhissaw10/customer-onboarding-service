package com.upgrad.customeronboardingservice.repository;

import com.upgrad.customeronboardingservice.entity.CustomerOboarding;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CustomerOnboardingRepository extends MongoRepository<CustomerOboarding,String> {
}
