package com.igot.cios.lti.repository;

import com.igot.cios.lti.entity.LtiProviderRegistrationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface LtiProviderRegistrationRepository
        extends JpaRepository<LtiProviderRegistrationEntity, String> {

    Optional<LtiProviderRegistrationEntity> findByClientId(String clientId);

    /** Several registrations may share a partnerCode: same partner, distinct registrations. */
    List<LtiProviderRegistrationEntity> findByPartnerCode(String partnerCode);
}
