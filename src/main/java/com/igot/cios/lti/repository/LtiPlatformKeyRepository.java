package com.igot.cios.lti.repository;

import com.igot.cios.lti.entity.LtiPlatformKeyEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface LtiPlatformKeyRepository extends JpaRepository<LtiPlatformKeyEntity, String> {

    Optional<LtiPlatformKeyEntity> findFirstByStatus(String status);

    /** Keys to publish in the JWKS: active plus rotating. */
    List<LtiPlatformKeyEntity> findByStatusIn(List<String> statuses);
}
