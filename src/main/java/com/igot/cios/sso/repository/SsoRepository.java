package com.igot.cios.sso.repository;

import com.igot.cios.sso.entity.SSOConfiguration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SsoRepository extends JpaRepository<SSOConfiguration,String> {
}
