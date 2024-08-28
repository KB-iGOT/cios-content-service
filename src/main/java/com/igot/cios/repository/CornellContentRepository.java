package com.igot.cios.repository;


import com.igot.cios.entity.CornellContentEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CornellContentRepository extends JpaRepository<CornellContentEntity, String> {

    Optional<CornellContentEntity> findByExternalId(String externalId);

    List<CornellContentEntity> findByIsActive(boolean b);

    @Query("SELECT c FROM CornellContentEntity c WHERE c.isActive = :isActive")
    Page<Object> findAllCiosDataAndIsActive(@Param("isActive") Boolean isActive, Pageable pageable);
}
