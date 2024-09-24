package com.igot.cios.repository;

import com.igot.cios.entity.UpgradContentEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface UpgradContentRepository extends JpaRepository<UpgradContentEntity, String> {

    Optional<UpgradContentEntity> findByExternalId(String externalId);

    List<UpgradContentEntity> findByIsActive(boolean b);
    @Query(value = "SELECT c.cios_data FROM upgrad_content_entity c " +
            "WHERE c.is_active = :isActive " +
            "AND LOWER(c.cios_data->'content'->>'name') ILIKE LOWER(CONCAT('%', :keyword, '%'))",
            nativeQuery = true)
    Page<UpgradContentEntity> findAllCiosDataAndIsActive(@Param("isActive") Boolean isActive, Pageable pageable, @Param("keyword") String keyword);

    List<UpgradContentEntity> findByExternalIdIn(List<String> externalIds);
}
