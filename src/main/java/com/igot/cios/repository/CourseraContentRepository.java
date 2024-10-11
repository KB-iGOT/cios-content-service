package com.igot.cios.repository;

import com.igot.cios.entity.CourseraContentEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CourseraContentRepository extends JpaRepository<CourseraContentEntity, String> {

    Optional<CourseraContentEntity> findByExternalId(String externalId);

    List<CourseraContentEntity> findByIsActive(boolean b);

    @Query(value = "SELECT * FROM public.coursera_content_entity c " +
            "WHERE c.is_active = :isActive " +
            "AND LOWER(c.cios_data->'content'->>'name') ILIKE LOWER(CONCAT('%', :keyword, '%'))",
            nativeQuery = true)
    Page<CourseraContentEntity> findAllCiosDataAndIsActive(@Param("isActive") Boolean isActive, Pageable pageable, @Param("keyword") String keyword);

    List<CourseraContentEntity> findByExternalIdIn(List<String> externalIds);
}
