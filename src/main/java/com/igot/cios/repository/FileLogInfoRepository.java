package com.igot.cios.repository;

import com.igot.cios.entity.FileLogInfoEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;
import java.util.List;

@Repository
public interface FileLogInfoRepository extends JpaRepository<FileLogInfoEntity, String> {
    List<FileLogInfoEntity> findByFileId(String fileId);

    @Transactional
    void deleteByFileId(String fileId);
}
