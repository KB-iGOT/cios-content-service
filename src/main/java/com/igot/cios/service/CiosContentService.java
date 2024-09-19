package com.igot.cios.service;

import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.PaginatedResponse;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.entity.FileInfoEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.io.IOException;
import java.util.List;


@Service
public interface CiosContentService {
    void loadContentFromExcel(MultipartFile file, String partnerCode) throws IOException;

    PaginatedResponse<?> fetchAllContentFromSecondaryDb(RequestDto dto);

    void loadContentProgressFromExcel(MultipartFile file, String orgId);

    List<FileInfoEntity> getAllFileInfos(String partnerId);

    ResponseEntity<?> deleteNotPublishContent(DeleteContentRequestDto deleteContentRequestDto);

    Object readContentByExternalId(String partnercode, String externalid);
}
