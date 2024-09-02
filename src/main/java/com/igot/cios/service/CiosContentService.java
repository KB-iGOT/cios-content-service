package com.igot.cios.service;


import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.PaginatedResponse;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.entity.ContentPartnerEntity;
import com.igot.cios.entity.FileInfoEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;


@Service
public interface CiosContentService {
    void loadContentFromExcel(MultipartFile file, String partnerName) throws IOException;

    PaginatedResponse<?> fetchAllContentFromDb(RequestDto dto);

    void loadContentProgressFromExcel(MultipartFile file, String providerName);

    ContentPartnerEntity getContentDetailsByPartnerName(String name);

    List<FileInfoEntity> getAllFileInfos(String partnerId);

    ResponseEntity<?> deleteNotPublishContent(DeleteContentRequestDto deleteContentRequestDto);
}
