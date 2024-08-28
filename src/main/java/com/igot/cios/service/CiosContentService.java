package com.igot.cios.service;


import com.igot.cios.dto.PaginatedResponse;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.entity.ContentPartnerEntity;
import com.igot.cios.entity.FileInfoEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;
import java.util.Optional;


@Service
public interface CiosContentService {
    void loadContentFromExcel(MultipartFile file, String name) throws IOException;

    PaginatedResponse<Object> fetchAllContentFromDb(RequestDto dto);

    void loadContentProgressFromExcel(MultipartFile file, String providerName);

    List<FileInfoEntity> getAllFileInfos(String partnerId);
    //ContentPartnerEntity getContentDetailsByPartnerName(String name);
}
