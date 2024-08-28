package com.igot.cios.service;


import com.igot.cios.dto.PaginatedResponse;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.entity.ContentPartnerEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;


@Service
public interface CiosContentService {
    void loadContentFromExcel(MultipartFile file, String name) throws IOException;

    PaginatedResponse<Object> fetchAllContentFromDb(RequestDto dto);

    void loadContentProgressFromExcel(MultipartFile file,String providerName);

    //ContentPartnerEntity getContentDetailsByPartnerName(String name);
}
