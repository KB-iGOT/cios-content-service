package com.igot.cios.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cios.dto.RequestDto;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public interface ContentPartnerPluginService {
    void loadContentFromExcel(JsonNode processedData, String orgId,String fileName,String fileId,List<Object> contentJson) throws IOException;
    Page<?> fetchAllContentFromSecondaryDb(RequestDto dto);
    List<?> fetchAllContentByPartnerName();
    void deleteContent(Object contentEntity);
}
