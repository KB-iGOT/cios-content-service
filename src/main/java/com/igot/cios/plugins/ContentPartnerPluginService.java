package com.igot.cios.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.RequestDto;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public interface ContentPartnerPluginService {
    void loadContentFromExcel(JsonNode processedData, String partnerCode,String fileName,String fileId,List<Object> contentJson) throws IOException;
    Page<?> fetchAllContentFromSecondaryDb(RequestDto dto);
    Object readContentByExternalId(String externalid);
    Object updateContent(JsonNode jsonNode,String partnerCode);
    ResponseEntity<?> deleteNotPublishContent(DeleteContentRequestDto deleteContentRequestDto);
}
