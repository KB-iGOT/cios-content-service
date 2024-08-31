package com.igot.cios.controller;

import com.igot.cios.dto.RequestDto;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.service.CiosContentService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping("/ciosIntegration")
@Slf4j
public class CiosContentController {
    @Autowired
    CiosContentService ciosContentService;

    @PostMapping(value = "/v1/loadContentFromExcel/{partnerName}", consumes = "multipart/form-data")
    public ResponseEntity<Object> loadContentFromExcel(
            @RequestParam(value = "file") MultipartFile file,
            @PathVariable("partnerName") String partnerName) {
        try {
            ciosContentService.loadContentFromExcel(file, partnerName);
            return ResponseEntity.status(HttpStatus.OK).body(new HashMap<>());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error during loading of content from excel: " + e.getMessage());
        }
    }

    @PostMapping(value = "/v1/readAllContentFromDb")
    public ResponseEntity<?> fetchContentFromDb(@RequestBody RequestDto dto) {
        try {
            return ResponseEntity.ok(ciosContentService.fetchAllContentFromDb(dto));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error during fetching of content from db: " + e.getMessage());
        }
    }

    @PostMapping(value = "/v1/loadContentProgressFromExcel", consumes = "multipart/form-data")
    public ResponseEntity<Object> loadContentProgressFromExcel(@RequestParam(value = "file") MultipartFile file, @RequestParam(value = "partnerName") String providerName) {
        try {
            ciosContentService.loadContentProgressFromExcel(file, providerName);
            return ResponseEntity.status(HttpStatus.OK).body(new HashMap<>());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error during loading of content from excel: " + e.getMessage());
        }
    }

    @GetMapping("/v1/file/info/{partnerId}")
    public ResponseEntity<List<FileInfoEntity>> getAllFileInfosByPartnerId(@PathVariable String partnerId) {
        List<FileInfoEntity> fileInfos = ciosContentService.getAllFileInfos(partnerId);
        if (fileInfos.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(fileInfos);
    }

}
