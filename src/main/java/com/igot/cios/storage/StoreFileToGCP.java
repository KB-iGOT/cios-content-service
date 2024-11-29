package com.igot.cios.storage;

import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.cloud.storage.BaseStorageService;
import org.sunbird.cloud.storage.factory.StorageConfig;
import org.sunbird.cloud.storage.factory.StorageServiceFactory;
import scala.Option;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class StoreFileToGCP {

    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    private BaseStorageService storageService = null;
    @Autowired
    private CbServerProperties cbServerProperties;

    @PostConstruct
    public void init() {
        if (storageService == null) {
            storageService = StorageServiceFactory.getStorageService(new StorageConfig(cbServerProperties.getCloudStorageTypeName(), cbServerProperties.getCloudStorageKey(), cbServerProperties.getCloudStorageSecret().replace("\\n", "\n"), Option.apply(cbServerProperties.getCloudStorageEndpoint()), Option.empty()));
        }
    }

    public SBApiResponse uploadCiosLogsFile(File file, String containerName, String cloudFolderName) {
        log.info("StoreFileToGCP :: uploadCiosLogsFile: uploading file to GCP {}", file);
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_CB_PLAN_PUBLISH);
        File tempFile = null;
        try {
            String uniqueFileName = System.currentTimeMillis() + "_" + file.getName();
            tempFile = new File(System.getProperty("java.io.tmpdir"), uniqueFileName);

            tempFile.createNewFile();
            try (FileInputStream fis = new FileInputStream(file); FileOutputStream fos = new FileOutputStream(tempFile)) {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    fos.write(buffer, 0, bytesRead);
                }
            }
            return uploadFile(tempFile, cloudFolderName, containerName);
        } catch (Exception e) {
            log.error("Failed to upload file. Exception: ", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Failed to upload file. Exception: " + e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        } finally {
            if (file != null) {
                file.delete();
            }
        }
    }

    public SBApiResponse uploadFile(File file, String cloudFolderName, String containerName) {
        log.info("StoreFileToGCP :: uploadFile: uploading file to GCP {}", file);
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_CB_PLAN_PUBLISH);
        try {
            String objectKey = cloudFolderName + "/" + file.getName();
            String url = storageService.upload(containerName, file.getAbsolutePath(), objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty());
            Map<String, String> uploadedFile = new HashMap<>();
            uploadedFile.put(Constants.NAME, file.getName());
            uploadedFile.put(Constants.URL, url);
            response.getResult().putAll(uploadedFile);
            return response;
        } catch (Exception e) {
            log.error("Failed to upload file. Exception: ", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Failed to upload file. Exception: " + e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        } finally {
            if (file != null) {
                file.delete();
            }
        }
    }

    public SBApiResponse uploadCiosContentFile(MultipartFile file, String containerName, String cloudFolderName) {
        log.info("StoreFileToGCP :: uploadCiosContentFile: uploading file to GCP {}", file);
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_CIOS_LOAD_EXCEL_CONTENT);
        File tempFile = null;
        try {
            String uniqueFileName = System.currentTimeMillis() + "_" + file.getOriginalFilename();

            tempFile = new File(System.getProperty("java.io.tmpdir"), uniqueFileName);
            file.transferTo(tempFile);
            return uploadFile(tempFile, cloudFolderName, containerName);
        } catch (Exception e) {
            log.error("Failed to upload file. Exception: ", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Failed to upload file. Exception: " + e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        } finally {
            if (tempFile != null && tempFile.exists()) {
                boolean deleted = tempFile.delete();
                if (!deleted) {
                    log.warn("Failed to delete temporary file: {}", tempFile.getAbsolutePath());
                }
            }
        }
    }

    public ResponseEntity<?> downloadCiosContentFile(String fileName) {
        Path tmpPath = Paths.get(Constants.LOCAL_BASE_PATH + fileName);
        try {
            String objectKey = cbServerProperties.getCiosContentFileCloudFolderName() + "/" + fileName;
            storageService.download(cbServerProperties.getCiosCloudContainerName(), objectKey, Constants.LOCAL_BASE_PATH,
                    Option.apply(Boolean.FALSE));
            ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(tmpPath));
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");
            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(tmpPath.toFile().length())
                    .contentType(MediaType.parseMediaType(MediaType.MULTIPART_FORM_DATA_VALUE))
                    .body(resource);
        } catch (Exception e) {
            logger.error("Failed to read the downloaded file: " + fileName + ", Exception: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            try {
                Files.deleteIfExists(tmpPath);
            } catch (IOException e) {
                logger.error("Failed to delete the temporary file: " + fileName + ", Exception: ", e);
            }
        }
    }
}
