package com.igot.cios.storage;

import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.sunbird.cloud.storage.BaseStorageService;
import org.sunbird.cloud.storage.factory.StorageConfig;
import org.sunbird.cloud.storage.factory.StorageServiceFactory;
import scala.Option;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class StoreFileToGCP {

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
}
