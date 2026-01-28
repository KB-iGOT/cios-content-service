package com.igot.cios.scheduler;

import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.service.CiosContentService;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.jcraft.jsch.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.stream.Collectors;

@Slf4j
@Service
public class HarvardSchedulerService {

    private final CbServerProperties cbServerProperties;
    private final CiosContentService ciosContentService;

    public HarvardSchedulerService(CbServerProperties cbServerProperties, CiosContentService ciosContentService) {
        this.cbServerProperties = cbServerProperties;
        this.ciosContentService = ciosContentService;
    }

    public SBApiResponse loadHarvardEnrollment() {
        log.info("HarvardSchedulerService :: loadHarvardEnrollment() - Starting Harvard file processing");
        SBApiResponse apiResponse = SBApiResponse.createDefaultResponse("harvard.enrollment");
        Session session = null;
        ChannelSftp channelSftp = null;
        int totalFiles = 0;
        int processedFiles = 0;
        int failedFiles = 0;

        try {
            session = createSftpSession();
            channelSftp = connectToSftp(session);

            List<ChannelSftp.LsEntry> fileList = listFiles(channelSftp);

            if (CollectionUtils.isEmpty(fileList)) {
                log.info("No files found in Harvard SFTP directory");
                apiResponse.put("message", "No files found in Harvard SFTP directory");
                apiResponse.put("totalFiles", 0);
                apiResponse.put("processedFiles", 0);
                apiResponse.put("failedFiles", 0);
                apiResponse.setResponseCode(HttpStatus.OK);
                return apiResponse;
            }

            for (ChannelSftp.LsEntry entry : fileList) {
                String fileName = entry.getFilename();

                if (isValidFile(fileName)) {
                    totalFiles++;
                    log.info("Processing file: {}", fileName);
                    try {
                        processFile(channelSftp, fileName);
                        processedFiles++;
                    } catch (Exception e) {
                        failedFiles++;
                        log.error("Failed to process file: {}", fileName, e);
                    }
                } else {
                    log.debug("Skipping non-supported file: {}", fileName);
                }
            }
            log.info("Harvard file processing completed. Total: {}, Processed: {}, Failed: {}", totalFiles, processedFiles, failedFiles);
            apiResponse.put("message", "Harvard file processing completed");
            apiResponse.setResponseCode(HttpStatus.OK);
            return apiResponse;

        } catch (Exception e) {
            log.error("Error in loadHarvardEnrollment", e);
            apiResponse.getParams().setErrmsg("Failed to process Harvard SFTP files: " + e.getMessage());
            apiResponse.getParams().setStatus(Constants.FAILED);
            apiResponse.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return apiResponse;
        } finally {
            disconnectSftp(channelSftp, session);
        }
    }

    private Session createSftpSession() throws JSchException {
        log.info("Creating SFTP session to host: {}", cbServerProperties.getHarvardSftpHost());

        JSch jsch = new JSch();
        Session session = jsch.getSession(
                cbServerProperties.getHarvardSftpUsername(),
                cbServerProperties.getHarvardSftpHost(),
                cbServerProperties.getHarvardSftpPort()
        );

        session.setPassword(cbServerProperties.getHarvardSftpPassword());

        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);

        session.connect();
        log.info("SFTP session connected successfully");

        return session;
    }

    private ChannelSftp connectToSftp(Session session) throws JSchException {
        log.info("Opening SFTP channel");
        Channel channel = session.openChannel("sftp");
        channel.connect();
        return (ChannelSftp) channel;
    }

    private List<ChannelSftp.LsEntry> listFiles(ChannelSftp channelSftp) throws SftpException {
        log.info("Listing files in directory: {}", cbServerProperties.getHarvardSftpRemoteDirectory());
        Vector<ChannelSftp.LsEntry> vector = channelSftp.ls(cbServerProperties.getHarvardSftpRemoteDirectory());
        return new ArrayList<>(vector);
    }

    private boolean isValidFile(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return false;
        }

        String lowerCaseFileName = fileName.toLowerCase();
        List<String> allowedExtensions = Arrays.stream(cbServerProperties.getHarvardAllowedFileExtensions().split(","))
                .map(String::trim)
                .map(String::toLowerCase)
                .toList();

        return allowedExtensions.stream()
                .anyMatch(lowerCaseFileName::endsWith);
    }

    private void processFile(ChannelSftp channelSftp, String fileName) {
        try {
            String remoteFilePath = cbServerProperties.getHarvardSftpRemoteDirectory() + Constants.PATH_DELIMITER + fileName;
            byte[] fileContent = downloadFile(channelSftp, remoteFilePath);
            boolean success = callLoadContentProgressFromExcel(fileName, fileContent);
            if (success) {
                moveFileToCompleted(channelSftp, fileName);
            } else {
                log.error("File processing failed for: {}, file will not be moved", fileName);
                throw new CiosContentException(Constants.ERROR, "File processing failed for: " + fileName,
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }

        } catch (Exception e) {
            log.error("Error processing file: {}", fileName, e);
        }
    }

    private byte[] downloadFile(ChannelSftp channelSftp, String remoteFilePath) {
        log.info("Downloading file: {}", remoteFilePath);

        try (InputStream inputStream = channelSftp.get(remoteFilePath);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[8192];
            int bytesRead;

            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }

            log.info("File downloaded successfully: {} ({} bytes)", remoteFilePath, outputStream.size());
            return outputStream.toByteArray();

        } catch (Exception e) {
            log.error("Error downloading file: {}", remoteFilePath, e);
            throw new CiosContentException(Constants.ERROR, "Failed to download file: " + remoteFilePath, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private boolean callLoadContentProgressFromExcel(String fileName, byte[] fileContent) {
        log.info("Calling loadContentProgressFromExcel for file: {}", fileName);
        try {
            MultipartFile multipartFile = new MockMultipartFile(
                    "file",
                    fileName,
                    "text/csv",
                    fileContent
            );
            ciosContentService.loadContentProgressFromExcel(multipartFile, cbServerProperties.getHarvardPartnerCode());
            log.info("Successfully processed file: {} with partner code: {}", fileName, cbServerProperties.getHarvardPartnerCode());
            return true;
        } catch (Exception e) {
            log.error("Error processing file: {}. Error: {}", fileName, e.getMessage(), e);
            return false;
        }
    }


    private void moveFileToCompleted(ChannelSftp channelSftp, String fileName) {
        try {
            String sourceFile = cbServerProperties.getHarvardSftpRemoteDirectory() + "/" + fileName;
            String targetFile = cbServerProperties.getHarvardSftpCompletedDirectory() + "/" + fileName;

            ensureCompletedDirectoryExists(channelSftp);

            channelSftp.rename(sourceFile, targetFile);
            log.info("File moved to completed directory: {} -> {}", sourceFile, targetFile);

        } catch (SftpException e) {
            log.error("Error moving file to completed directory: {}", fileName, e);
            throw new CiosContentException(Constants.ERROR, "Failed to move file: " + fileName + " to completed directory. " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void ensureCompletedDirectoryExists(ChannelSftp channelSftp) {
        try {
            String completedDir = cbServerProperties.getHarvardSftpCompletedDirectory();
            channelSftp.cd(completedDir);
        } catch (SftpException e) {
            try {
                log.info("Creating completed directory: {}", cbServerProperties.getHarvardSftpCompletedDirectory());
                channelSftp.mkdir(cbServerProperties.getHarvardSftpCompletedDirectory());
            } catch (SftpException ex) {
                log.error("Failed to create completed directory", ex);
            }
        }
    }

    private void disconnectSftp(ChannelSftp channelSftp, Session session) {
        if (channelSftp != null && channelSftp.isConnected()) {
            channelSftp.disconnect();
            log.info("SFTP channel disconnected");
        }
        if (session != null && session.isConnected()) {
            session.disconnect();
            log.info("SFTP session disconnected");
        }
    }
}
