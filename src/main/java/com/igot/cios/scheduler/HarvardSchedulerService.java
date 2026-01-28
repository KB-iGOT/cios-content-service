package com.igot.cios.scheduler;

import com.igot.cios.exception.CiosContentException;
import com.igot.cios.service.CiosContentService;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.jcraft.jsch.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

@Slf4j
@Service
public class HarvardSchedulerService {

    private final CbServerProperties cbServerProperties;
    private final CiosContentService ciosContentService;

    public HarvardSchedulerService(CbServerProperties cbServerProperties, CiosContentService ciosContentService) {
        this.cbServerProperties = cbServerProperties;
        this.ciosContentService = ciosContentService;
    }

    public void loadHarvardEnrollment() {
        log.info("HarvardSchedulerService :: loadHarvardEnrollment() - Starting Harvard file processing");
        Session session = null;
        ChannelSftp channelSftp = null;

        try {
            session = createSftpSession();
            channelSftp = connectToSftp(session);

            List<ChannelSftp.LsEntry> fileList = listFiles(channelSftp);

            if (fileList == null || fileList.isEmpty()) {
                log.info("No files found in Harvard SFTP directory");
                return;
            }

            for (ChannelSftp.LsEntry entry : fileList) {
                String fileName = entry.getFilename();

                if (isValidFile(fileName)) {
                    log.info("Processing file: {}", fileName);
                    processFile(channelSftp, fileName);
                } else {
                    log.debug("Skipping non-supported file: {}", fileName);
                }
            }

        } catch (Exception e) {
            log.error("Error in loadHarvardEnrollment", e);
            throw new CiosContentException(Constants.ERROR, "Failed to process Harvard SFTP files: " + e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
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
        String lowerCaseFileName = fileName.toLowerCase();
        return lowerCaseFileName.endsWith(".xlsx")
                || lowerCaseFileName.endsWith(".xls")
                || lowerCaseFileName.endsWith(".csv");
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
            }

        } catch (Exception e) {
            log.error("Error processing file: {}", fileName, e);
        }
    }

    private byte[] downloadFile(ChannelSftp channelSftp, String remoteFilePath) throws SftpException {
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
