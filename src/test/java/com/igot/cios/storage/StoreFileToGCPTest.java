package com.igot.cios.storage;

import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.sunbird.cloud.storage.BaseStorageService;
import org.sunbird.cloud.storage.factory.StorageConfig;
import org.sunbird.cloud.storage.factory.StorageServiceFactory;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StoreFileToGCPTest {

    private StoreFileToGCP service;

    private BaseStorageService storageService;
    private CbServerProperties cbProps;

    @BeforeEach
    void setUp() {
        service = new StoreFileToGCP();
        storageService = mock(BaseStorageService.class);
        cbProps = mock(CbServerProperties.class);

        when(cbProps.getCiosCloudContainerName()).thenReturn("cios-container");
        when(cbProps.getCiosContentFileCloudFolderName()).thenReturn("cios-folder");
        when(cbProps.getCloudStorageTypeName()).thenReturn("gcloud");
        when(cbProps.getCloudStorageKey()).thenReturn("key");
        when(cbProps.getCloudStorageSecret()).thenReturn("secret-with-\\n-newline");
        when(cbProps.getCloudStorageEndpoint()).thenReturn("https://gcs.example.com");

        ReflectionTestUtils.setField(service, "cbServerProperties", cbProps);
        ReflectionTestUtils.setField(service, "storageService", storageService);
    }

    @Test
    void init_shouldInitializeStorageServiceFromFactory() {
        StoreFileToGCP freshService = new StoreFileToGCP();
        ReflectionTestUtils.setField(freshService, "cbServerProperties", cbProps);

        BaseStorageService factoryService = mock(BaseStorageService.class);
        try (MockedStatic<StorageServiceFactory> mocked = Mockito.mockStatic(StorageServiceFactory.class)) {
            mocked.when(() -> StorageServiceFactory.getStorageService(any(StorageConfig.class)))
                    .thenReturn(factoryService);

            freshService.init();

            BaseStorageService set = (BaseStorageService) ReflectionTestUtils.getField(freshService, "storageService");
            assertNotNull(set);
            assertEquals(factoryService, set);
        }
    }

    @Test
    void uploadFile_shouldUploadAndReturnUrlAndDeleteFile() throws Exception {
        File temp = File.createTempFile("uploadFile_ok_", ".txt");
        try (FileWriter fw = new FileWriter(temp)) { fw.write("hello"); }
        when(storageService.upload(anyString(), anyString(), anyString(), any(), any(), any(), any()))
                .thenReturn("https://cdn.example.com/cios-folder/" + temp.getName());

        SBApiResponse resp = service.uploadFile(temp, "cios-folder", "cios-container");

        assertNotNull(resp);
        assertTrue(resp.getResult().containsKey(Constants.NAME));
        assertTrue(resp.getResult().containsKey(Constants.URL));
        assertEquals(temp.getName(), resp.getResult().get(Constants.NAME));
        assertEquals("https://cdn.example.com/cios-folder/" + temp.getName(), resp.getResult().get(Constants.URL));
        assertFalse(temp.exists(), "Temp file should be deleted by uploadFile");
        verify(storageService, times(1))
                .upload(eq("cios-container"), anyString(), contains("cios-folder"), any(), any(), any(), any());
    }

    @Test
    void uploadFile_whenUploadThrows_shouldReturnFailedAndDeleteFile() throws Exception {
        File temp = File.createTempFile("uploadFile_err_", ".txt");
        when(storageService.upload(anyString(), anyString(), anyString(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("oops"));


        SBApiResponse resp = service.uploadFile(temp, "cios-folder", "cios-container");

        assertNotNull(resp);
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertEquals(500, resp.getResponseCode().value());
        assertFalse(temp.exists(), "Temp file should be deleted on failure as well");
    }

    @Test
    void uploadCiosLogsFile_shouldCopyThenUploadAndDeleteOriginal() throws Exception {
        File original = File.createTempFile("cioslogs_ok_", ".log");
        try (FileWriter fw = new FileWriter(original)) { fw.write("log-data"); }
        when(storageService.upload(anyString(), anyString(), anyString(), any(), any(), any(), any()))
                .thenReturn("https://cdn.example.com/cios-folder/copied.log");

        SBApiResponse resp = service.uploadCiosLogsFile(original, "cios-container", "cios-folder");

        assertNotNull(resp);
        assertTrue(resp.getResult().containsKey(Constants.URL));
        assertFalse(original.exists(), "Original file must be deleted in finally");
        verify(storageService, times(1))
                .upload(eq("cios-container"), anyString(), contains("cios-folder/"), any(), any(), any(), any());
    }

    @Test
    void uploadCiosLogsFile_whenInputFileMissing_shouldReturnFailed() {
        File missing = new File("surely_missing_" + System.nanoTime() + ".log");
        assertFalse(missing.exists());

        SBApiResponse resp = service.uploadCiosLogsFile(missing, "cios-container", "cios-folder");

        assertNotNull(resp);
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertEquals(500, resp.getResponseCode().value());
    }

    @Test
    void uploadCiosContentFile_shouldTransferThenUpload()  {
        byte[] data = "excel,content".getBytes(StandardCharsets.UTF_8);
        MockMultipartFile mf = new MockMultipartFile("file", "content.xlsx", "application/vnd.ms-excel", data);
        when(storageService.upload(anyString(), anyString(), anyString(), any(), any(), any(), any()))
                .thenReturn("https://cdn.example.com/cios-folder/content.xlsx");

        SBApiResponse resp = service.uploadCiosContentFile(mf, "cios-container", "cios-folder");

        assertNotNull(resp);
        assertTrue(resp.getResult().containsKey(Constants.URL));
        verify(storageService, times(1))
                .upload(eq("cios-container"), anyString(), contains("cios-folder/"), any(), any(), any(), any());
    }

    @Test
    void uploadCiosContentFile_whenTransferFails_shouldReturnFailed() throws Exception {
        org.springframework.web.multipart.MultipartFile failing = mock(org.springframework.web.multipart.MultipartFile.class);
        when(failing.getOriginalFilename()).thenReturn("bad.xlsx");
        doThrow(new RuntimeException("transfer fail")).when(failing).transferTo(any(File.class));

        SBApiResponse resp = service.uploadCiosContentFile(failing, "cios-container", "cios-folder");

        assertNotNull(resp);
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertEquals(500, resp.getResponseCode().value());
    }

    @Test
    void downloadCiosContentFile_success() throws Exception {
        String fileName = "sample-" + System.nanoTime() + ".txt";
        Path base = Paths.get(Constants.LOCAL_BASE_PATH);
        Files.createDirectories(base);
        Path filePath = Paths.get(Constants.LOCAL_BASE_PATH + fileName);
        Files.write(filePath, "hello-world".getBytes(StandardCharsets.UTF_8));

        doNothing().when(storageService).download(anyString(), anyString(), anyString(), any());

        ResponseEntity<Object> resp = service.downloadCiosContentFile(fileName);

        assertNotNull(resp);
        assertEquals(200, resp.getStatusCode().value());
        assertTrue(resp.getHeaders().getFirst("Content-Disposition").contains(fileName));
        assertTrue(resp.getHeaders().getContentType().toString().contains("multipart/form-data"));
        assertTrue(resp.getBody() instanceof ByteArrayResource);
        ByteArrayResource body = (ByteArrayResource) resp.getBody();
        assertNotNull(body);
        assertEquals("hello-world".getBytes(StandardCharsets.UTF_8).length, body.contentLength());

        assertFalse(Files.exists(filePath), "Downloaded temp file should be deleted");
        verify(storageService, times(1))
                .download(eq("cios-container"), contains("cios-folder/"), anyString(), any());
    }

    @Test
    void downloadCiosContentFile_whenDownloadThrows_shouldReturn500() {
        String fileName = "missing-" + System.nanoTime() + ".txt";
        doThrow(new RuntimeException("network down")).when(storageService)
                .download(anyString(), anyString(), anyString(), any());

        ResponseEntity<Object> resp = service.downloadCiosContentFile(fileName);

        assertEquals(500, resp.getStatusCode().value());
        Path filePath = Paths.get(Constants.LOCAL_BASE_PATH + fileName);
        assertFalse(Files.exists(filePath));
    }
}