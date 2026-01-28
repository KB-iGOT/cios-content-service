package com.igot.cios.scheduler;


import com.igot.cios.service.CiosContentService;
import com.igot.cios.util.CbServerProperties;
import com.jcraft.jsch.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Vector;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class HarvardSchedulerServiceTest {

    @Mock
    private CbServerProperties cbServerProperties;

    @Mock
    private CiosContentService ciosContentService;

    @Mock
    private Session session;

    @Mock
    private ChannelSftp channelSftp;

    @Mock
    private Channel channel;

    private HarvardSchedulerService harvardSchedulerService;

    @BeforeEach
    void setUp() {
        lenient().when(cbServerProperties.getHarvardAllowedFileExtensions()).thenReturn(".xlsx,.xls,.csv");
        harvardSchedulerService = new HarvardSchedulerService(cbServerProperties, ciosContentService);
    }

    @Test
    void testConstructor() {
        assertNotNull(harvardSchedulerService);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "igot_HMM12_20260127.csv",
            "igot_hmm12_20260127.CSV",
            "test.csv",
            "test.CSV",
            "test.xlsx",
            "test.XLSX",
            "test.xls",
            "test.XLS"
    })
    void testIsValidFile_validFiles(String fileName) throws Exception {
        Method method = HarvardSchedulerService.class.getDeclaredMethod("isValidFile", String.class);
        method.setAccessible(true);

        assertTrue((Boolean) method.invoke(harvardSchedulerService, fileName));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "test.txt",
            "test.pdf",
            "test.doc",
            "test.xml",
            "test",
            "test.csvx",
            "test.xlsm"
    })
    void testIsValidFile_invalidFiles(String fileName) throws Exception {
        Method method = HarvardSchedulerService.class.getDeclaredMethod("isValidFile", String.class);
        method.setAccessible(true);

        assertFalse((Boolean) method.invoke(harvardSchedulerService, fileName));
    }

    @Test
    void testIsValidFile_blankFileName() throws Exception {
        Method method = HarvardSchedulerService.class.getDeclaredMethod("isValidFile", String.class);
        method.setAccessible(true);

        assertFalse((Boolean) method.invoke(harvardSchedulerService, ""));
        assertFalse((Boolean) method.invoke(harvardSchedulerService, "   "));
        assertFalse((Boolean) method.invoke(harvardSchedulerService, (String) null));
    }

    @Test
    void testIsValidFile_configDriven() throws Exception {
        when(cbServerProperties.getHarvardAllowedFileExtensions()).thenReturn(".pdf,.txt");

        Method method = HarvardSchedulerService.class.getDeclaredMethod("isValidFile", String.class);
        method.setAccessible(true);

        assertTrue((Boolean) method.invoke(harvardSchedulerService, "test.pdf"));
        assertTrue((Boolean) method.invoke(harvardSchedulerService, "test.txt"));
        assertFalse((Boolean) method.invoke(harvardSchedulerService, "test.csv"));
        assertFalse((Boolean) method.invoke(harvardSchedulerService, "test.xlsx"));
    }

    @Test
    void testIsValidFile_withSpacesInConfig() throws Exception {
        when(cbServerProperties.getHarvardAllowedFileExtensions()).thenReturn(".csv, .xlsx , .xls");

        Method method = HarvardSchedulerService.class.getDeclaredMethod("isValidFile", String.class);
        method.setAccessible(true);

        assertTrue((Boolean) method.invoke(harvardSchedulerService, "test.csv"));
        assertTrue((Boolean) method.invoke(harvardSchedulerService, "test.xlsx"));
        assertTrue((Boolean) method.invoke(harvardSchedulerService, "test.xls"));
    }

    @Test
    void testCreateSftpSession_success() throws Exception {
        when(cbServerProperties.getHarvardSftpHost()).thenReturn("transfer.hbsp.harvard.edu");
        when(cbServerProperties.getHarvardSftpPort()).thenReturn(22);
        when(cbServerProperties.getHarvardSftpUsername()).thenReturn("igot_cl");
        when(cbServerProperties.getHarvardSftpPassword()).thenReturn("password");

        Method method = HarvardSchedulerService.class.getDeclaredMethod("createSftpSession");
        method.setAccessible(true);

        assertThrows(Exception.class, () -> method.invoke(harvardSchedulerService));
    }

    @Test
    void testConnectToSftp_success() throws Exception {
        when(session.openChannel("sftp")).thenReturn(channelSftp);

        Method method = HarvardSchedulerService.class.getDeclaredMethod("connectToSftp", Session.class);
        method.setAccessible(true);

        Object result = method.invoke(harvardSchedulerService, session);

        assertNotNull(result);
        verify(channelSftp, times(1)).connect();
    }

    @Test
    void testListFiles_success() throws Exception {
        when(cbServerProperties.getHarvardSftpRemoteDirectory()).thenReturn("/igot_cl");

        Vector<ChannelSftp.LsEntry> mockVector = new Vector<>();
        ChannelSftp.LsEntry mockEntry1 = mock(ChannelSftp.LsEntry.class);
        ChannelSftp.LsEntry mockEntry2 = mock(ChannelSftp.LsEntry.class);
        mockVector.add(mockEntry1);
        mockVector.add(mockEntry2);

        when(channelSftp.ls("/igot_cl")).thenReturn(mockVector);

        Method method = HarvardSchedulerService.class.getDeclaredMethod("listFiles", ChannelSftp.class);
        method.setAccessible(true);

        Object result = method.invoke(harvardSchedulerService, channelSftp);

        assertNotNull(result);
        verify(channelSftp, times(1)).ls("/igot_cl");
    }

    @Test
    void testListFiles_emptyDirectory() throws Exception {
        when(cbServerProperties.getHarvardSftpRemoteDirectory()).thenReturn("/igot_cl");
        when(channelSftp.ls("/igot_cl")).thenReturn(new Vector<>());

        Method method = HarvardSchedulerService.class.getDeclaredMethod("listFiles", ChannelSftp.class);
        method.setAccessible(true);

        Object result = method.invoke(harvardSchedulerService, channelSftp);

        assertNotNull(result);
    }

    @Test
    void testDownloadFile_success() throws Exception {
        String remoteFilePath = "/igot_cl/igot_HMM12_20260127.csv";
        byte[] fileData = "test,data,csv\n1,2,3".getBytes();
        InputStream inputStream = new ByteArrayInputStream(fileData);

        when(channelSftp.get(remoteFilePath)).thenReturn(inputStream);

        Method method = HarvardSchedulerService.class.getDeclaredMethod("downloadFile", ChannelSftp.class, String.class);
        method.setAccessible(true);

        byte[] result = (byte[]) method.invoke(harvardSchedulerService, channelSftp, remoteFilePath);

        assertNotNull(result);
        assertTrue(result.length > 0);
        verify(channelSftp, times(1)).get(remoteFilePath);
    }

    @Test
    void testDownloadFile_exception() throws Exception {
        String remoteFilePath = "/igot_cl/nonexistent.csv";

        when(channelSftp.get(remoteFilePath)).thenThrow(new SftpException(0, "File not found"));

        Method method = HarvardSchedulerService.class.getDeclaredMethod("downloadFile", ChannelSftp.class, String.class);
        method.setAccessible(true);

        assertThrows(Exception.class, () -> method.invoke(harvardSchedulerService, channelSftp, remoteFilePath));
    }

    @Test
    void testCallLoadContentProgressFromExcel_success() throws Exception {
        String fileName = "igot_HMM12_20260127.csv";
        byte[] fileContent = "test,data\n1,2".getBytes();

        when(cbServerProperties.getHarvardPartnerCode()).thenReturn("HARVARD");
        doNothing().when(ciosContentService).loadContentProgressFromExcel(any(MultipartFile.class), eq("HARVARD"));

        Method method = HarvardSchedulerService.class.getDeclaredMethod("callLoadContentProgressFromExcel", String.class, byte[].class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(harvardSchedulerService, fileName, fileContent);

        assertTrue(result);
        verify(ciosContentService, times(1)).loadContentProgressFromExcel(any(MultipartFile.class), eq("HARVARD"));
    }

    @Test
    void testCallLoadContentProgressFromExcel_exception() throws Exception {
        String fileName = "igot_HMM12_20260127.csv";
        byte[] fileContent = "test,data\n1,2".getBytes();

        when(cbServerProperties.getHarvardPartnerCode()).thenReturn("HARVARD");
        doThrow(new RuntimeException("Processing failed")).when(ciosContentService).loadContentProgressFromExcel(any(MultipartFile.class), eq("HARVARD"));

        Method method = HarvardSchedulerService.class.getDeclaredMethod("callLoadContentProgressFromExcel", String.class, byte[].class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(harvardSchedulerService, fileName, fileContent);

        assertFalse(result);
        verify(ciosContentService, times(1)).loadContentProgressFromExcel(any(MultipartFile.class), eq("HARVARD"));
    }

    @Test
    void testProcessFile_success() throws Exception {
        String fileName = "igot_HMM12_20260127.csv";
        String remoteFilePath = "/igot_cl/igot_HMM12_20260127.csv";
        byte[] fileData = "test,data\n1,2".getBytes();
        InputStream inputStream = new ByteArrayInputStream(fileData);

        when(cbServerProperties.getHarvardSftpRemoteDirectory()).thenReturn("/igot_cl");
        when(cbServerProperties.getHarvardSftpCompletedDirectory()).thenReturn("/igot_cl/completed");
        when(cbServerProperties.getHarvardPartnerCode()).thenReturn("HARVARD");
        when(channelSftp.get(remoteFilePath)).thenReturn(inputStream);
        doNothing().when(ciosContentService).loadContentProgressFromExcel(any(MultipartFile.class), eq("HARVARD"));
        doNothing().when(channelSftp).cd("/igot_cl/completed");
        doNothing().when(channelSftp).rename(anyString(), anyString());

        Method method = HarvardSchedulerService.class.getDeclaredMethod("processFile", ChannelSftp.class, String.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(harvardSchedulerService, channelSftp, fileName));
    }

    @Test
    void testProcessFile_failure() throws Exception {
        String fileName = "igot_HMM12_20260127.csv";
        String remoteFilePath = "/igot_cl/igot_HMM12_20260127.csv";

        when(cbServerProperties.getHarvardSftpRemoteDirectory()).thenReturn("/igot_cl");
        when(channelSftp.get(remoteFilePath)).thenThrow(new SftpException(0, "File not found"));

        Method method = HarvardSchedulerService.class.getDeclaredMethod("processFile", ChannelSftp.class, String.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(harvardSchedulerService, channelSftp, fileName));
    }

    @Test
    void testMoveFileToCompleted_success() throws Exception {
        String fileName = "igot_HMM12_20260127.csv";
        String sourceFile = "/igot_cl/igot_HMM12_20260127.csv";
        String targetFile = "/igot_cl/completed/igot_HMM12_20260127.csv";

        when(cbServerProperties.getHarvardSftpRemoteDirectory()).thenReturn("/igot_cl");
        when(cbServerProperties.getHarvardSftpCompletedDirectory()).thenReturn("/igot_cl/completed");
        doNothing().when(channelSftp).cd("/igot_cl/completed");
        doNothing().when(channelSftp).rename(sourceFile, targetFile);

        Method method = HarvardSchedulerService.class.getDeclaredMethod("moveFileToCompleted", ChannelSftp.class, String.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(harvardSchedulerService, channelSftp, fileName));
        verify(channelSftp, times(1)).rename(sourceFile, targetFile);
    }

    @Test
    void testMoveFileToCompleted_exception() throws Exception {
        String fileName = "igot_HMM12_20260127.csv";
        String sourceFile = "/igot_cl/igot_HMM12_20260127.csv";
        String targetFile = "/igot_cl/completed/igot_HMM12_20260127.csv";

        when(cbServerProperties.getHarvardSftpRemoteDirectory()).thenReturn("/igot_cl");
        when(cbServerProperties.getHarvardSftpCompletedDirectory()).thenReturn("/igot_cl/completed");
        doNothing().when(channelSftp).cd("/igot_cl/completed");
        doThrow(new SftpException(0, "Rename failed")).when(channelSftp).rename(sourceFile, targetFile);

        Method method = HarvardSchedulerService.class.getDeclaredMethod("moveFileToCompleted", ChannelSftp.class, String.class);
        method.setAccessible(true);

        assertThrows(Exception.class, () -> method.invoke(harvardSchedulerService, channelSftp, fileName));
    }

    @Test
    void testEnsureCompletedDirectoryExists_directoryExists() throws Exception {
        when(cbServerProperties.getHarvardSftpCompletedDirectory()).thenReturn("/igot_cl/completed");
        doNothing().when(channelSftp).cd("/igot_cl/completed");

        Method method = HarvardSchedulerService.class.getDeclaredMethod("ensureCompletedDirectoryExists", ChannelSftp.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(harvardSchedulerService, channelSftp));
        verify(channelSftp, times(1)).cd("/igot_cl/completed");
        verify(channelSftp, never()).mkdir(anyString());
    }

    @Test
    void testEnsureCompletedDirectoryExists_directoryNotExists() throws Exception {
        when(cbServerProperties.getHarvardSftpCompletedDirectory()).thenReturn("/igot_cl/completed");
        doThrow(new SftpException(0, "No such file")).when(channelSftp).cd("/igot_cl/completed");
        doNothing().when(channelSftp).mkdir("/igot_cl/completed");

        Method method = HarvardSchedulerService.class.getDeclaredMethod("ensureCompletedDirectoryExists", ChannelSftp.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(harvardSchedulerService, channelSftp));
        verify(channelSftp, times(1)).cd("/igot_cl/completed");
        verify(channelSftp, times(1)).mkdir("/igot_cl/completed");
    }

    @Test
    void testEnsureCompletedDirectoryExists_createFails() throws Exception {
        when(cbServerProperties.getHarvardSftpCompletedDirectory()).thenReturn("/igot_cl/completed");
        doThrow(new SftpException(0, "No such file")).when(channelSftp).cd("/igot_cl/completed");
        doThrow(new SftpException(0, "Permission denied")).when(channelSftp).mkdir("/igot_cl/completed");

        Method method = HarvardSchedulerService.class.getDeclaredMethod("ensureCompletedDirectoryExists", ChannelSftp.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(harvardSchedulerService, channelSftp));
    }

    @Test
    void testDisconnectSftp_bothConnected() throws Exception {
        when(channelSftp.isConnected()).thenReturn(true);
        when(session.isConnected()).thenReturn(true);

        Method method = HarvardSchedulerService.class.getDeclaredMethod("disconnectSftp", ChannelSftp.class, Session.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(harvardSchedulerService, channelSftp, session));

        verify(channelSftp, times(1)).disconnect();
        verify(session, times(1)).disconnect();
    }

    @Test
    void testDisconnectSftp_bothNull() throws Exception {
        Method method = HarvardSchedulerService.class.getDeclaredMethod("disconnectSftp", ChannelSftp.class, Session.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(harvardSchedulerService, null, null));
    }

    @Test
    void testDisconnectSftp_channelNull() throws Exception {
        when(session.isConnected()).thenReturn(true);

        Method method = HarvardSchedulerService.class.getDeclaredMethod("disconnectSftp", ChannelSftp.class, Session.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(harvardSchedulerService, null, session));

        verify(session, times(1)).disconnect();
    }

    @Test
    void testDisconnectSftp_sessionNull() throws Exception {
        when(channelSftp.isConnected()).thenReturn(true);

        Method method = HarvardSchedulerService.class.getDeclaredMethod("disconnectSftp", ChannelSftp.class, Session.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(harvardSchedulerService, channelSftp, null));

        verify(channelSftp, times(1)).disconnect();
    }

    @Test
    void testDisconnectSftp_notConnected() throws Exception {
        when(channelSftp.isConnected()).thenReturn(false);
        when(session.isConnected()).thenReturn(false);

        Method method = HarvardSchedulerService.class.getDeclaredMethod("disconnectSftp", ChannelSftp.class, Session.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(harvardSchedulerService, channelSftp, session));

        verify(channelSftp, never()).disconnect();
        verify(session, never()).disconnect();
    }
}

