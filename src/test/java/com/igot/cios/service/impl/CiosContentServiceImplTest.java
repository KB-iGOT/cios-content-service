package com.igot.cios.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.PaginatedResponse;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.entity.CornellContentEntity;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.repository.CornellContentRepository;
import com.igot.cios.repository.FileInfoRepository;
import com.igot.cios.storage.StoreFileToGCP;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PayloadValidation;
import com.igot.cios.util.elasticsearch.dto.SearchResult;
import com.igot.cios.util.elasticsearch.service.EsUtilService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CiosContentServiceImplTest {

    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private KafkaProducer kafkaProducer;
    @Mock
    private DataTransformUtility dataTransformUtility;
    @Mock
    private CornellContentRepository repository;
    @Mock
    private FileInfoRepository fileInfoRepository;
    @Mock
    private StoreFileToGCP storeFileToGCP;
    @Mock
    private CbServerProperties cbServerProperties;
    @Mock
    private PayloadValidation payloadValidation;
    @Mock
    private EsUtilService esUtilService;
    @InjectMocks
    private CiosContentServiceImpl ciosContentService;

    @Test
    void loadContentFromExcel_validFile_uploadSuccess_triggersKafkaAndReturnsOk() {
        MultipartFile file = new MockMultipartFile("file", "test.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "dummy".getBytes());
        SBApiResponse uploadResp = SBApiResponse.createDefaultResponse(Constants.API_CIOS_LOAD_EXCEL_CONTENT);
        uploadResp.setResponseCode(HttpStatus.OK);
        uploadResp.getResult().put(Constants.NAME, "gcpFileName.xlsx");

        when(storeFileToGCP.uploadCiosContentFile(any(MultipartFile.class), anyString(), anyString()))
                .thenReturn(uploadResp);
        when(cbServerProperties.getCiosCloudContainerName()).thenReturn("container");
        when(cbServerProperties.getCiosContentFileCloudFolderName()).thenReturn("folder");
        when(dataTransformUtility.createFileInfo(anyString(), any(), anyString(), any(), any(), anyString(), any(), anyString()))
                .thenReturn("file-id-123");
        when(cbServerProperties.getCiosContentOnboardTopic()).thenReturn("cios.topic");

        SBApiResponse response = ciosContentService.loadContentFromExcel(file, "CORNELL", "partner123");

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(kafkaProducer, times(1)).push(eq("cios.topic"), anyMap());
    }

    @Test
    void loadContentFromExcel_invalidFileFormat_returnsBadRequest_andDoesNotUpload() {
        MultipartFile file = new MockMultipartFile("file", "test.txt", "text/plain", "dummy".getBytes());

        SBApiResponse response = ciosContentService.loadContentFromExcel(file, "CORNELL", "partner123");

        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertTrue(response.getParams().getErrmsg().toLowerCase().contains("invalid file format"));
        verify(storeFileToGCP, never()).uploadCiosContentFile(any(), anyString(), anyString());
    }

    @Test
    void loadContentFromExcel_uploadFails_returnsInternalServerErrorWithFailedStatus() {
        MultipartFile file = new MockMultipartFile("file", "test.csv", "text/csv", "a,b,c\n1,2,3".getBytes());
        SBApiResponse uploadResp = SBApiResponse.createDefaultResponse(Constants.API_CIOS_LOAD_EXCEL_CONTENT);
        uploadResp.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        uploadResp.getParams().setErrmsg("upload failed");

        when(storeFileToGCP.uploadCiosContentFile(any(MultipartFile.class), anyString(), anyString()))
                .thenReturn(uploadResp);
        when(cbServerProperties.getCiosCloudContainerName()).thenReturn("container");
        when(cbServerProperties.getCiosContentFileCloudFolderName()).thenReturn("folder");

        SBApiResponse response = ciosContentService.loadContentFromExcel(file, "CORNELL", "partner123");

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals("Failed", response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().toLowerCase().contains("failed to upload file"));
        verify(kafkaProducer, never()).push(anyString(), anyMap());
    }

    @Test
    void loadContentFromExcel_validExcel_uploadsAndPushesKafka() {
        MultipartFile file = new MockMultipartFile("file", "content.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "xls".getBytes());
        SBApiResponse uploadResp = SBApiResponse.createDefaultResponse(Constants.API_CIOS_LOAD_EXCEL_CONTENT);
        uploadResp.setResponseCode(HttpStatus.OK);
        uploadResp.getResult().put(Constants.NAME, "stored.xlsx");

        when(storeFileToGCP.uploadCiosContentFile(any(MultipartFile.class), anyString(), anyString()))
                .thenReturn(uploadResp);
        when(cbServerProperties.getCiosCloudContainerName()).thenReturn("container");
        when(cbServerProperties.getCiosContentFileCloudFolderName()).thenReturn("folder");
        when(dataTransformUtility.createFileInfo(anyString(), any(), anyString(), any(), any(), anyString(), any(), anyString()))
                .thenReturn("file-id");
        when(cbServerProperties.getCiosContentOnboardTopic()).thenReturn("cios.onboard");

        SBApiResponse resp = ciosContentService.loadContentFromExcel(file, "CORNELL", "partner");

        assertNotNull(resp);
        assertEquals(HttpStatus.OK, resp.getResponseCode());
        verify(storeFileToGCP, times(1)).uploadCiosContentFile(any(), anyString(), anyString());
        verify(kafkaProducer, times(1)).push(eq("cios.onboard"), anyMap());
    }

    @Test
    void loadContentFromExcel_invalidExtension_returnsBadRequest_noUpload() {
        MultipartFile file = new MockMultipartFile("file", "readme.txt", "text/plain", "data".getBytes());

        SBApiResponse resp = ciosContentService.loadContentFromExcel(file, "CORNELL", "partner");

        assertNotNull(resp);
        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
        assertTrue(resp.getParams().getErrmsg().toLowerCase().contains("invalid file format"));
        verify(storeFileToGCP, never()).uploadCiosContentFile(any(), anyString(), anyString());
        verifyNoInteractions(kafkaProducer);
    }

    @Test
    void loadContentFromExcel_uploadReturnsError_returnsInternalServerError_andNoKafka() {
        MultipartFile file = new MockMultipartFile("file", "data.csv", "text/csv", "a,b\n1,2".getBytes());
        SBApiResponse uploadResp = SBApiResponse.createDefaultResponse(Constants.API_CIOS_LOAD_EXCEL_CONTENT);
        uploadResp.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        uploadResp.getParams().setErrmsg("upload failed");

        when(storeFileToGCP.uploadCiosContentFile(any(MultipartFile.class), anyString(), anyString()))
                .thenReturn(uploadResp);
        when(cbServerProperties.getCiosCloudContainerName()).thenReturn("container");
        when(cbServerProperties.getCiosContentFileCloudFolderName()).thenReturn("folder");

        SBApiResponse resp = ciosContentService.loadContentFromExcel(file, "CORNELL", "partner");

        assertNotNull(resp);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
        assertEquals("Failed", resp.getParams().getStatus());
        assertTrue(resp.getParams().getErrmsg().toLowerCase().contains("failed to upload file"));
        verify(kafkaProducer, never()).push(anyString(), anyMap());
    }

    @Test
    void loadContentFromExcel_uploadThrowsException_returnsInternalServerError_noKafka() {
        MultipartFile file = new MockMultipartFile("file", "file.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "xls".getBytes());

        when(storeFileToGCP.uploadCiosContentFile(any(MultipartFile.class), anyString(), anyString()))
                .thenThrow(new RuntimeException("gcp error"));
        when(cbServerProperties.getCiosCloudContainerName()).thenReturn("container");
        when(cbServerProperties.getCiosContentFileCloudFolderName()).thenReturn("folder");

        SBApiResponse resp = ciosContentService.loadContentFromExcel(file, "CORNELL", "partner");

        assertNotNull(resp);
        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
        verify(kafkaProducer, never()).push(anyString(), anyMap());
    }

    @Test
    void fetchAllContentFromSecondaryDb_returnsPaginatedResponse_whenPageDataPresent() {
        RequestDto dto = mock(RequestDto.class);
        when(dto.getPage()).thenReturn(0);
        when(dto.getSize()).thenReturn(10);
        when(dto.getIsActive()).thenReturn(true);
        when(dto.getKeyword()).thenReturn("kw");

        CornellContentEntity entity = new CornellContentEntity();
        entity.setExternalId("1");
        List<CornellContentEntity> content = List.of(entity);
        Page<CornellContentEntity> page = new PageImpl<>(content, PageRequest.of(0, 10), 1);

        when(repository.findAllCiosDataAndIsActive(eq(true), any(Pageable.class), eq("kw"))).thenReturn(page);

        PaginatedResponse<?> resp = ciosContentService.fetchAllContentFromSecondaryDb(dto);

        assertNotNull(resp);
        assertEquals(1, resp.getResult().size());
        assertEquals(page.getTotalElements(), resp.getTotalElements());
        assertEquals(page.getTotalPages(), resp.getTotalPages());
        verify(repository, times(1)).findAllCiosDataAndIsActive(eq(true), any(Pageable.class), eq("kw"));
    }

    @Test
    void fetchAllContentFromSecondaryDb_returnsEmpty_whenRepositoryReturnsNull() {
        RequestDto dto = mock(RequestDto.class);
        when(dto.getPage()).thenReturn(1);
        when(dto.getSize()).thenReturn(5);
        when(dto.getIsActive()).thenReturn(false);
        when(dto.getKeyword()).thenReturn(null);

        when(repository.findAllCiosDataAndIsActive(eq(false), any(Pageable.class), isNull())).thenReturn(null);

        PaginatedResponse<?> resp = ciosContentService.fetchAllContentFromSecondaryDb(dto);

        assertNotNull(resp);
        assertTrue(resp.getResult().isEmpty());
        assertEquals(0, resp.getTotalElements());
        assertEquals(0, resp.getTotalPages());
        verify(repository, times(1)).findAllCiosDataAndIsActive(eq(false), any(Pageable.class), isNull());
    }

    @Test
    void fetchAllContentFromSecondaryDb_throwsCiosContentException_onDataAccessException() {
        RequestDto dto = mock(RequestDto.class);
        when(dto.getPage()).thenReturn(0);
        when(dto.getSize()).thenReturn(10);
        when(dto.getIsActive()).thenReturn(true);
        when(dto.getKeyword()).thenReturn("kw");

        when(repository.findAllCiosDataAndIsActive(eq(true), any(Pageable.class), eq("kw")))
                .thenThrow(new DataAccessException("db error") {});

        CiosContentException ex = assertThrows(CiosContentException.class, () -> ciosContentService.fetchAllContentFromSecondaryDb(dto));
        assertTrue(ex.getMessage().toLowerCase().contains("database access"));
        verify(repository, times(1)).findAllCiosDataAndIsActive(eq(true), any(Pageable.class), eq("kw"));
    }

    @Test
    void getAllFileInfos_returnsList_whenFound() {
        String partnerId = "pid";
        FileInfoEntity fie = new FileInfoEntity();
        when(fileInfoRepository.findByPartnerId(partnerId)).thenReturn(List.of(fie));

        List<FileInfoEntity> res = ciosContentService.getAllFileInfos(partnerId);

        assertNotNull(res);
        assertEquals(1, res.size());
        verify(fileInfoRepository, times(1)).findByPartnerId(partnerId);
    }

    @Test
    void getAllFileInfos_throwsCiosContentException_onDataAccessException() {
        when(fileInfoRepository.findByPartnerId(anyString())).thenThrow(new DataAccessException("db") {
        });

        CiosContentException ex = assertThrows(CiosContentException.class, () -> ciosContentService.getAllFileInfos("pid"));
        assertTrue(ex.getMessage().toLowerCase().contains("database access"));
    }

    @Test
    void deleteNotPublishContent_returnsBadRequest_whenValidationErrors() {
        DeleteContentRequestDto dto = new DeleteContentRequestDto();
        dto.setPartnerCode("P");
        dto.setExternalId(List.of("ext1"));

        CornellContentEntity entity = new CornellContentEntity();
        entity.setExternalId("ext1");
        entity.setIsActive(true);
        ObjectNode ciosData = JsonNodeFactory.instance.objectNode();
        ObjectNode content = JsonNodeFactory.instance.objectNode();
        content.put("status", Constants.DRAFT);
        ciosData.set("content", content);
        entity.setCiosData(ciosData);

        when(repository.findByExternalIdInAndPartnerCode(dto.getExternalId(), dto.getPartnerCode())).thenReturn(List.of(entity));
        when(repository.countByPartnerCode(dto.getPartnerCode())).thenReturn(1L);

        ObjectNode partnerInfo = JsonNodeFactory.instance.objectNode();
        ObjectNode dataNode = JsonNodeFactory.instance.objectNode();
        partnerInfo.set("data", dataNode);
        when(dataTransformUtility.fetchPartnerInfoUsingApi(dto.getPartnerCode())).thenReturn(partnerInfo);

        ResponseEntity<?> resp = ciosContentService.deleteNotPublishContent(dto);

        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        SBApiResponse body = (SBApiResponse) resp.getBody();
        assertNotNull(body);
        assertFalse(body.getParams().getErr().isEmpty());
    }

    @Test
    void readContentByExternalId_returnsCiosData_whenPresent() {
        CornellContentEntity entity = new CornellContentEntity();
        ObjectNode ciosData = JsonNodeFactory.instance.objectNode();
        ciosData.put("k", "v");
        entity.setCiosData(ciosData);
        when(repository.findByExternalIdAndPartnerCode("ext", "P")).thenReturn(Optional.of(entity));

        Object result = ciosContentService.readContentByExternalId("P", "ext");
        assertEquals(ciosData, result);
    }

    @Test
    void readContentByExternalId_throwsCiosContentException_whenMissing() {
        when(repository.findByExternalIdAndPartnerCode("ext", "P")).thenReturn(Optional.empty());
        assertThrows(CiosContentException.class, () -> ciosContentService.readContentByExternalId("P", "ext"));
    }

    @Test
    void searchContent_returnsSearchResult() {
        SearchResult sr = new SearchResult();
        when(esUtilService.searchDocuments(eq(Constants.CIOS_CONTENT_INDEX_NAME), any())).thenReturn(sr);

        SearchResult res = ciosContentService.searchContent(mock(com.igot.cios.util.elasticsearch.dto.SearchCriteria.class));
        assertSame(sr, res);
    }

    @Test
    void searchContent_throwsCiosContentException_onException() {
        when(esUtilService.searchDocuments(eq(Constants.CIOS_CONTENT_INDEX_NAME), any()))
                .thenThrow(new RuntimeException("es error"));

        assertThrows(CiosContentException.class, () -> ciosContentService.searchContent(mock(com.igot.cios.util.elasticsearch.dto.SearchCriteria.class)));
    }

    @Test
    void updateContent_createsOrUpdatesEntity_andUpdatesEsIndex() {
        ObjectNode root = JsonNodeFactory.instance.objectNode();
        ObjectNode content = root.putObject("content");
        ObjectNode cp = content.putObject("contentPartner");
        cp.put("partnerCode", "P");
        cp.put("id", "PID");
        content.put("externalId", "E1");
        content.put("isActive", true);
        content.put("name", "nm");

        when(repository.findByExternalIdAndPartnerId(("E1"), ("PID"))).thenReturn(Optional.empty());

        CornellContentEntity saved = new CornellContentEntity();
        saved.setExternalId("E1");
        saved.setPartnerCode("P");
        saved.setPartnerId("PID");
        saved.setCiosData(root);
        when(repository.save(any(CornellContentEntity.class))).thenReturn(saved);

        when(objectMapper.convertValue(any(), eq(Map.class))).thenReturn(new HashMap<>());
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("path");

        Object result = ciosContentService.updateContent(root);
        assertNotNull(result);
        verify(repository, times(1)).save(any(CornellContentEntity.class));
        verify(esUtilService, times(1)).updateDocument(eq(Constants.CIOS_CONTENT_INDEX_NAME), anyString(), anyMap(), eq("path"));
    }

    @Test
    void fetchAllContentFromSecondaryDb_throwsCiosContentException_onGenericException() {
        RequestDto dto = mock(RequestDto.class);
        when(dto.getPage()).thenReturn(0);
        when(dto.getSize()).thenReturn(10);
        when(dto.getIsActive()).thenReturn(true);
        when(dto.getKeyword()).thenReturn("kw");

        when(repository.findAllCiosDataAndIsActive(eq(true), any(Pageable.class), eq("kw")))
                .thenThrow(new RuntimeException("unexpected error"));

        CiosContentException ex = assertThrows(CiosContentException.class, () -> ciosContentService.fetchAllContentFromSecondaryDb(dto));
        assertTrue(ex.getMessage().toLowerCase().contains("unexpected"));
        verify(repository, times(1)).findAllCiosDataAndIsActive(eq(true), any(Pageable.class), eq("kw"));
    }

    @Test
    void loadContentProgressFromExcel_processesRows_andPushesToKafka_usingReflection() throws Exception {
        MultipartFile file = new MockMultipartFile("file", "progress.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "data".getBytes());
        List<Map<String, String>> processed = List.of(Collections.singletonMap("r", "row"));

        when(dataTransformUtility.processExcelFile(any(MultipartFile.class))).thenReturn(processed);

        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        arrayNode.addObject().put("r", "row");
        when(objectMapper.valueToTree(processed)).thenReturn(arrayNode);

        ObjectNode partnerInfo = JsonNodeFactory.instance.objectNode();
        partnerInfo.set("transformProgressJson", JsonNodeFactory.instance.arrayNode());
        partnerInfo.put("id", "partner-id");
        when(dataTransformUtility.fetchPartnerInfoUsingApi("CORNELL")).thenReturn(partnerInfo);

        ObjectNode transformed = JsonNodeFactory.instance.objectNode();
        transformed.put("some", "data");
        doReturn(transformed).when(dataTransformUtility).transformData(any(JsonNode.class), any());

        doNothing().when(payloadValidation).validatePayload(anyString(), any(JsonNode.class));

        // ensure the service has a non-null topic so kafkaProducer.push receives a real topic
        java.lang.reflect.Field topicField = CiosContentServiceImpl.class.getDeclaredField("topic");
        topicField.setAccessible(true);
        topicField.set(ciosContentService, "cios.progress");

        ciosContentService.loadContentProgressFromExcel(file, "CORNELL");

        verify(kafkaProducer, atLeastOnce()).push(("cios.progress"), (transformed));
    }

    @Test
    void callEnrollmentAPI_invokedViaReflection_throwsCiosContentException_whenTransformFails() throws Exception {
        ObjectNode raw = JsonNodeFactory.instance.objectNode();
        raw.put("r", "row");

        ObjectNode partnerInfo = JsonNodeFactory.instance.objectNode();
        partnerInfo.set("transformProgressJson", JsonNodeFactory.instance.arrayNode());
        partnerInfo.put("id", "partner-id");
        when(dataTransformUtility.fetchPartnerInfoUsingApi("CORNELL")).thenReturn(partnerInfo);

        doThrow(new RuntimeException("transform failure")).when(dataTransformUtility).transformData(any(JsonNode.class), any());

        Method m = CiosContentServiceImpl.class.getDeclaredMethod("callEnrollmentAPI", JsonNode.class, String.class);
        m.setAccessible(true);

        InvocationTargetException ite = assertThrows(InvocationTargetException.class, () -> m.invoke(ciosContentService, raw, "CORNELL"));
        assertInstanceOf(CiosContentException.class, ite.getCause());

        verify(kafkaProducer, never()).push(anyString(), any(JsonNode.class));
    }

    @Test
    void callEnrollmentAPI_success_pushesKafka_withTopic() throws Exception {
        ObjectNode raw = JsonNodeFactory.instance.objectNode();
        raw.put("r", "row");

        ObjectNode partnerInfo = JsonNodeFactory.instance.objectNode();
        partnerInfo.set("transformProgressJson", JsonNodeFactory.instance.arrayNode());
        partnerInfo.put("id", "partner-id");
        when(dataTransformUtility.fetchPartnerInfoUsingApi("CORNELL")).thenReturn(partnerInfo);

        ObjectNode transformed = JsonNodeFactory.instance.objectNode();
        transformed.put("some", "data");
        doReturn(transformed).when(dataTransformUtility).transformData(any(JsonNode.class), any());

        doNothing().when(payloadValidation).validatePayload(anyString(), any(JsonNode.class));

        // set topic
        java.lang.reflect.Field topicField = CiosContentServiceImpl.class.getDeclaredField("topic");
        topicField.setAccessible(true);
        topicField.set(ciosContentService, "cios.progress");

        Method m = CiosContentServiceImpl.class.getDeclaredMethod("callEnrollmentAPI", JsonNode.class, String.class);
        m.setAccessible(true);
        m.invoke(ciosContentService, raw, "CORNELL");

        verify(kafkaProducer, times(1)).push(("cios.progress"), (transformed));
    }

    @Test
    void callEnrollmentAPI_payloadValidationThrows_throwsCiosContentException_andNoKafka() throws Exception {
        ObjectNode raw = JsonNodeFactory.instance.objectNode();
        raw.put("r", "row");

        ObjectNode partnerInfo = JsonNodeFactory.instance.objectNode();
        partnerInfo.set("transformProgressJson", JsonNodeFactory.instance.arrayNode());
        partnerInfo.put("id", "partner-id");
        when(dataTransformUtility.fetchPartnerInfoUsingApi("CORNELL")).thenReturn(partnerInfo);

        ObjectNode transformed = JsonNodeFactory.instance.objectNode();
        transformed.put("some", "data");
        doReturn(transformed).when(dataTransformUtility).transformData(any(JsonNode.class), any());

        doThrow(new RuntimeException("validation failed")).when(payloadValidation).validatePayload(anyString(), any(JsonNode.class));

        Method m = CiosContentServiceImpl.class.getDeclaredMethod("callEnrollmentAPI", JsonNode.class, String.class);
        m.setAccessible(true);

        InvocationTargetException ite = assertThrows(InvocationTargetException.class, () -> m.invoke(ciosContentService, raw, "CORNELL"));
        assertInstanceOf(CiosContentException.class, ite.getCause());

        verify(kafkaProducer, never()).push(anyString(), any(JsonNode.class));
    }

    @Test
    void deleteNotPublishContent_handlesMissingCiosData_andReturnsBadRequest() {
        DeleteContentRequestDto dto = new DeleteContentRequestDto();
        dto.setPartnerCode("P");
        dto.setExternalId(List.of("ext1"));

        CornellContentEntity entity = new CornellContentEntity();
        entity.setExternalId("ext1");
        entity.setIsActive(false);
        entity.setCiosData(null);

        when(repository.findByExternalIdInAndPartnerCode(dto.getExternalId(), dto.getPartnerCode())).thenReturn(List.of(entity));
        when(repository.countByPartnerCode(dto.getPartnerCode())).thenReturn(1L);

        ObjectNode partnerInfo = JsonNodeFactory.instance.objectNode();
        ObjectNode dataNode = JsonNodeFactory.instance.objectNode();
        partnerInfo.set("data", dataNode);
        when(dataTransformUtility.fetchPartnerInfoUsingApi(dto.getPartnerCode())).thenReturn(partnerInfo);

        ResponseEntity<?> resp = ciosContentService.deleteNotPublishContent(dto);

        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        SBApiResponse body = (SBApiResponse) resp.getBody();
        assertNotNull(body);
        assertFalse(body.getParams().getErr().isEmpty());
        assertTrue(body.getParams().getErr().toLowerCase().contains("does not have a valid status"));
    }

    @Test
    void updateContent_updatePath_updatesExistingEntity_andUpdatesEsIndex() {
        ObjectNode root = JsonNodeFactory.instance.objectNode();
        ObjectNode content = root.putObject("content");
        ObjectNode cp = content.putObject("contentPartner");
        cp.put("partnerCode", "P");
        cp.put("id", "PID");
        content.put("externalId", "E1");
        content.put("isActive", true);
        content.put("name", "nm");

        CornellContentEntity existing = new CornellContentEntity();
        existing.setExternalId("E1");
        existing.setPartnerCode("P");
        existing.setPartnerId("PID");
        existing.setCiosData(root);
        when(repository.findByExternalIdAndPartnerId("E1", "PID")).thenReturn(Optional.of(existing));

        CornellContentEntity saved = new CornellContentEntity();
        saved.setExternalId("E1");
        saved.setPartnerCode("P");
        saved.setPartnerId("PID");
        saved.setCiosData(root);
        when(repository.save(any(CornellContentEntity.class))).thenReturn(saved);

        when(objectMapper.convertValue(any(), eq(Map.class))).thenReturn(new HashMap<>());
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("path");

        Object result = ciosContentService.updateContent(root);
        assertNotNull(result);
        verify(repository, times(1)).save(any(CornellContentEntity.class));
        verify(esUtilService, times(1)).updateDocument(eq(Constants.CIOS_CONTENT_INDEX_NAME), anyString(), anyMap(), eq("path"));
    }

    @Test
    void loadContentProgressFromExcel_processExcelThrows_throwsCiosContentException() {
        MultipartFile file = new MockMultipartFile("file", "progress.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "data".getBytes());
        doThrow(new RuntimeException("process error")).when(dataTransformUtility).processExcelFile(any(MultipartFile.class));

        CiosContentException ex = assertThrows(CiosContentException.class, () -> ciosContentService.loadContentProgressFromExcel(file, "CORNELL"));
        assertTrue(ex.getMessage().toLowerCase().contains("process error"));
        verify(kafkaProducer, never()).push(anyString(), any());
    }

    @Test
    void deleteNotPublishContent_deletesEntity_whenInactiveAndNotInitiated() {
        DeleteContentRequestDto dto = new DeleteContentRequestDto();
        dto.setPartnerCode("P");
        dto.setExternalId(List.of("ext1"));

        CornellContentEntity entity = new CornellContentEntity();
        entity.setExternalId("ext1");
        entity.setIsActive(false);
        ObjectNode ciosData = JsonNodeFactory.instance.objectNode();
        ObjectNode content = JsonNodeFactory.instance.objectNode();
        content.put("status", Constants.NOT_INITIATED);
        ciosData.set("content", content);
        entity.setCiosData(ciosData);

        when(repository.findByExternalIdInAndPartnerCode(dto.getExternalId(), dto.getPartnerCode())).thenReturn(List.of(entity));
        when(repository.countByPartnerCode(dto.getPartnerCode())).thenReturn(0L);

        ObjectNode partnerInfo = JsonNodeFactory.instance.objectNode();
        ObjectNode dataNode = JsonNodeFactory.instance.objectNode();
        partnerInfo.set("data", dataNode);
        when(dataTransformUtility.fetchPartnerInfoUsingApi(dto.getPartnerCode())).thenReturn(partnerInfo);

        ResponseEntity<?> resp = ciosContentService.deleteNotPublishContent(dto);

        assertEquals(HttpStatus.OK, resp.getStatusCode());
        SBApiResponse body = (SBApiResponse) resp.getBody();
        assertNotNull(body);
        assertEquals(Constants.SUCCESS, body.getResult().get(Constants.STATUS));
        verify(repository, times(1)).delete(entity);
        verify(esUtilService, times(1)).deleteDocument("P_ext1", Constants.CIOS_CONTENT_INDEX_NAME);
    }

    @Test
    void deleteNotPublishContent_handlesMissingOrNonObjectDataNode() {
        DeleteContentRequestDto dto = new DeleteContentRequestDto();
        dto.setPartnerCode("P");
        dto.setExternalId(List.of("ext1"));

        CornellContentEntity entity = new CornellContentEntity();
        entity.setExternalId("ext1");
        entity.setIsActive(false);
        ObjectNode ciosData = JsonNodeFactory.instance.objectNode();
        ObjectNode content = JsonNodeFactory.instance.objectNode();
        content.put("status", Constants.NOT_INITIATED);
        ciosData.set("content", content);
        entity.setCiosData(ciosData);

        when(repository.findByExternalIdInAndPartnerCode(dto.getExternalId(), dto.getPartnerCode())).thenReturn(List.of(entity));
        when(repository.countByPartnerCode(dto.getPartnerCode())).thenReturn(0L);

        ObjectNode partnerInfoMissing = JsonNodeFactory.instance.objectNode();
        when(dataTransformUtility.fetchPartnerInfoUsingApi(dto.getPartnerCode())).thenReturn(partnerInfoMissing);

        ResponseEntity<?> resp1 = ciosContentService.deleteNotPublishContent(dto);
        assertEquals(HttpStatus.OK, resp1.getStatusCode());

        ObjectNode partnerInfoNonObject = JsonNodeFactory.instance.objectNode();
        partnerInfoNonObject.put(Constants.DATA, "string");
        when(dataTransformUtility.fetchPartnerInfoUsingApi(dto.getPartnerCode())).thenReturn(partnerInfoNonObject);

        ResponseEntity<?> resp2 = ciosContentService.deleteNotPublishContent(dto);
        assertEquals(HttpStatus.OK, resp2.getStatusCode());
    }
}
