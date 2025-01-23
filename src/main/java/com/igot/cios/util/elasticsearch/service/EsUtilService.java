package com.igot.cios.util.elasticsearch.service;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cios.util.elasticsearch.dto.SearchCriteria;
import com.igot.cios.util.elasticsearch.dto.SearchResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface EsUtilService {
    String addDocument(String esIndexName, String id, Map<String, Object> document, String jsonFilePath);

    String updateDocument(String index, String entityId, Map<String, Object> document, String jsonFilePath);

    void deleteDocument(String documentId, String esIndexName);

    void deleteDocumentsByCriteria(String esIndexName, Query query);

    SearchResult searchDocuments(String esIndexName, SearchCriteria searchCriteria);

    boolean isIndexPresent(String indexName);

    BulkResponse saveAll(String esIndexName, List<JsonNode> entities) throws IOException;
}
