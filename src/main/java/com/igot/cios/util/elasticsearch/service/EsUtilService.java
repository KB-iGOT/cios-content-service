package com.igot.cios.util.elasticsearch.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cios.util.elasticsearch.dto.SearchCriteria;
import com.igot.cios.util.elasticsearch.dto.SearchResult;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
public interface EsUtilService {
    RestStatus addDocument(String esIndexName, String type, String id, Map<String, Object> document, String JsonFilePath);
    RestStatus updateDocument(String index, String indexType, String entityId, Map<String, Object> document, String JsonFilePath);
    void deleteDocument(String documentId, String esIndexName);
    void deleteDocumentsByCriteria(String esIndexName, SearchSourceBuilder sourceBuilder);
    SearchResult searchDocuments(String esIndexName, SearchCriteria searchCriteria) throws Exception;
    boolean isIndexPresent(String indexName);
    BulkResponse saveAll(String esIndexName, String type, List<JsonNode> entities) throws IOException;
}
