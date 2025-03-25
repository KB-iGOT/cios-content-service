package com.igot.cios.util.elasticsearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.util.Constants;
import com.igot.cios.util.elasticsearch.config.EsConfig;
import com.igot.cios.util.elasticsearch.dto.FacetDTO;
import com.igot.cios.util.elasticsearch.dto.SearchCriteria;
import com.igot.cios.util.elasticsearch.dto.SearchResult;
import com.networknt.schema.JsonSchemaFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

@Service
@Slf4j
public class EsUtilServiceImpl implements EsUtilService {
    private final ElasticsearchClient elasticsearchClient;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    public EsUtilServiceImpl(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public String addDocument(
            String esIndexName, String id, Map<String, Object> document, String JsonFilePath) {
        try {
            JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
            InputStream schemaStream = schemaFactory.getClass().getResourceAsStream(JsonFilePath);
            Map<String, Object> map = objectMapper.readValue(schemaStream,
                    new TypeReference<Map<String, Object>>() {
                    });
            Iterator<Entry<String, Object>> iterator = document.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, Object> entry = iterator.next();
                String key = entry.getKey();
                if (!map.containsKey(key)) {
                    iterator.remove();
                }
            }
            IndexRequest<Map<String,Object>> indexRequest = new IndexRequest.Builder<Map<String, Object>>()
                    .index(esIndexName)
                    .id(id)
                    .document(document)
                    .refresh(Refresh.True)
                    .build();
            IndexResponse response = elasticsearchClient.index(indexRequest);
            return "Successfully indexed document with id: " + response.result();
        } catch (Exception e) {
            log.error("Issue while Indexing to es: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public String updateDocument(
            String index, String entityId, Map<String, Object> updatedDocument, String JsonFilePath) {
        try {
            JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
            InputStream schemaStream = schemaFactory.getClass().getResourceAsStream(JsonFilePath);
            Map<String, Object> map = objectMapper.readValue(schemaStream,
                    new TypeReference<Map<String, Object>>() {
                    });
            Iterator<Entry<String, Object>> iterator = updatedDocument.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, Object> entry = iterator.next();
                String key = entry.getKey();
                if (!map.containsKey(key)) {
                    iterator.remove();
                }
            }
            IndexRequest<Map<String, Object>> indexRequest = new IndexRequest.Builder<Map<String, Object>>()
                    .index(index)
                    .id(entityId)
                    .document(updatedDocument)
                    .refresh(Refresh.True)
                    .build();
            IndexResponse response = elasticsearchClient.index(indexRequest);
            return response.result().jsonValue();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void deleteDocument(String documentId, String esIndexName) {
        try {
            DeleteRequest request = new DeleteRequest.Builder().index(esIndexName).id(documentId).build();
            DeleteResponse response = elasticsearchClient.delete(request);
            if (response.result().jsonValue().equalsIgnoreCase("DELETED")) {
                log.info("Document deleted successfully from elasticsearch.");
                RefreshRequest refreshRequest = new RefreshRequest.Builder().index(esIndexName).build();
                elasticsearchClient.indices().refresh(refreshRequest);
                log.info("Index refreshed to reflect the document deletion.");
            } else {
                log.error("Document not found or failed to delete from elasticsearch.");
            }
        } catch (Exception e) {
            log.error("Error occurred during deleting document in elasticsearch");
        }
    }

    @Override
    public SearchResult searchDocuments(String esIndexName, SearchCriteria searchCriteria) {
        SearchRequest.Builder searchRequestBuilder = buildSearchRequest(searchCriteria);
        assert searchRequestBuilder != null;
        searchRequestBuilder.index(esIndexName);
        try {
            if (searchCriteria != null) {
                int pageNumber = searchCriteria.getPageNumber();
                int pageSize = searchCriteria.getPageSize();
                int from = pageNumber * pageSize;
                searchRequestBuilder.from(from);
                if (pageSize > 0) {
                    searchRequestBuilder.size(pageSize);
                }
            }
            SearchRequest searchRequest = searchRequestBuilder.build();
            log.info("Final search query: {}", searchRequest.toString());
            SearchResponse<Object> paginatedSearchResponse =
                    elasticsearchClient.search(searchRequest, Object.class);
            List<Map<String, Object>> paginatedResult = extractPaginatedResult(paginatedSearchResponse);
            Map<String, List<FacetDTO>> fieldAggregations =
                    extractFacetData(paginatedSearchResponse, searchCriteria);
            SearchResult searchResult = new SearchResult();
            searchResult.setData(objectMapper.valueToTree(paginatedResult));
            searchResult.setFacets(fieldAggregations);
            searchResult.setTotalCount(paginatedSearchResponse.hits().total().value());
            return searchResult;
        } catch (IOException e) {
            log.error("Error while fetching details from elastic search");
            return null;
        }
    }

    private Map<String, List<FacetDTO>> extractFacetData(
            SearchResponse<Object> searchResponse, SearchCriteria searchCriteria) {
        Map<String, List<FacetDTO>> fieldAggregations = new HashMap<>();
        if (searchCriteria.getFacets() != null) {
            for (String field : searchCriteria.getFacets()) {
                Aggregate aggregate = searchResponse
                        .aggregations()
                        .get(field + "_agg");
                if (aggregate.isSterms()) {
                    List<FacetDTO> fieldValueList = new ArrayList<>();
                    for (StringTermsBucket bucket : aggregate.sterms().buckets().array()) {
                        if (!bucket.key().stringValue().isEmpty()) {
                            FacetDTO facetDTO = new FacetDTO(bucket.key().stringValue(), bucket.docCount());
                            fieldValueList.add(facetDTO);
                        }
                    }
                    fieldAggregations.put(field, fieldValueList);
                }
            }
        }
        return fieldAggregations;
    }

    private List<Map<String, Object>> extractPaginatedResult(SearchResponse<Object> paginatedSearchResponse) {
        List<Map<String, Object>> paginatedResult = new ArrayList<>();
        for (Hit<Object> hit : paginatedSearchResponse.hits().hits()) {
            paginatedResult.add((Map<String, Object>) hit.source());
        }
        return paginatedResult;
    }

    private SearchRequest.Builder buildSearchRequest(SearchCriteria searchCriteria) {
        log.info("Building search query");
        if (searchCriteria == null || searchCriteria.toString().isEmpty()) {
            log.error("Search criteria body is missing");
            return null;
        }
        BoolQuery.Builder boolQueryBuilder = buildFilterQuery(searchCriteria.getFilterCriteriaMap());
        SearchRequest.Builder searchSourceBuilder = new SearchRequest.Builder();
        searchSourceBuilder.query(boolQueryBuilder.build()._toQuery());
        addSortToSearchSourceBuilder(searchCriteria, searchSourceBuilder);
        addRequestedFieldsToSearchSourceBuilder(searchCriteria, searchSourceBuilder);
        addQueryStringToFilter(searchCriteria.getSearchString(), boolQueryBuilder);
        addFacetsToSearchSourceBuilder(searchCriteria.getFacets(), searchSourceBuilder);
        Query queryPart = buildQueryPart(searchCriteria.getQuery());
        boolQueryBuilder.must(queryPart);
        return searchSourceBuilder;
    }

    private BoolQuery.Builder buildFilterQuery(Map<String, Object> filterCriteriaMap) {
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        List<Query> mustNotQueries = new ArrayList<>();
        List<Query> boolQueries = new ArrayList<>();
        if (filterCriteriaMap != null) {
            filterCriteriaMap.forEach(
                    (field, value) -> {
                        if (field.equals("must_not") && value instanceof ArrayList) {
                            mustNotQueries.add(Query.of(q ->q.termsSet(t->t.field(field).terms((ArrayList<String>) value))));
                        } else if (value instanceof Boolean) {
                            boolQueries.add(Query.of(q ->q.term(t->t.field(field).value((boolean)value))));
                        } else if (value instanceof ArrayList) {
                            List<FieldValue> termsList = ((ArrayList<String>) value).stream()
                                    .map(FieldValue::of)
                                    .collect(Collectors.toList());
                            boolQueryBuilder.must(Query.of(q -> q.terms(t -> t.field(field + Constants.KEYWORD).terms(terms -> terms.value(termsList)))));
                        } else if (value instanceof String) {
                            boolQueryBuilder.must(Query.of(q -> q.terms(t ->
                                    t.field(field + Constants.KEYWORD)
                                            .terms(terms -> terms.value(List.of(FieldValue.of((String) value))))
                            )));
                        } else if (value instanceof Map) {
                            Map<String, Object> nestedMap = (Map<String, Object>) value;
                            if (isRangeQuery(nestedMap)) {
                                // Handle range query
                                BoolQuery.Builder rangeOrNullQuery = QueryBuilders.bool();
                                RangeQuery.Builder rangeQuery = QueryBuilders.range().field(field);
                                nestedMap.forEach((rangeOperator, rangeValue) -> {
                                    switch (rangeOperator) {
                                        case Constants.SEARCH_OPERATION_GREATER_THAN_EQUALS:
                                            rangeQuery.gte((JsonData) rangeValue);
                                            break;
                                        case Constants.SEARCH_OPERATION_LESS_THAN_EQUALS:
                                            rangeQuery.lte((JsonData) rangeValue);
                                            break;
                                        case Constants.SEARCH_OPERATION_GREATER_THAN:
                                            rangeQuery.gt((JsonData) rangeValue);
                                            break;
                                        case Constants.SEARCH_OPERATION_LESS_THAN:
                                            rangeQuery.lt((JsonData) rangeValue);
                                            break;
                                    }
                                });
                                rangeOrNullQuery.should(rangeQuery.build()._toQuery());
                                rangeOrNullQuery.should(Query.of(q -> q.bool(b -> b.mustNot(Query.of(qn -> qn.exists(e -> e.field(field)))))));
                                boolQueryBuilder.must(rangeOrNullQuery.build()._toQuery());
                            } else {
                                nestedMap.forEach((nestedField, nestedValue) -> {
                                    String fullPath = field + "." + nestedField;
                                    if (nestedValue instanceof Boolean) {
                                        boolQueryBuilder.must(Query.of(q -> q.term(t -> t.field(fullPath).value((Boolean) nestedValue))));
                                    } else if (nestedValue instanceof String) {
                                        List<FieldValue> termList = Collections.singletonList(FieldValue.of((String) nestedValue));
                                        boolQueryBuilder.must(Query.of(q -> q.terms(t -> t.field(fullPath + Constants.KEYWORD).terms((TermsQueryField) termList))));
                                    } else if (nestedValue instanceof ArrayList) {
                                        boolQueryBuilder.must(Query.of(q -> q.terms(t -> t.field(fullPath + Constants.KEYWORD).terms((TermsQueryField) nestedValue))));
                                    }
                                });
                            }
                        }
                    });
            mustNotQueries.forEach(mustNotQuery -> boolQueryBuilder.mustNot(mustNotQuery));
            boolQueries.forEach(boolQuery -> boolQueryBuilder.must(boolQuery));
        }
        return boolQueryBuilder;
    }

    private void addSortToSearchSourceBuilder(
            SearchCriteria searchCriteria, SearchRequest.Builder searchRequestBuilder) {
        if (isNotBlank(searchCriteria.getOrderBy()) && isNotBlank(searchCriteria.getOrderDirection())) {
            SortOrder sortOrder =
                    Constants.ASC.equals(searchCriteria.getOrderDirection()) ? SortOrder.Asc : SortOrder.Desc;
            searchRequestBuilder.sort(SortOptions.of(so -> so
                    .field(f -> f
                            .field(searchCriteria.getOrderBy() + Constants.KEYWORD)
                            .order(sortOrder)
                    )
            ));
        }
    }

    private void addRequestedFieldsToSearchSourceBuilder(
            SearchCriteria searchCriteria, SearchRequest.Builder searchRequestBuilder) {
        if (searchCriteria.getRequestedFields() == null) {
            // Get all fields in response
            searchRequestBuilder.source(SourceConfig.of(sc -> sc.fetch(true)));
        } else {
            if (searchCriteria.getRequestedFields().isEmpty()) {
                log.error("Please specify at least one field to include in the results.");
            }
            searchRequestBuilder.source(SourceConfig.of(sc -> sc.filter(filter -> filter.includes(searchCriteria.getRequestedFields()))));
        }
    }

    private void addQueryStringToFilter(String searchString, BoolQuery.Builder boolQueryBuilder) {
        if (isNotBlank(searchString)) {
            Query wildcardQuery = Query.of(q -> q.wildcard(
                    WildcardQuery.of(w -> w
                            .field("contentSearchTags.keyword")
                            .value("*" + searchString.toLowerCase() + "*"))
            ));
            boolQueryBuilder.must(wildcardQuery);
        }
    }

    private void addFacetsToSearchSourceBuilder(
            List<String> facets, SearchRequest.Builder searchRequestBuilder) {
        if (facets != null && !facets.isEmpty()) {
            Map<String, Aggregation> aggregationMap = facets.stream()
                    .collect(Collectors.toMap(
                            field -> field + "_agg",
                            field -> Aggregation.of(a -> a.terms(
                                    TermsAggregation.of(t -> t.field(field + ".keyword").size(250))))
                    ));
            searchRequestBuilder.aggregations(aggregationMap);
        }
    }

    private boolean isNotBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }

    @Override
    public void deleteDocumentsByCriteria(String esIndexName, Query Query) {
        try {
            HitsMetadata<Object> searchHits = executeSearch(esIndexName, Query);
            assert searchHits.total() != null;
            if (searchHits.total().value() > 0) {
                BulkResponse bulkResponse = deleteMatchingDocuments(esIndexName, searchHits);
                if (!bulkResponse.errors()) {
                    log.info("Documents matching the criteria deleted successfully from Elasticsearch.");
                } else {
                    log.error("Some documents failed to delete from Elasticsearch.");
                }
            } else {
                log.info("No documents match the criteria.");
            }
        } catch (Exception e) {
            log.error("Error occurred during deleting documents by criteria from Elasticsearch.", e);
        }
    }

    private HitsMetadata<Object> executeSearch(String esIndexName, Query query) throws IOException {
        SearchRequest searchRequest = new SearchRequest.Builder()
                .index(esIndexName)
                .query(query)
                .build();
        SearchResponse<Object> searchResponse =
                elasticsearchClient.search(searchRequest, Object.class);
        return searchResponse.hits();
    }

    private BulkResponse deleteMatchingDocuments(String esIndexName,  HitsMetadata<Object> searchHits)
            throws IOException {
        List<BulkOperation> operations = new ArrayList<>();
        for (Hit<Object> hit : searchHits.hits()) {
             new DeleteRequest.Builder()
                    .index(esIndexName)
                    .id(hit.id())
                    .build();
            operations.add(new BulkOperation.Builder().delete(d -> d.index(esIndexName).id(hit.id())).build());
        }
        BulkRequest bulkRequest = new BulkRequest.Builder().operations(operations).build();
        return elasticsearchClient.bulk(bulkRequest);
    }

    private boolean isRangeQuery(Map<String, Object> nestedMap) {
        return nestedMap.keySet().stream().anyMatch(key -> key.equals(Constants.SEARCH_OPERATION_GREATER_THAN_EQUALS) ||
                key.equals(Constants.SEARCH_OPERATION_LESS_THAN_EQUALS) || key.equals(Constants.SEARCH_OPERATION_GREATER_THAN) ||
                key.equals(Constants.SEARCH_OPERATION_LESS_THAN));
    }
    private Query buildQueryPart(Map<String, Object> queryMap) {
        log.info("Search:: buildQueryPart");
        if (queryMap == null || queryMap.isEmpty()) {
            return QueryBuilders.matchAll().build()._toQuery();
        }
        for (Entry<String, Object> entry : queryMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            switch (key) {
                case Constants.BOOL:
                    return buildBoolQuery((Map<String, Object>) value)._toQuery();
                case Constants.TERM:
                    return buildTermQuery((Map<String, Object>) value);
                case Constants.TERMS:
                    return buildTermsQuery((Map<String, Object>) value);
                case Constants.MATCH:
                    return buildMatchQuery((Map<String, Object>) value);
                case Constants.RANGE:
                    return buildRangeQuery((Map<String, Object>) value);
                default:
                    throw new IllegalArgumentException(Constants.UNSUPPORTED_QUERY + key);
            }
        }
        return null;
    }
    private BoolQuery buildBoolQuery(Map<String, Object> boolMap) {
        log.info("Search:: builderBoolQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        if (boolMap.containsKey(Constants.MUST)) {
            List<Map<String, Object>> mustList = (List<Map<String, Object>>) boolMap.get("must");
            mustList.forEach(must -> boolQueryBuilder.must(buildQueryPart(must)));
        }
        if (boolMap.containsKey(Constants.FILTER)) {
            List<Map<String, Object>> filterList = (List<Map<String, Object>>) boolMap.get("filter");
            filterList.forEach(filter -> boolQueryBuilder.filter(buildQueryPart(filter)));
        }
        if (boolMap.containsKey(Constants.MUST_NOT)) {
            List<Map<String, Object>> mustNotList = (List<Map<String, Object>>) boolMap.get("must_not");
            mustNotList.forEach(mustNot -> boolQueryBuilder.mustNot(buildQueryPart(mustNot)));
        }
        if (boolMap.containsKey(Constants.SHOULD)) {
            List<Map<String, Object>> shouldList = (List<Map<String, Object>>) boolMap.get("should");
            shouldList.forEach(should -> boolQueryBuilder.should(buildQueryPart(should)));
        }
        return boolQueryBuilder.build();
    }

    private Query buildTermQuery(Map<String, Object> termMap) {
        log.info("search::buildTermQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Entry<String, Object> entry : termMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders.term(t -> t.field(entry.getKey()).value((FieldValue) entry.getValue())));
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private Query buildTermsQuery(Map<String, Object> termsMap) {
        log.info("search:: buildTermsQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Entry<String, Object> entry : termsMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders.terms(t -> t.field(entry.getKey()).terms((TermsQueryField) entry.getValue())));
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private Query buildMatchQuery(Map<String, Object> matchMap) {
        log.info("search:: buildMatchQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Entry<String, Object> entry : matchMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders.match(m -> m.field(entry.getKey()).query((FieldValue) entry.getValue())));
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private Query buildRangeQuery(Map<String, Object> rangeMap) {
        log.info("search:: buildRangeQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Entry<String, Object> entry : rangeMap.entrySet()) {
            Map<String, Object> rangeConditions = (Map<String, Object>) entry.getValue();
            RangeQuery.Builder rangeQueryBuilder = new RangeQuery.Builder().field(entry.getKey());
            rangeConditions.forEach((condition, value) -> {
                switch (condition) {
                    case "gt":
                        rangeQueryBuilder.gt(JsonData.of(value));
                        break;
                    case "gte":
                        rangeQueryBuilder.gte(JsonData.of(value));
                        break;
                    case "lt":
                        rangeQueryBuilder.lt(JsonData.of(value));
                        break;
                    case "lte":
                        rangeQueryBuilder.lte(JsonData.of(value));
                        break;
                    default:
                        throw new IllegalArgumentException(Constants.UNSUPPORTED_RANGE + condition);
                }
            });
            boolQueryBuilder.must(rangeQueryBuilder.build()._toQuery());
        }
        return boolQueryBuilder.build()._toQuery();
    }

    @Override
    public boolean isIndexPresent(String indexName) {
        try {
            GetIndexRequest request = new GetIndexRequest.Builder().index(indexName).build();
            GetIndexResponse response = elasticsearchClient.indices().get(request);
            return response != null;
        } catch (IOException e) {
            log.error("Error checking if index exists", e);
            return false;
        }
    }

    @Override
    public BulkResponse saveAll(String esIndexName, List<JsonNode> entities) throws IOException {
        try {
            log.info("EsUtilServiceImpl :: saveAll");
            List<BulkOperation> operations = new ArrayList<>();
            entities.forEach(entity -> {
                String formattedId = entity.get(Constants.ID).asText();
                Map<String, Object> entityMap = objectMapper.convertValue(entity, Map.class);
                BulkOperation operation = BulkOperation.of(b -> b
                        .index(i -> i
                                .index(esIndexName)
                                .id(formattedId)
                                .document(entityMap)
                        )
                );
                operations.add(operation);
            });
            BulkRequest bulkRequest = BulkRequest.of(b -> b.operations(operations));
            return elasticsearchClient.bulk(bulkRequest);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new CiosContentException("error bulk uploading", e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private SearchRequest.Builder searchRequestBuild(SearchRequest original, String esIndexName) {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index(esIndexName);
        builder.query(original.query());
        if (original.aggregations() != null) {
            builder.aggregations(original.aggregations());
        }
        if (original.source() != null) {
            builder.source(original.source());
        }
        if (original.from() != null) {
            builder.from(original.from());
        }
        if (original.size() != null) {
            builder.size(original.size());
        }
        return builder;
    }
}
