package com.igot.cios.util;

/**
 * @author Mahesh RV
 */
public class Constants {

    public static final String KEYSPACE_SUNBIRD = "sunbird";
    public static final String TABLE_USER_EXTERNAL_ENROLMENTS = "user_external_enrolments";
    public static final String KEYSPACE_SUNBIRD_COURSES = "sunbird_courses";
    public static final String CORE_CONNECTIONS_PER_HOST_FOR_LOCAL = "coreConnectionsPerHostForLocal";
    public static final String CORE_CONNECTIONS_PER_HOST_FOR_REMOTE = "coreConnectionsPerHostForRemote";
    public static final String MAX_CONNECTIONS_PER_HOST_FOR_LOCAL = "maxConnectionsPerHostForLocal";
    public static final String MAX_CONNECTIONS_PER_HOST_FOR_REMOTE = "maxConnectionsPerHostForRemote";
    public static final String MAX_REQUEST_PER_CONNECTION = "maxRequestsPerConnection";
    public static final String HEARTBEAT_INTERVAL = "heartbeatIntervalSeconds";
    public static final String POOL_TIMEOUT = "poolTimeoutMillis";
    public static final String CASSANDRA_CONFIG_HOST = "cassandra.config.host";
    public static final String SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL = "LOCAL_QUORUM";
    public static final String EXCEPTION_MSG_FETCH = "Exception occurred while fetching record from ";
    public static final String INSERT_INTO = "INSERT INTO ";
    public static final String DOT = ".";
    public static final String OPEN_BRACE = "(";
    public static final String VALUES_WITH_BRACE = ") VALUES (";
    public static final String QUE_MARK = "?";
    public static final String COMMA = ",";
    public static final String CLOSING_BRACE = ");";
    public static final String INTEREST_ID = "interest_id";
    public static final String RESPONSE = "response";
    public static final String SUCCESS = "success";
    public static final String FAILED = "Failed";
    public static final String ERROR_MESSAGE = "errmsg";
    public static final String DATA_PAYLOAD_VALIDATION_FILE = "/PayloadValidation/ContentFileValidation.json";
    public static final String PROGRESS_DATA_VALIDATION_FILE = "/PayloadValidation/ProgressFileValidation.json";
    public static final String ERROR = "ERROR";
    public static final String REDIS_KEY_PREFIX = "cbpores_";
    public static final String CONTENT_PARTNER_REDIS_KEY_PREFIX = "contentpartner_";
    public static final String CONTENT_UPLOAD_SUCCESSFULLY= "success";
    public static final String CONTENT_UPLOAD_FAILED= "failed";
    public static final String SOURCE= "source";
    public static final String FETCH_RESULT_CONSTANT = ".fetchResult:";
    public static final String URI_CONSTANT = "URI: ";
    public static final String RESULT = "result";
    public static final String TOTAL_COURSES_COUNT = "totalCoursesCount";
    public static final String RESPONSE_CODE = "responseCode";
    public static final String OK = "OK";
    public static final String STATUS = "status";
    public static final String DATA = "data";
    public static final String UPDATED_ON = "updatedOn";
    public static final String CREATED_ON = "createdOn";
    public static final String NOT_INITIATED = "notInitiated";
    public static final String INDEX_TYPE = "_doc";
    public static final String KEYWORD = ".keyword";
    public static final String SEARCH_OPERATION_LESS_THAN = "<";
    public static final String SEARCH_OPERATION_GREATER_THAN = ">";
    public static final String SEARCH_OPERATION_LESS_THAN_EQUALS = "<=";
    public static final String SEARCH_OPERATION_GREATER_THAN_EQUALS = ">=";
    public static final String ASC = "asc";
    public static final String BOOL="bool";
    public static final String TERM="term";
    public static final String TERMS="terms";
    public static final String MATCH="match";
    public static final String RANGE="range";
    public static final String UNSUPPORTED_QUERY="Unsupported query type";
    public static final String UNSUPPORTED_RANGE= "Unsupported range condition";
    public static final String MUST= "must";
    public static final String FILTER= "filter";
    public static final String MUST_NOT="must_not";
    public static final String SHOULD= "should";
    public static final String ID = "id";
    public static final String CIOS_CONTENT_INDEX_NAME= "cios_onboarding_content_entity";
    public static final String REQUEST_PAYLOAD = "requestPayload";
    public static final String JWT_SECRET_KEY = "cios_search_result";
    public static final String API_VERSION_1 = "1.0";
    public static final String API_CB_PLAN_PUBLISH = "api.ciosIntegration.v1.deleteContent";
    public static final String MESSAGE = "message";
    public static final String UPDATED_DATE = "updatedDate";
    public static final String CREATED_DATE = "createdDate";
    public static final String ACTIVE = "isActive";
    public static final String PUBLISHED_ON = "publishedOn";
    public static final String ACTIVE_STATUS = "false";
    public static final String CONTENT_SEARCH_TAGS = "contentSearchTags";
    public static final String CONTENT = "content";
    public static final String FILE_ID = "fileId";
    public static final String PARTNER_CODE = "partnerCode";
    public static final String NAME = "name";
    public static final String EXTERNAL_ID = "externalId";
    public static final String CIOS_DATA = "ciosData";





    private Constants() {
    }

}
