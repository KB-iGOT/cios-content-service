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
    public static final String CORNELL_DATA_PAYLOAD_VALIDATION_FILE = "/PayloadValidation/CornellFileValidation.json";
    public static final String CORNELL_PROGRESS_DATA_VALIDATION_FILE = "/PayloadValidation/CornellProgressFileValidation.json";
    public static final String ERROR = "ERROR";
    public static final String REDIS_KEY_PREFIX = "cbpores_";
    public static final String CONTENT_PARTNER_REDIS_KEY_PREFIX = "contentpartner_";
    public static final String CONTENT_UPLOAD_SUCCESSFULLY= "success";
    public static final String CONTENT_UPLOAD_FAILED= "failed";
    public static final String SOURCE= "source";
    public static final String FETCH_RESULT_CONSTANT = ".fetchResult:";
    public static final String URI_CONSTANT = "URI: ";
    public static final String RESULT = "result";
    public static final String TOTAL_COURSE_COUNT = "totalCourseCount";
    public static final String RESPONSE_CODE = "responseCode";
    public static final String OK = "OK";
    public static final String STATUS = "status";
    public static final String DATA = "data";
    public static final String UPDATED_ON = "updatedOn";
    public static final String CREATED_ON = "createdOn";
    public static final String CONTENT_UPLOAD_LAST_UPDATED_DATE = "contentUploadLastUpdatedDate";
    public static final String CONTENT_PROGRESS_LAST_UPDATED_DATE = "contentProgressLastUpdatedDate";

    private Constants() {
    }

}
