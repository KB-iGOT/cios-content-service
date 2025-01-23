package com.igot.cios.util.transactional.cassandrautils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PropertiesCache;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author Mahesh RV
 * @author Ruksana
 * <p>
 * Manages Cassandra connections and sessions.
 */
@Component
public class CassandraConnectionManagerImpl implements CassandraConnectionManager {
    private final Logger logger = LogManager.getLogger(getClass());
    private static final Map<String, CqlSession> cassandraSessionMap = new ConcurrentHashMap<>(2);
    private static CqlSession session;

    /**
     * Method invoked after bean creation for initialization
     */
    @PostConstruct
    private void initialize() {
        logger.info("Initializing CassandraConnectionManager...");
        registerShutdownHook();
        createCassandraConnection();
        initializeSessions();
        logger.info("CassandraConnectionManager initialized.");
    }

    /**
     * Retrieves a session for the specified keyspace.
     * If a session for the keyspace already exists, returns it; otherwise, creates a new session.
     *
     * @param keyspace The keyspace for which to retrieve the session.
     * @return The session object for the specified keyspace.
     */
    public CqlSession getSession(String keyspace) {
        return cassandraSessionMap.computeIfAbsent(keyspace, k ->
                CqlSession.builder()
                        .withKeyspace(keyspace)
                        .build());
    }

    /**
     * Creates a Cassandra connection based on properties
     */
    private void createCassandraConnection() {
        try {
            PropertiesCache cache = PropertiesCache.getInstance();
            String localDatacenter = cache.getProperty(Constants.LOCAL_DATACENTER);
            DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                    .withStringList(DefaultDriverOption.CONTACT_POINTS, Arrays.stream(StringUtils.split(cache.getProperty(Constants.CASSANDRA_CONFIG_HOST), ","))
                            .map(host -> host + ":9042").toList())
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, Integer.parseInt(cache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_LOCAL)))
                    .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, Integer.parseInt(cache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_REMOTE)))
                    .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, Integer.parseInt(cache.getProperty(Constants.MAX_REQUEST_PER_CONNECTION)))
                    .withInt(DefaultDriverOption.HEARTBEAT_INTERVAL, Integer.parseInt(cache.getProperty(Constants.HEARTBEAT_INTERVAL))).build();
            CqlSessionBuilder builder = CqlSession.builder().withConfigLoader(loader).withLocalDatacenter(localDatacenter);
            session = builder.build();
            logClusterDetails(session.getMetadata());
        } catch (Exception e) {
            logger.error("Error creating Cassandra connection", e);
            throw new CiosContentException("Internal Server Error", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Creates a Cluster object with specified hosts and pooling options
     *
     * @param hosts          - Cassandra host configuration
     * @param poolingOptions -   // Configure connection pooling options
     * @return - Cluster object with specified hosts and pooling options
     */
/*    private static Cluster createCluster(String[] hosts, PoolingOptions poolingOptions) {
        Cluster.Builder builder = Cluster.builder()
                .addContactPoints(hosts)
                .withProtocolVersion(ProtocolVersion.V3)
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withTimestampGenerator(new AtomicMonotonicTimestampGenerator())
                .withPoolingOptions(poolingOptions);

        ConsistencyLevel consistencyLevel = getConsistencyLevel();
        if (consistencyLevel != null) {
            builder.withQueryOptions(new QueryOptions().setConsistencyLevel(consistencyLevel));
        }

        return builder.build();
    }*/

    /**
     * Retrieves consistency level from properties
     *
     * @return -consistency level from properties
     */
    /*private static ConsistencyLevel getConsistencyLevel() {
        String consistency = PropertiesCache.getInstance().readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL);
        if (StringUtils.isBlank(consistency)) return null;

        try {
            return ConsistencyLevel.valueOf(consistency.toUpperCase());
        } catch (IllegalArgumentException exception) {
            LogManager.getLogger(CassandraConnectionManagerImpl.class)
                    .info("Exception occurred with error message = {}", exception.getMessage());
        }
        return null;
    }*/

    /**
     * Initializes sessions for predefined keyspaces
     */
    private void initializeSessions() {
        List<String> keyspacesList = Collections.singletonList(Constants.KEYSPACE_SUNBIRD);
        for (String keyspace : keyspacesList) {
            getSession(keyspace);
        }
    }

    /**
     * Registers a shutdown hook to clean-up resources
     */
    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::cleanupResources));
        logger.info("Cassandra shutdown hook registered.");
    }

    /**
     * Cleans up Cassandra resources during shutdown
     */
    private void cleanupResources() {
        logger.info("Starting resource cleanup for Cassandra...");
        cassandraSessionMap.values().forEach(CqlSession::close);
        if (session != null) {
            session.close();
        }
        logger.info("Resource cleanup for Cassandra completed.");
    }

    private void logClusterDetails(Metadata metadata) {
        String clusterName = String.valueOf(metadata.getClusterName());
        logger.info("Connected to cluster: {}", clusterName != null ? clusterName : "Unknown");
        metadata.getNodes().values().forEach(node ->
                logger.info("Datacenter: {}; Host: {}; Rack: {}",
                        node.getDatacenter(),
                        node.getEndPoint().resolve(),
                        node.getRack() != null ? node.getRack() : "Unknown"));
    }
}