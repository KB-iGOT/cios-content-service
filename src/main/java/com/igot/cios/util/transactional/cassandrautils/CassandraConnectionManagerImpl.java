package com.igot.cios.util.transactional.cassandrautils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.driver.internal.core.time.AtomicTimestampGenerator;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PropertiesCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


/**
 * @author Mahesh RV
 * @author Ruksana
 * <p>
 * Manages Cassandra connections and sessions.
 */
@Component
@Slf4j
public class CassandraConnectionManagerImpl implements CassandraConnectionManager {
    private final Logger logger = LogManager.getLogger(CassandraConnectionManagerImpl.class);
    private static final Map<String, CqlSession> cassandraSessionMap = new ConcurrentHashMap<>(2);
    private static CqlSession session;

    /**
     * Retrieves a session for the specified keyspace.
     * If a session for the keyspace already exists, returns it; otherwise, creates a new session.
     *
     * @param keyspace The keyspace for which to retrieve the session.
     * @return The session object for the specified keyspace.
     */
    public CqlSession getSession(String keyspace) {
        // Check if session for keyspace already exists
        CqlSession currentSession = cassandraSessionMap.get(keyspace);
        if (currentSession != null && !currentSession.isClosed()) {
            return currentSession;
        } else {
            CqlSession newSession = createCassandraConnectionWithKeySpaces(keyspace);
            cassandraSessionMap.put(keyspace, newSession);
            return newSession;
        }
    }

    public CassandraConnectionManagerImpl() {
        registerShutDownHook();
        createCassandraConnection();
    }

    /**
     * Creates a Cassandra connection based on properties
     */
    private void createCassandraConnection() {
        try {
            session = createCassandraConnectionWithKeySpaces(null);
        } catch (Exception e) {
            logger.error("Error while creating Cassandra connection", e);
            throw new CiosContentException(e.getMessage(), "Error while creating Cassandra connection", HttpStatus.INTERNAL_SERVER_ERROR);
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
    private static ConsistencyLevel getConsistencyLevel() {
        String consistency = PropertiesCache.getInstance().readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL);
        if (StringUtils.isBlank(consistency)) return null;

        try {
            return  DefaultConsistencyLevel.valueOf(consistency.toUpperCase());
        } catch (IllegalArgumentException exception) {
            LogManager.getLogger(CassandraConnectionManagerImpl.class)
                    .info("Exception occurred with error message = {}", exception.getMessage());
        }
        return null;
    }

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
    public void registerShutDownHook() {
        Runtime runtime = Runtime.getRuntime();
        runtime.addShutdownHook(new ResourceCleanUp());
        logger.info("Cassandra ShutDownHook registered.");
    }

    /**
     * Cleans up Cassandra resources during shutdown
     */
     static class ResourceCleanUp extends Thread {
        @Override
        public void run() {
            try {
                log.info("Started resource cleanup for Cassandra.");
                for (Map.Entry<String, CqlSession> entry : cassandraSessionMap.entrySet()) {
                    entry.getValue().close();
                }
                if (session != null) {
                    session.close();
                }
                log.info("Completed resource cleanup for Cassandra.");
            } catch (Exception ex) {
                log.error("Error during resource cleanup", ex);
            }
        }
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

    private CqlSession createCassandraConnectionWithKeySpaces(String keySpaceName) {
        try {
            // Load the properties required for connection
            PropertiesCache cache = PropertiesCache.getInstance();
            String cassandraHost = cache.getProperty(Constants.CASSANDRA_CONFIG_HOST);
            if (StringUtils.isBlank(cassandraHost)) {
                throw new CiosContentException("Cassandra host is not configured",
                        HttpStatus.INTERNAL_SERVER_ERROR
                        );
            }

            List<String> hosts = Arrays.asList(cassandraHost.split(","));
            List<InetSocketAddress> contactPoints = hosts.stream()
                    .map(host -> new InetSocketAddress(host.trim(), 9042))
                    .collect(Collectors.toList());

            List<String> contactPointsString = hosts.stream()
                    .map(host -> host.trim() + ":9042")
                    .collect(Collectors.toList());
            DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                    .withStringList(DefaultDriverOption.CONTACT_POINTS, contactPointsString)
                    .withString(DefaultDriverOption.REQUEST_CONSISTENCY, getConsistencyLevel().name())
                    .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE,
                            Integer.parseInt(cache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_LOCAL)))
                    .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE,
                            Integer.parseInt(cache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_REMOTE)))
                    .withInt(DefaultDriverOption.HEARTBEAT_INTERVAL,
                            Integer.parseInt(cache.getProperty(Constants.HEARTBEAT_INTERVAL)))
                    .withInt(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, 10000)
                    .withInt(DefaultDriverOption.REQUEST_TIMEOUT, 10000)
                    .withString(DefaultDriverOption.PROTOCOL_VERSION, ProtocolVersion.V4.toString())
                    .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, DefaultRetryPolicy.class)
                    .withClass(DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS, AtomicTimestampGenerator.class)
                    .build();

            CqlSession sessionWithKeyspaces;
            if (StringUtils.isNotBlank(keySpaceName)) {
                sessionWithKeyspaces = CqlSession.builder()
                        .addContactPoints(contactPoints)
                        .withLocalDatacenter("datacenter1")
                        .withKeyspace(keySpaceName)
                        .withConfigLoader(loader)
                        .build();
            } else {
                sessionWithKeyspaces = CqlSession.builder()
                        .addContactPoints(contactPoints)
                        .withLocalDatacenter("datacenter1")
                        .withConfigLoader(loader)
                        .build();
            }
            logger.info("Connected to the keyspaces: " + keySpaceName);
            // Get metadata and log cluster information
            final Metadata metadata = sessionWithKeyspaces.getMetadata();
            logger.info(String.format("Connected to cluster: %s", metadata.getClusterName()));

            // Log nodes in the cluster
            for (Node host : metadata.getNodes().values()) {
                logger.info(String.format("Datacenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getEndPoint(), host.getRack()));
            }
            return sessionWithKeyspaces;
        } catch (Exception e) {
            logger.error("Error while creating Cassandra connection", e);
            throw new CiosContentException(e.getMessage(), "Error while creating Cassandra connection", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}