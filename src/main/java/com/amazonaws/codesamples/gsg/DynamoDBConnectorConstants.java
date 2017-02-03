package com.amazonaws.codesamples.gsg;

import com.amazonaws.services.dynamodbv2.model.StreamViewType;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by srramas on 1/30/17.
 */
public class DynamoDBConnectorConstants {

    /**
     * Logger for the {@link DynamoDBConnectorConstants} class.
     */
    //private static final Logger LOGGER = Logger.getLogger(DynamoDBConnectorConstants.class);

    /**
     * Command Line Messages
     */
    public static final String MSG_INVALID_PIPELINE = "Pipeline is not recognized.";
    public static final String MSG_NO_STREAMS_FOUND = "Source table does not have Streams enabled.";
    public static final String STREAM_NOT_READY = "Source table does not have Streams enabled, or does not have ViewType NEW_AND_OLD_IMAGES.";

    /**
     * Service-related status
     */
    public static final String ENABLED_STRING = "Enabled";
    public static final StreamViewType NEW_AND_OLD = StreamViewType.NEW_AND_OLD_IMAGES;

    /**
     * Prefixes, suffixes and limits
     */
    public static final String TASKNAME_DELIMITER = "_";
    public static final String SERVICE_PREFIX = "DynamoDBCrossRegionReplication";
    public static final String STREAMS_PREFIX = "streams.";
    public static final String PROTOCOL_REGEX = "^(https?://)?(.+)";
    public static final int DYNAMODB_TABLENAME_LIMIT = 255;

    /**
     * KCL constants
     */
    public static final int IDLE_TIME_BETWEEN_READS = 500;
    public static final int STREAMS_RECORDS_LIMIT = 1000;
    public static final int KCL_RECORD_BUFFER_SIZE = 10 * STREAMS_RECORDS_LIMIT;
    public static final int KCL_FAILOVER_TIME = 60000;
    public static final String WORKER_LABEL = "worker";

    /**
     * MD5 digest instance
     */
    private static final String HASH_ALGORITHM = "MD5";
    public static final String BYTE_ENCODING = "UTF-8";
    public static final MessageDigest MD5_DIGEST = getMessageDigestInstance(HASH_ALGORITHM);

    private static MessageDigest getMessageDigestInstance(String hashAlgorithm) {
        try {
            return MessageDigest.getInstance(hashAlgorithm);
        } catch (NoSuchAlgorithmException e) {
           // LOGGER.error("Specified hash algorithm does not exist: " + hashAlgorithm + e);
            return null;
        }
    }
}
