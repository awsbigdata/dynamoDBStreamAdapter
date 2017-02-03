package com.amazonaws.codesamples.gsg;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by srramas on 1/30/17.
 */
public class DynamoDBConnectorUtilities {

    /**
     * Logger for the {@link DynamoDBConnectorUtilities} class.
     */



    /**
     * Convert a given DynamoDB endpoint into its corresponding Streams endpoint
     *
     * @param endpoint
     *            given endpoint URL
     * @return the extracted Streams endpoint corresponding to the given DynamoDB endpoint
     */
    public static String getStreamsEndpoint(String endpoint) {
        String regex = DynamoDBConnectorConstants.PROTOCOL_REGEX;
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(endpoint);
        String ret;
        if (matcher.matches()) {
            ret = ((matcher.group(1) == null) ? "" : matcher.group(1)) + DynamoDBConnectorConstants.STREAMS_PREFIX
                    + matcher.group(2);
        } else {
            ret = DynamoDBConnectorConstants.STREAMS_PREFIX + endpoint;
        }
        return ret;
    }


}
