package com.amazonaws.codesamples.gsg;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;


/**
 * Created by srramas on 1/30/17.
 */


public class StreamsRecordProcessorFactory implements
        IRecordProcessorFactory {

    private  AWSCredentialsProvider dynamoDBCredentials=null;
    private  String dynamoDBEndpoint=null;
    IRecordProcessor iRecordProcessor= new StreamsRecordProcessor();

    public StreamsRecordProcessorFactory(
            AWSCredentialsProvider dynamoDBCredentials,
            String dynamoDBEndpoint) {
        this.dynamoDBCredentials = dynamoDBCredentials;
        this.dynamoDBEndpoint = dynamoDBEndpoint;
    }
    public StreamsRecordProcessorFactory(){

    }

    public void setCredential(
            AWSCredentialsProvider dynamoDBCredentials,
            String dynamoDBEndpoint){
        this.dynamoDBCredentials = dynamoDBCredentials;
        this.dynamoDBEndpoint = dynamoDBEndpoint;
    }

    public void setProcessor(StreamsRecordProcessor iRecordProcessor){
        this.iRecordProcessor=iRecordProcessor;
    }

    public IRecordProcessor createProcessor() {
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(dynamoDBCredentials, new ClientConfiguration());
        dynamoDBClient.setEndpoint(dynamoDBEndpoint);
        return iRecordProcessor;
    }

}
