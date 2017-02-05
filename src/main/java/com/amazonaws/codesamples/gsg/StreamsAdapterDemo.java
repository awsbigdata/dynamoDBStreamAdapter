// Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under the Apache License, Version 2.0.
package com.amazonaws.codesamples.gsg;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

import java.util.UUID;

public class StreamsAdapterDemo {

    private static Worker worker;
    private static KinesisClientLibConfiguration workerConfig;
    private static IRecordProcessorFactory recordProcessorFactory;

    private static AmazonDynamoDBStreamsAdapterClient adapterClient;
    private static AWSCredentialsProvider streamsCredentials;

    private static AmazonDynamoDBClient dynamoDBClient;
    private static AWSCredentialsProvider dynamoDBCredentials;

    private static AmazonCloudWatchClient cloudWatchClient;

    private static String serviceName = "dynamodb";
    private static String dynamodbEndpoint = "https://dynamodb.us-east-1.amazonaws.com";
    private static String streamsEndpoint = "";
    private static String tablePrefix = "KCL-Demo";
    private static String streamArn;


    public StreamsAdapterDemo(){
        streamsCredentials = new ProfileCredentialsProvider();
        dynamoDBCredentials = new ProfileCredentialsProvider();
    }


    public StreamsAdapterDemo(String profilename){
        streamsCredentials = new ProfileCredentialsProvider(profilename);
        dynamoDBCredentials = new ProfileCredentialsProvider(profilename);
    }



    public StreamsAdapterDemo(String accesskey,String secretkey){
        streamsCredentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accesskey,secretkey));
        dynamoDBCredentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accesskey,secretkey));
    }



    public static void  main(String args[] ){


        if(args.length<2){
            System.out.println("Please enter the table name and end point");
            System.exit(1);
        }
        System.out.println("Starting DynamoStream...");

        String srcTable = args[0];
        dynamodbEndpoint = args[1];
        streamsCredentials = new ProfileCredentialsProvider();
        dynamoDBCredentials = new ProfileCredentialsProvider();

        recordProcessorFactory = new StreamsRecordProcessorFactory(dynamoDBCredentials, endpoint);
        streamsEndpoint = DynamoDBConnectorUtilities.getStreamsEndpoint(dynamodbEndpoint);
        //System.out.println(streamsEndpoint);
        //sourceRegion = DynamoDBConnectorUtilities.getRegionFromEndpoint(params.getSourceEndpoint());
        /* ===== REQUIRED =====
         * Users will have to explicitly instantiate and configure the adapter, then pass it to
         * the KCL worker.
         */
        adapterClient = new AmazonDynamoDBStreamsAdapterClient(streamsCredentials, new ClientConfiguration());
        adapterClient.setEndpoint(streamsEndpoint);

        dynamoDBClient = new AmazonDynamoDBClient(dynamoDBCredentials);
        dynamoDBClient.setEndpoint(dynamodbEndpoint);

        cloudWatchClient = new AmazonCloudWatchClient(dynamoDBCredentials, new ClientConfiguration());



        String taskName="dynamostream";
        // obtain the Stream ID associated with the source table
        String streamArn = dynamoDBClient.describeTable(srcTable).getTable().getLatestStreamArn();
        if (streamArn == null) {
            throw new IllegalArgumentException(DynamoDBConnectorConstants.MSG_NO_STREAMS_FOUND);
        }
        workerConfig = new KinesisClientLibConfiguration(taskName, streamArn,
                dynamoDBCredentials, DynamoDBConnectorConstants.WORKER_LABEL + taskName + UUID.randomUUID().toString())
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON) // worker will use checkpoint table
                // if
                // available, otherwise it is safer
                // to start
                // at beginning of the stream
                .withMaxRecords(DynamoDBConnectorConstants.STREAMS_RECORDS_LIMIT) // we want the maximum batch size to
                // avoid network transfer
                // latency overhead
                .withIdleTimeBetweenReadsInMillis(DynamoDBConnectorConstants.IDLE_TIME_BETWEEN_READS) // wait a
                // reasonable
                // amount of time
                .withValidateSequenceNumberBeforeCheckpointing(false) // Remove calls to GetShardIterator
                .withFailoverTimeMillis(DynamoDBConnectorConstants.KCL_FAILOVER_TIME); // avoid losing leases too often

        //  System.out.println("Creating worker for stream: " + streamArn);
        worker = new Worker(recordProcessorFactory, workerConfig, adapterClient, dynamoDBClient, cloudWatchClient);
        System.out.println("Starting worker...");
        worker.run();
    }



    public void jthonCall(StreamsRecordProcessorFactory streamsRecordProcessorFactory,String table,String dynamodbEndpoint){

        System.out.println("Starting DynamoStream...");

        String srcTable = table;

        streamsRecordProcessorFactory.setCredential(dynamoDBCredentials, dynamodbEndpoint);
        streamsEndpoint = DynamoDBConnectorUtilities.getStreamsEndpoint(dynamodbEndpoint);
        //System.out.println(streamsEndpoint);
        //sourceRegion = DynamoDBConnectorUtilities.getRegionFromEndpoint(params.getSourceEndpoint());
        /* ===== REQUIRED =====
         * Users will have to explicitly instantiate and configure the adapter, then pass it to
         * the KCL worker.
         */
        adapterClient = new AmazonDynamoDBStreamsAdapterClient(streamsCredentials, new ClientConfiguration());
        adapterClient.setEndpoint(streamsEndpoint);

        dynamoDBClient = new AmazonDynamoDBClient(dynamoDBCredentials);
        dynamoDBClient.setEndpoint(dynamodbEndpoint);

        cloudWatchClient = new AmazonCloudWatchClient(dynamoDBCredentials, new ClientConfiguration());



        String taskName="dynamostream";
        // obtain the Stream ID associated with the source table
        String streamArn = dynamoDBClient.describeTable(srcTable).getTable().getLatestStreamArn();
        if (streamArn == null) {
            throw new IllegalArgumentException(DynamoDBConnectorConstants.MSG_NO_STREAMS_FOUND);
        }
        workerConfig = new KinesisClientLibConfiguration(taskName, streamArn,
                dynamoDBCredentials, DynamoDBConnectorConstants.WORKER_LABEL + taskName + UUID.randomUUID().toString())
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON) // worker will use checkpoint table
                // if
                // available, otherwise it is safer
                // to start
                // at beginning of the stream
                .withMaxRecords(DynamoDBConnectorConstants.STREAMS_RECORDS_LIMIT) // we want the maximum batch size to
                // avoid network transfer
                // latency overhead
                .withIdleTimeBetweenReadsInMillis(DynamoDBConnectorConstants.IDLE_TIME_BETWEEN_READS) // wait a
                // reasonable
                // amount of time
                .withValidateSequenceNumberBeforeCheckpointing(false) // Remove calls to GetShardIterator
                .withFailoverTimeMillis(DynamoDBConnectorConstants.KCL_FAILOVER_TIME); // avoid losing leases too often

        //  System.out.println("Creating worker for stream: " + streamArn);
        worker = new Worker(streamsRecordProcessorFactory, workerConfig, adapterClient, dynamoDBClient, cloudWatchClient);
        System.out.println("Starting worker...");
        worker.run();
    }



}