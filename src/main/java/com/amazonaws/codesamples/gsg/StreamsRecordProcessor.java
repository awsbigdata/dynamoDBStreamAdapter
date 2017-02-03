// Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under the Apache License, Version 2.0.
package com.amazonaws.codesamples.gsg;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import java.util.List;

public class StreamsRecordProcessor implements IRecordProcessor {

    private Integer checkpointCounter;


    public StreamsRecordProcessor() {

    }


    public void initialize(String shardId) {
        checkpointCounter = 0;
    }


    public void processRecords(List<Record> records,
                               IRecordProcessorCheckpointer checkpointer) {
        for(Record record : records) {
            String data = new String(record.getData().array());
            System.out.println(data);
            checkpointCounter += 1;
            if(checkpointCounter % 10 == 0) {
                try {
                    checkpointer.checkpoint();
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }


    public String getRecord(Record record){
        return new String(record.getData().array());
    }


    public void shutdown(IRecordProcessorCheckpointer checkpointer,
                         ShutdownReason reason) {
        if(reason == ShutdownReason.TERMINATE) {
            try {
                checkpointer.checkpoint();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}