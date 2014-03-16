package com.amazonaws.services.kinesis.flume;

/*
 * Copyright 2012-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;


public class KinesisSink extends AbstractSink implements Configurable {
  
  private static final Log LOG = LogFactory.getLog(KinesisSink.class);
  private String accessKey;
  private String accessSecretKey;
  private String streamName;
  static AmazonKinesisClient kinesisClient;
  private String numberOfPartitions;
  private String kinesisEndpoint;
  
  @Override
  public void configure(Context context) {
	
	 this.accessKey = context.getString("accessKey", "defaultValue");
	 this.accessSecretKey = context.getString("accessSecretKey", "defaultValue");
	 this.numberOfPartitions = context.getString("kinesisPartitions", "1");
	 this.streamName = context.getString("streamName", "defaultValue");
	 LOG.info(streamName);
	 this.kinesisEndpoint = context.getString("kinesisEndpoint","https://kinesis.us-east-1.amazonaws.com");
	 if (streamName.equals("defaultValue") || accessKey.equals("defaultValue")|| this.accessSecretKey.equals("defaultValue") || this.streamName.equals("defaultValue")){
		 LOG.info("One Of The Required Config Param Is Missing: Accesseky,AccessSecretKey,StreamName");
		 throw new Error();
	 }
  }

  @Override
  public void start() {
	  
   kinesisClient = new AmazonKinesisClient(new BasicAWSCredentials(this.accessKey,this.accessSecretKey));
   kinesisClient.setEndpoint(kinesisEndpoint);
  
  }

  @Override
  public void stop () {
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = null;

    // Start transaction
    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    txn.begin();
    try {
   
      Event event = ch.take();
    
      int partitionKey=new Random().nextInt(( Integer.valueOf(numberOfPartitions)- 1) + 1) + 1;
      PutRecordRequest putRecordRequest = new PutRecordRequest();
	  putRecordRequest.setStreamName( this.streamName);
	  putRecordRequest.setData(ByteBuffer.wrap(event.getBody()));
	  putRecordRequest.setPartitionKey("partitionKey_"+partitionKey);
	  PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);	
	 
	  txn.commit();
      status = Status.READY;
    } catch (Throwable t) {
      txn.rollback();

      // Log exception, handle individual exceptions as needed

      status = Status.BACKOFF;

      // re-throw all Errors
      if (t instanceof Error) {
        throw (Error)t;
      }
    } finally {
      txn.close();
    }
    return status;
  }
}