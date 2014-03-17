package com.amazonaws.services.kinesis;
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

import org.apache.flume.channel.ChannelProcessor;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class RecordProcessorFactory implements IRecordProcessorFactory {
    

	
	ChannelProcessor chProcessor;
    public RecordProcessorFactory(ChannelProcessor channelProcessor) {
        super();
        this.chProcessor = channelProcessor;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new FlumeSourceRecordProcessor(chProcessor);
    }

}
