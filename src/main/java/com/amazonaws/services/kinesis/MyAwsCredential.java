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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

public class MyAwsCredential implements AWSCredentialsProvider{
	
	String accessKey;
	String accessSecretKey;
	
	public MyAwsCredential(String accessKey,String accessSecretKey){
		this.accessKey = accessKey;
		this.accessSecretKey = accessSecretKey;
	}
	
	@Override
	public AWSCredentials getCredentials(){
		return new BasicAWSCredentials(this.accessKey,this.accessSecretKey);
	}
	
	@Override 
	public void refresh() {
		
	}

}
