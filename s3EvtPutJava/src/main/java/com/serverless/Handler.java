package com.serverless;

import java.io.InputStream;
import java.util.Collections;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class Handler implements RequestHandler<S3Event, ApiGatewayResponse> {

	@Override
	public ApiGatewayResponse handleRequest(S3Event s3event, Context context) {
		System.out.println("received s3 put event :  " + s3event.toJson());
		Response responseBody = new Response("Java Lambda - File uploaded to destination bucket successfully", null);
		
		// Download the image from S3 into a stream
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        S3EventNotificationRecord record = s3event.getRecords().get(0);
        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getKey()
                .replace('+', ' ');
        
        System.out.println("Bucket name and SrcKey " +srcBucket + ", & "  + srcKey );
        
        //Get the Object from Source bucket and move it to destination bucket.
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                srcBucket, srcKey));
        InputStream objectData = s3Object.getObjectContent();
        
        try {
        	String dest_bucket  = "ny-lambda-s3evt-dest-bucket2";
        	s3Client.putObject(dest_bucket, srcKey, objectData,s3Object.getObjectMetadata());
            System.out.println("Object uploaded to destination Bucket ");
            
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
        
		return ApiGatewayResponse.builder()
				.setStatusCode(200)
				.setObjectBody(responseBody)
				.setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda & serverless"))
				.build();
	}
}
