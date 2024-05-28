package org.example;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final int REQUESTS_PER_SECOND = 2000;
    private static final int CONNECTION_POOL_SIZE = 2000;
    private static final int DURATION_SECONDS = 60;
    private static final AtomicInteger SUCCESS_COUNTER = new AtomicInteger(0);
    private static final AtomicInteger FAILURE_COUNTER = new AtomicInteger(0);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    public static void main(String[] args) {
        String bucketName = "sqa-test";
        String keyNamePrefix = "test-file-key";
        String filePath = "test.js";

        String customEndpoint = "us-lax-3.linodeobjects.com";
        String accessKeyId = "";
        String secretAccessKey = "";

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setMaxConnections(CONNECTION_POOL_SIZE);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(customEndpoint, "us-lax-3"))
                .withClientConfiguration(clientConfig)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withPathStyleAccessEnabled(true)  // Enable path-style access if required by your custom endpoint
                .build();

        ExecutorService executorService = Executors.newFixedThreadPool(REQUESTS_PER_SECOND);

        long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(DURATION_SECONDS);

        while (System.currentTimeMillis() < endTime) {
            for (int i = 0; i < REQUESTS_PER_SECOND; i++) {
                final int requestId = i;
                executorService.submit(() -> {
                    String keyName = keyNamePrefix + requestId + "-" + System.currentTimeMillis();
                    try {
                        File file = new File(filePath);
                        PutObjectRequest request = new PutObjectRequest(bucketName, keyName, file);
                        PutObjectResult response = s3Client.putObject(request);
                        SUCCESS_COUNTER.addAndGet(1);
                       // System.out.println("Upload succeeded for " + keyName + ", ETag: " + response.getETag());
                    } catch (AmazonS3Exception e) {
                        System.err.println(LocalDateTime.now().format(formatter) + " - AmazonS3Exception: " + e.getErrorMessage());
                        System.err.println(LocalDateTime.now().format(formatter) + " - Status Code: " + e.getStatusCode());
                        System.err.println(LocalDateTime.now().format(formatter) + " - AWS Error Code: " + e.getErrorCode());
                        System.err.println(LocalDateTime.now().format(formatter) + " - Error Type: " + e.getErrorType());
                        System.err.println(LocalDateTime.now().format(formatter) + " - Request ID: " + e.getRequestId());
                        FAILURE_COUNTER.addAndGet(1);
                    } catch (AmazonServiceException e) {
                        System.err.println(LocalDateTime.now().format(formatter) + " - AmazonServiceException: " + e.getMessage());
                        FAILURE_COUNTER.addAndGet(1);
                    } catch (SdkClientException e) {
                        System.err.println(LocalDateTime.now().format(formatter) + " - SdkClientException: " + e.getMessage());
                        FAILURE_COUNTER.addAndGet(1);
                    } catch (Exception e) {
                        System.err.println(LocalDateTime.now().format(formatter) + " - Exception: " + e.getMessage());
                        FAILURE_COUNTER.addAndGet(1);
                    }
                });
            }

            try {
                Thread.sleep(1000);  // Wait for 1 second before submitting the next batch of requests
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                System.out.println(LocalDateTime.now().format(formatter) + " - SUCCESS_COUNTER: " + SUCCESS_COUNTER.get());
                System.out.println(LocalDateTime.now().format(formatter) + " - FAILURE_COUNTER: " + FAILURE_COUNTER.get());
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.out.println(LocalDateTime.now().format(formatter) + " - SUCCESS_COUNTER: " + SUCCESS_COUNTER.get());
            System.out.println(LocalDateTime.now().format(formatter) + " - FAILURE_COUNTER: " + FAILURE_COUNTER.get());
            executorService.shutdownNow();
        }
    }
}
