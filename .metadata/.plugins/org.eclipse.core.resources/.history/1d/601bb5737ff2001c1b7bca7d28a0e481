// Author: Christian Carpena
// CS643-852 Programming Assignment 1
// This code is intended to run on EC2 A
// Used the GitHub repository for code references: https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/

package net.njit.cc362;

//Region Service = US_EAST_1
import software.amazon.awssdk.regions.Region;
//Client for Rekognition service
import software.amazon.awssdk.services.rekognition.RekognitionClient;
//Using Image,Label,DetectLabelsRequest,DetectLabelsResponse, S3Object Objects
import software.amazon.awssdk.services.rekognition.model.*;
//Client for S3 service
import software.amazon.awssdk.services.s3.S3Client;
//Using ListObjectsV2Request,ListObjectsV2Response Objects
import software.amazon.awssdk.services.s3.model.*;
//Client for SQS service
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
//Using CreateQueueRequest,GetQueueRequest,ListQueuesRequest,ListQueuesResponse,QueueNameExistsException,SendMessageRequest
import software.amazon.awssdk.services.sqs.model.*;
//List,Map
import java.util.*;

public class CarRecognize {

    public static void main(String[] args) {

        String bucketName = "njit-cs-643";
        String queueName = "car.fifo"; // -1 is the last on to get processed in the FIFO queue
        String queueGroup = "group1";

        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        RekognitionClient rek = RekognitionClient.builder()
                .region(Region.US_EAST_1)
                .build();
        SqsClient sqs = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();

        processBucketImages(s3, rek, sqs, bucketName, queueName, queueGroup);
    }

    public static void processBucketImages(S3Client s3, RekognitionClient rek, SqsClient sqs, String bucketName,
                                           String queueName, String queueGroup) {

        // Create queue or retrieve the queueUrl if it already exists.
        String queueUrl = "";
        try {
            ListQueuesRequest QueReq = ListQueuesRequest.builder()
                    .queueNamePrefix(queueName)
                    .build();
            ListQueuesResponse QueRes = sqs.listQueues(QueReq);

            if (QueRes.queueUrls().size() == 0) {
                CreateQueueRequest request = CreateQueueRequest.builder()
                        .attributesWithStrings(Map.of("FifoQueue", "true", "ContentBasedDeduplication", "true"))
                        .queueName(queueName)
                        .build();
                sqs.createQueue(request);

                GetQueueUrlRequest getURLQue = GetQueueUrlRequest.builder()
                        .queueName(queueName)
                        .build();
                queueUrl = sqs.getQueueUrl(getURLQue).queueUrl();
            } else {
                queueUrl = QueRes.queueUrls().get(0);
            }
        } catch (QueueNameExistsException e) {
            throw e;
        }

        // Process the 10 images in the S3 bucket
        try {
            ListObjectsV2Request listObjectsReqManual = ListObjectsV2Request.builder().bucket(bucketName).maxKeys(10)
                    .build();
            ListObjectsV2Response listObjResponse = s3.listObjectsV2(listObjectsReqManual);

            for (S3Object obj : listObjResponse.contents()) {
                System.out.println("Gathered image in njit-cs-643 S3 bucket: " + obj.key());

                Image img = Image.builder().s3Object(software.amazon.awssdk.services.rekognition.model.S3Object
                                .builder().bucket(bucketName).name(obj.key()).build())
                        .build();
                DetectLabelsRequest request = DetectLabelsRequest.builder().image(img).minConfidence((float) 90)
                        .build();
                DetectLabelsResponse result = rek.detectLabels(request);
                List<Label> labels = result.labels();

                for (Label label : labels) {
                    if (label.name().equals("Car")) {
                        sqs.sendMessage(SendMessageRequest.builder().messageGroupId(queueGroup).queueUrl(queueUrl)
                                .messageBody(obj.key()).build());
                        break;
                    }
                }
            }

            // Signal the end of image processing by sending "-1" to the queue
            sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageGroupId(queueGroup).messageBody("-1")
                    .build());
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
            System.exit(1);
        }
    }
}
