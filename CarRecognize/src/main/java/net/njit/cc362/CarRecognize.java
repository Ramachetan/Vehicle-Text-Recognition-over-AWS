package net.njit.cc362;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.util.*;

public class CarRecognize {

    public static void main(String[] args) {

        String bucketName = "njit-cs-643";
        String queueName = "car.fifo";
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

            sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageGroupId(queueGroup).messageBody("-1")
                    .build());
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
            System.exit(1);
        }
    }
}
