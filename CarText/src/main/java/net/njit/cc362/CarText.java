// Author: Christian Carpena
// CS643-852 Programming Assignment 1
// This code is intended to run on EC2 B
// Used example code from the GitHub repository: https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/

package net.njit.cc362;

//Region Service = US_EAST_1
import software.amazon.awssdk.regions.Region;
//Client for Rekognition service
import software.amazon.awssdk.services.rekognition.RekognitionClient;
//Using Image,DetectTextRequest,DetectTextResponse,S3Object,TextDetection,TextTypes Objects
import software.amazon.awssdk.services.rekognition.model.*;
//Client for S3 Bucket service
import software.amazon.awssdk.services.s3.S3Client;
//Client for SQS service
import software.amazon.awssdk.services.sqs.SqsClient;
//Using DeleteMessageRequest, GetQueueUrlRequest,ListQueuesRequest,ListQueuesResponse,Message,QueueNameExistsException,ReceiveMessageRequest
import software.amazon.awssdk.services.sqs.model.*;
//Using List,Map,HashMap,Iterator
import java.util.*;
//Using FileWriter,IOException
import java.io.*;

public class CarText {

    public static void main(String[] args) {

        String bucketName = "njit-cs-643";
        String queueName = "car.fifo"; // -1 is the last on to get processed in the FIFO queue

        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        RekognitionClient rek = RekognitionClient.builder()
                .region(Region.US_EAST_1)
                .build();
        SqsClient sqs = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();

        processCarImages(s3, rek, sqs, bucketName, queueName);
    }

    public static void processCarImages(S3Client s3, RekognitionClient rek, SqsClient sqs, String bucketName,
                                        String queueName) {

        // Poll SQS until the queue is created (by DetectCars)
        boolean QExists = false;
        while (!QExists) {
            ListQueuesRequest ReqQList = ListQueuesRequest.builder()
                    .queueNamePrefix(queueName)
                    .build();
            ListQueuesResponse ResQList = sqs.listQueues(ReqQList);
            if (ResQList.queueUrls().size() > 0)
                QExists = true;
        }

        // Retrieve the queueURL
        String queueUrl = "";
        try {
            GetQueueUrlRequest getReqQ = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            queueUrl = sqs.getQueueUrl(getReqQ)
                    .queueUrl();
        } catch (QueueNameExistsException e) {
            throw e;
        }

        // Process every car images
        try {
            boolean endOfQ = false;
            HashMap<String, String> outputs = new HashMap<String, String>();

            while (!endOfQ) {
                // Retrieve next image index
                ReceiveMessageRequest MsgReqRx = ReceiveMessageRequest.builder().queueUrl(queueUrl)
                        .maxNumberOfMessages(1).build();
                List<Message> messages = sqs.receiveMessage(MsgReqRx).messages();

                if (messages.size() > 0) {
                    Message message = messages.get(0);
                    String label = message.body();

                    if (label.equals("-1")) {
                        //When instance A terminates its image processing, it adds index -1 to the queue
                        // to signal to instance B that no more indexes will come.
                        endOfQ = true;
                    } else {
                        System.out.println("Processing car image with text from njit-cs-643 S3 bucket: " + label);

                        Image img = Image.builder().s3Object(S3Object.builder().bucket(bucketName).name(label).build())
                                .build();
                        DetectTextRequest request = DetectTextRequest.builder()
                                .image(img)
                                .build();
                        DetectTextResponse result = rek.detectText(request);
                        List<TextDetection> textDetections = result.textDetections();

                        if (textDetections.size() != 0) {
                            String text = "";
                            for (TextDetection textDetection : textDetections) {
                                if (textDetection.type().equals(TextTypes.WORD))
                                    text = text.concat(" " + textDetection.detectedText());
                            }
                            outputs.put(label, text);
                        }
                    }

                    // Delete the message in the queue now that it's been handled
                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queueUrl)
                            .receiptHandle(message.receiptHandle())
                            .build();
                    sqs.deleteMessage(deleteMessageRequest);
                }
            }
            try {
                FileWriter writer = new FileWriter("output.txt");

                Iterator<Map.Entry<String, String>> it = outputs.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, String> pair = it.next();
                    writer.write(pair.getKey() + ":" + pair.getValue() + "\n");
                    it.remove();
                }

                writer.close();
                System.out.println("Results written to file output.txt");
            } catch (IOException e) {
                System.out.println("An error occurred writing to file.");
                e.printStackTrace();
            }
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
            System.exit(1);
        }
    }
}
