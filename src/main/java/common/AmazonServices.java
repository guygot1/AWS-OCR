package common;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static common.Msg.*;


public class AmazonServices {

    public static String createEC2Instance(Ec2Client ec2, String name, String amiId,String script) {
        IamInstanceProfileSpecification role = IamInstanceProfileSpecification.builder()
                .arn("arn:aws:iam::586599331616:instance-profile/Guygotrole").build();

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .iamInstanceProfile(role)
                .userData(script)
                .keyName("ec2-ass1")
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value(name)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 Instance %s based on AMI %s",
                    instanceId, amiId);
            return instanceId;
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    public static String QueueSetup(SqsClient sqs, String queueName) {
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            throw e;

        }

        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        System.out.println("Queue url initiallized");

        return queueUrl;
    }

    public static String waitForAnswer(SqsClient sqs,String queueURL,String waitFor,int numofmessages) {
        String result = "";
        int counter = 0;
        while (counter < numofmessages) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueURL)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            for (Message message : messages) {
                Msg done = deserialize(message.body());
                if (done != null) {
                    if (done.getTopic().equals(waitFor)) {
                        counter++;
                        result = result.concat(done.getBody());
                        deleteMessage(queueURL, sqs, message);
                    }
                } else {
                    System.out.println("class Msg fail");
                }
            }
        }
        return result;
    }

    public static void bucketSetup(S3Client s3, String bucketName, Region region) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()

                                    .build())
                    .build());
            System.out.println("Creating bucket: " + bucketName);
            HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            System.out.println(bucketName + " is ready.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static LinkedList<String> getUrlsFromFile(File file){
        LinkedList<String> ll= new LinkedList<String>();

        String url;
        try {
            BufferedReader buffer = new BufferedReader(new FileReader(file));
            while ((url = buffer.readLine()) != null) {
                if (url.contains("http")) {
                    ll.add(url);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ll;

    }

    public static void bucketCleanUp(S3Client s3, String bucketName, String keyName) {
        System.out.println("Cleaning up...");
        try {
            System.out.println("Deleting object:: " + keyName);
            DeleteObjectRequest deleteObjectRequest =
                    DeleteObjectRequest.builder().bucket(bucketName).key(keyName).build();
            s3.deleteObject(deleteObjectRequest);
            System.out.println(keyName + " has been deleted.");
            System.out.println("Deleting bucket: " + bucketName);
            DeleteBucketRequest deleteBucketRequest =
                    DeleteBucketRequest.builder().bucket(bucketName).build();
            s3.deleteBucket(deleteBucketRequest);
            System.out.println(bucketName + " has been deleted.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        System.out.println("Cleanup complete");
        System.out.printf("%n");
    }

    public static void deleteMessage(String queueUrl,SqsClient sqs,Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteMessageRequest);
    }

    public static void cleanpUpSqs(String queueURL,SqsClient sqs) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueURL)
                .waitTimeSeconds(0)
                .maxNumberOfMessages(10)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        while (!messages.isEmpty()) {
            for (Message message : messages) {
                deleteMessage(queueURL, sqs, message);
            }
            messages = sqs.receiveMessage(receiveRequest).messages();
        }
    }

}
