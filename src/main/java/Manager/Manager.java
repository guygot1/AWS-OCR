package Manager;

import common.Msg;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

// ThreadPool
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import java.util.List;
import java.util.concurrent.TimeUnit;

import static common.AmazonServices.*;
import static common.Msg.*;

public class Manager   {
    // guy:
    public static String LOCAL_MANAGER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/586599331616/LocalManagerQueue.fifo";
    public static String MANAGER_WORKERS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/586599331616/ManagerWorkersQueue.fifo";


    public static void main(String[] args) throws InterruptedException {
        Ec2Client ec2 = Ec2Client.create(); //connect to EC2 service
        S3Client s3 = S3Client.create(); //connect to S3 service
        SqsClient sqs = SqsClient.create(); //connect to SQS service

        int numOfTasks = 0;
        String bucket = null;
        String key = null;
        String n = null;
        String applicationId = null;
        String outputFileName = null;
        String queueUrl = null;
        Task newtask = null;
        boolean terminate = false;
        String INSTANCE_ID = null;

        ExecutorService executor = Executors.newFixedThreadPool(10);

        while (!terminate) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(LOCAL_MANAGER_QUEUE_URL)
                    .maxNumberOfMessages(1)
                    .build();

            while (bucket == null & !terminate) {
                List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
                for (Message message : messages) {
                    Msg mess = deserialize(message.body());
                    if (mess != null) {
                        switch (mess.getTopic()) {
                            case "instance id":
                                INSTANCE_ID = mess.getBody();
                                break;
                            case "terminate":
                                terminate = true;
                                break;
                            case "new task":
                                applicationId = mess.getFrom().substring(5);
                                bucket = mess.getBody();
                                n = mess.getInfo1();
                                queueUrl = mess.getInfo2();
                                key = mess.getAttached();
                                newtask = new Task(bucket, key, applicationId, Integer.parseInt(n), numOfTasks, queueUrl);
                                break;
                        }
                        deleteMessage(LOCAL_MANAGER_QUEUE_URL, sqs, message);
                    }
                }
            }
            if(!terminate) {
                numOfTasks++;
                Runnable task = new ManagerThread(newtask, ec2, sqs, s3);
                executor.execute(task);
                bucket = null;
            }
        }
        executor.shutdown();
        try {
            while (!executor.awaitTermination(1000, TimeUnit.MINUTES)) ;

        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
        if (INSTANCE_ID != null) {
            ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(INSTANCE_ID).build());
            System.out.println("layla tov," + INSTANCE_ID);
        }
    }

}
