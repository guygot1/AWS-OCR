package Manager;

import java.util.Base64;
import common.Msg;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.LinkedList;

import static common.AmazonServices.*;
import static Manager.Manager.*;
import static common.Msg.*;

public class ManagerThread implements Runnable {
    private Task task;
    private S3Client s3;
    private SqsClient sqs;
    private Ec2Client ec2;

    public ManagerThread(Task task, Ec2Client ec2, SqsClient sqs, S3Client s3) {
         this.sqs = sqs;
         this.ec2 = ec2;
         this.s3 = s3;
         this.task = task;
    }

    public void run() {
//
//        Path imageUrlPath = Paths.get("ImageUrls" + task.getNumOfTasks());
//        Path summeryPath = Paths.get("Summery" + task.getNumOfTasks());
        Path imageUrlPath = Paths.get("ImageUrls" +new Date().getTime());
        Path summeryPath = Paths.get("Summery" + new Date().getTime());
                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(task.getBucket())
                .key(task.getKey())
                .build();
        s3.getObject(getObjectRequest, imageUrlPath);

        File Images = imageUrlPath.toFile();
        LinkedList<String> urls = getUrlsFromFile(Images);

        //setting up queue
        String queueUrl = QueueSetup(sqs, "manager_worker_queue" + task.getLocalApplicationId());

        int n = task.getN();

        for (String url : urls) {
            Msg task = new Msg("manager","worker","new ocr task",queueUrl,"","","",url);
            SendMessageRequest send_msg_request = SendMessageRequest.builder()
                    .queueUrl(MANAGER_WORKERS_QUEUE_URL)
                    .messageBody(serialize(task))
                    .messageGroupId("managerworkers")
                    .build();
            sqs.sendMessage(send_msg_request);
        }

        int numOfWorkers = (urls.size() / n);
        if ((urls.size() % n) != 0) {
            numOfWorkers++;
        }
        //ami-074284bd389da0e9c
        String[] workerInstancesId = new String[numOfWorkers];
        for (int i = 0; i < numOfWorkers; i++) {
            workerInstancesId[i] = createEC2Instance(ec2, "Worker" + i, "ami-074284bd389da0e9c",
                    new String(Base64.getEncoder().encode(("#!/bin/bash \n" +
                                    "export TESSDATA_PREFIX=/home/ubuntu \n"+
                                    "wget https://ocrproject.s3.amazonaws.com/NewWorker.jar\n " +
                                    "java -jar NewWorker.jar\n"
                    ).getBytes())));
        }




        String result = waitForAnswer(sqs, queueUrl, "done ocr", urls.size());
        try {
            Files.write(Paths.get(summeryPath.toString()), result.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }

        File summery = new File(summeryPath.toString());
        System.out.println("Uploading object...");
        s3.putObject(PutObjectRequest.builder().bucket(task.getBucket()).key("summery")
                .build(), RequestBody.fromFile(summery));
        System.out.println("Uploaded the summery successfully\n");
        System.out.print(result);
        //terminate all instances
        for (int i = 0; i < workerInstancesId.length; i++) {
            ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(workerInstancesId[i]).build());
            System.out.println("terminating" + workerInstancesId[i]);
        }


        Msg done = new Msg("manager","local","done task","","","","","");
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(task.getQueueUrl())
                .messageBody(serialize(done))//need to be modified;
                .build();
        sqs.sendMessage(send_msg_request);
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());

    }
}