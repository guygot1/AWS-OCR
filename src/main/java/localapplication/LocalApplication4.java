package localapplication;

import common.Msg;
import org.apache.commons.io.FileUtils;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.UUID;


import static common.Msg.*;
import static common.AmazonServices.*;



public class LocalApplication4 {

    // guy:
    public static String LOCAL_MANAGER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/586599331616/LocalManagerQueue.fifo";


    public static final String BUCKET_NAME = "bucket" + System.currentTimeMillis();

    // inputFileName  arg[0]
    // outputFileName arg[1]
    // n              arg[2]
    // terminate      arg[3]
    public static void main(String[] args) {
        Ec2Client ec2;
        Region region = Region.US_EAST_1; //set region
        ec2= Ec2Client.builder().credentialsProvider(ProfileCredentialsProvider.create())
                .region(region)

                .build();
        S3Client s3 = S3Client.create(); //connect to S3 service
        SqsClient sqs = SqsClient.create(); //connect to SQS service
        String instanceID = null;

//        cleanpUpSqs(LOCAL_MANAGER_QUEUE_URL, sqs);
//        cleanpUpSqs(MANAGER_WORKERS_QUEUE_URL, sqs);
//        cleanpUpSqs(WORKER_MANAGER_QUEUE_URL, sqs);
//        cleanpUpSqs(MANAGER_LOCAL_QUEUE_URL, sqs);

        if (!isManagerRunning(ec2)) {
            //ami-0ff8a91507f77f867
            //ami-084a0743b133cf124
            //ami-074284bd389da0e9c
            instanceID = createEC2Instance(ec2, "manager", "ami-074284bd389da0e9c"

                    ,
                    new String(Base64.getEncoder().encode(("#!/bin/bash \n" +
                            "export TESSDATA_PREFIX=/home/ubuntu \n"+
                            "wget https://ocrproject.s3.amazonaws.com/NewManager.jar\n " +
                            "java -jar NewManager.jar\n"
                    ).getBytes())));

            //send the instance message with its id
            Msg mess = new Msg("local","manager","instance id",instanceID,"","","","");
            SendMessageRequest send_msg_request = SendMessageRequest.builder()
                    .queueUrl(LOCAL_MANAGER_QUEUE_URL)
                    .messageBody(serialize(mess))
                    .messageGroupId("localmanager")
                    .build();
            sqs.sendMessage(send_msg_request);
        }

        String localApplicationId = UUID.randomUUID().toString();
        String key = "key";
        File imageFile = new File(args[0]);
        String outputFileName = args[1];
        String n = args[2];
        boolean terminate = false;

        if (args.length == 4 && args[3].equals("terminate")) {
            terminate = true;
        }

        //setting up a bucket
        bucketSetup(s3, BUCKET_NAME, region);

        //upload images to the bucket
        System.out.println("Uploading object...");
        s3.putObject(PutObjectRequest.builder().bucket(BUCKET_NAME).key(key)
                .build(), RequestBody.fromFile(imageFile));
        System.out.println("Uploaded" + imageFile.getName() + "successfully\n");

        //Setup all queues
        String queueUrl = QueueSetup(sqs, "manager_local_queue" + localApplicationId);

        //send message in the SQS
        Msg task = new Msg("local"+localApplicationId,"manager","new task",BUCKET_NAME,n,queueUrl,"",key);
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(LOCAL_MANAGER_QUEUE_URL)
                .messageBody(serialize(task))
                .messageGroupId("localmanager")
                .build();
        sqs.sendMessage(send_msg_request);


        if (terminate) {
            Msg ter = new Msg("local","manager","terminate","","","","","");
            SendMessageRequest send_termination = SendMessageRequest.builder()
                    .queueUrl(LOCAL_MANAGER_QUEUE_URL)
                    .messageBody(serialize(ter))//terminate yourself
                    .messageGroupId("localmanager")
                    .build();
            sqs.sendMessage(send_termination);
        }

        //local application waits for the workers to finish
        String loc = Answer(sqs, queueUrl, "done task");

        //download summery

        Path path = Paths.get("SummeryDownload" + localApplicationId);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("summery")
                .build();
        s3.getObject(getObjectRequest, path);

        try {
            File file = new File("SummeryDownload" + localApplicationId);
            String summery = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            createHTML(summery, outputFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        //bucketCleanUp(s3, BUCKET_NAME, key);
        sqs.close();
    }

    public static boolean isManagerRunning(Ec2Client ec2) {
        boolean res = false;
        List<Reservation> reservList = ec2.describeInstances().reservations();
        if (!reservList.isEmpty()) {
            whenFound:
            for (Reservation reservation : reservList) {
                List<Instance> instances = reservation.instances();
                for (Instance instance : instances) {

                    for (Tag tag : instance.tags()) {
                        if (tag.value().equals("manager") && instance.state().name().toString().equals("running")) {
                            res = true;
                            break whenFound;
                        } else {
                            res = false;
                        }


                    }
                }
            }
        }
        return res;
    }

    public static void createHTML(String summery,String name){
        File f = new File(name+".html");
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(f));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            bw.write("<!DOCTYPE html>\n" +
                    "<html lang=\"en\">\n" +
                    "<head>\n" +
                    "    <meta charset=\"UTF-8\">\n" +
                    "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                    "    <title>Document</title>\n" +
                    "</head>\n" +
                    "<body>\n" +
                    "    <h1>OCR-Summery</h1>\n"
            );

            bw.write(summery);

            bw.write("</body>\n" +
                    "</html>");
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String Answer(SqsClient sqs,String queueURL,String waitFor) {
        String loc = null;
        boolean receive = false;
        while (!receive) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueURL)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            if (!messages.isEmpty()) {
                for (Message message : messages) {
                    Msg done = deserialize(message.body());
                    if (done != null) {
                        if (done.getTopic().equals(waitFor)) {
                            receive = true;
                            loc = done.getBody(); //summery location in S3
                            deleteMessage(queueURL, sqs, message);
                        }
                    } else {
                        System.out.println("class Msg fail");
                    }
                }
            }
        }
        return loc;
    }
}