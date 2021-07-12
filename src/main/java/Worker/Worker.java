package Worker;

import common.Msg;
import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;


import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import static common.AmazonServices.*;
import static common.Msg.*;


public class Worker {
    // guy:

     public static String MANAGER_WORKERS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/586599331616/ManagerWorkersQueue.fifo";




    public static void main(String[] args) {

        SqsClient sqs = SqsClient.create(); //connect to SQS service
        ITesseract tess = new Tesseract();

try {
    System.out.println("before the loop\n");
    ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
            .queueUrl(MANAGER_WORKERS_QUEUE_URL)
            .maxNumberOfMessages(1)
            .build();
    List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
    while (true) {
        for (Message message : messages) {
            Msg task = deserialize(message.body());
            if (task != null) {
           System.out.println(" url : "+task.getAttached()+"\n");
           System.out.println(" queueurl : "+task.getBody()+"\n");
           System.out.println(" topic : "+task.getTopic()+"\n");
                String url = task.getAttached();

                String queueUrl = task.getBody();

                deleteMessage(MANAGER_WORKERS_QUEUE_URL, sqs, message);
                System.out.println("delete message"+ message.body());
                Image temp = download_image_from_web(url);
                if(temp != null){
                    String importedImage = OCR(temp,tess);
                    System.out.println(" imported image : "+importedImage+"\n");
                    Msg done = new Msg("worker", "manager", "done ocr", "<img src='" + url + "'/> <br/>" + "<p>" + importedImage + "</p><br/>", "", "", "", "");
                    SendMessageRequest send_msg_request = SendMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .messageBody(serialize(done))
                            .build();
                    sqs.sendMessage(send_msg_request);
                    System.out.println(" message sent without error : "+done.getTopic()+"\n");
                }else {
                    Msg done = new Msg("worker", "manager", "done ocr", "<img src='" + url + "'/> <br/>" + "<p>" +"Error-Illegal Image" + "</p><br/>", "", "", "", "");
                    SendMessageRequest send_msg_request = SendMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .messageBody(serialize(done))
                            .build();
                    sqs.sendMessage(send_msg_request);
                    System.out.println(" message sent with error : "+done.getTopic()+"\n");
                }

            }
        }
        System.out.println("receiving  new message");
        messages = sqs.receiveMessage(receiveRequest).messages();
        System.out.println("received new message:"+messages.toString());
    }
}catch (Exception e){
    e.printStackTrace();
}
        System.out.println("ALL MESSAGE FINISHED");
        System.out.println("Stop Running");
    }

    public static String OCR(Image image,ITesseract tess ) {

        try{
            String str =  tess.doOCR((BufferedImage)image);
            System.out.println("Data from image is "+ str);
            return str;
        } catch ( TesseractException e){
            System.out.println("Exception " + e.getMessage());
            return  e.getMessage();
        }
    }



    private static Image download_image_from_web(String image_url) {
        Image image = null;
        try {
            URL url = new URL(image_url);
            image = ImageIO.read(url);
        } catch (IOException e) {
            System.out.println("Exception " + e.getMessage());
        }
        return image;
    }


}