package Manager;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;

public class Task {
    // information from local application
    private String bucket;
    private String localApplicationId;
    private int n;
    private String key;

    private String queueUrl;
    private int numOfTasks;

    // information gathered from manager
    private int numberOfImages;
    private int numberOfImagesDone;
    private String[] workersId;

    public Task(String bucket, String key, String localApplicationId, int n, int numOfTasks, String queueUrl) {
        this.bucket = bucket;
        this.localApplicationId = localApplicationId;
        this.n = n;
        this.key = key;

        this.numOfTasks = numOfTasks;
        this.queueUrl = queueUrl;
        this.numberOfImages = 0;
        this.numberOfImagesDone = 0;
    }

    public boolean isReady() {
        return numberOfImagesDone == numberOfImages;
    }

    public void terminateWorkers(Ec2Client ec2){
        for(int i=0 ;i<workersId.length;i++){
            ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(workersId[i]).build());
        }


    }

    public String getLocalApplicationId() {
        return localApplicationId;
    }

    public int getN() {
        return n;
    }

    public String getKey() {
        return key;
    }


    public String getBucket() {
        return bucket;
    }

    public String getQueueUrl() {
        return queueUrl;
    }
}
