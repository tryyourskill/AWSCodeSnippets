package cloudComputing;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.parser.JSONParser;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQSManager {

	BasicAWSCredentials credentials = new BasicAWSCredentials("XXXXXXXXXXXXXXXXXXXXX",
			"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");

	AmazonSQS SQSClient = AmazonSQSClientBuilder.standard().withRegion(Regions.US_EAST_1)
			.withCredentials(new AWSStaticCredentialsProvider(credentials)).build(); 
	JSONParser jsonParser = new JSONParser();
	
	public boolean isQueueExist(String inputQueue){
		List<String> queueList = SQSClient.listQueues().getQueueUrls();
		boolean queueExist = false;
		for(String queue : queueList){
			String queueName = queue.substring(queue.lastIndexOf("/") + 1);
			queueExist = queueExist || queueName.equals(inputQueue);
		}
		return queueExist;
	}
	
	public void createQueue(String inputQueue){
		
		List<String> queueList = SQSClient.listQueues().getQueueUrls();
		boolean queueExist = false;
		for(String queue : queueList){
			String queueName = queue.substring(queue.lastIndexOf("/") + 1);
			queueExist = queueExist || queueName.equals(inputQueue);
		}
		if (!queueExist) {
			SQSClient.createQueue(new CreateQueueRequest().withQueueName(inputQueue));
		}
		
	}
	
	public boolean sendMessage(String inputQueue, String messageBody){
		//To check if a queue is exist or not. if queue doesn't exist then it will return false else it will send the message to the input queue.
		List<String> queueList = SQSClient.listQueues().getQueueUrls();
		boolean queueExist = false;
		for(String queue : queueList){
			String queueName = queue.substring(queue.lastIndexOf("/") + 1);
			queueExist = queueExist || queueName.equals(inputQueue);
		}
		if (queueExist) {
			GetQueueUrlResult queueURLResult = SQSClient.getQueueUrl(inputQueue);
			String queueURL = queueURLResult.getQueueUrl();
			SQSClient.sendMessage(new SendMessageRequest().withMessageBody(messageBody).
					withQueueUrl(queueURL));
			return true;
		} else {
			System.out.println("Queue doesn't exist !!");
			return false;
		}
		
	}
	
	public boolean sendMessageBatch(String inputQueue, ArrayList<String> batchMessage) {
		//To check if a queue is exist or not before sending a message request.
		if (isQueueExist(inputQueue)) {
			GetQueueUrlResult queueURLResult = SQSClient.getQueueUrl(inputQueue);
			String queueURL = queueURLResult.getQueueUrl();
			ArrayList<SendMessageBatchRequestEntry> MessageBatchRequest = new ArrayList<>();
			int count = 1;
			for (String message : batchMessage) {
				MessageBatchRequest.add(new SendMessageBatchRequestEntry("msg_" + count, message));
				count++;
			}
			SQSClient.sendMessageBatch(
					new SendMessageBatchRequest().withQueueUrl(queueURL).withEntries(MessageBatchRequest));
			return true;
		} else {
			System.out.println(inputQueue + " Queue doesn't exist !!");
			return false;
		}
	}
	
	public boolean readMessage(String inputQueue){
		
		GetQueueUrlResult queueURLResult = SQSClient.getQueueUrl(inputQueue);
		String queueURL = queueURLResult.getQueueUrl();
		
		SQSClient.receiveMessage(new ReceiveMessageRequest().withQueueUrl(queueURL));
		
		return false;
	}

}
