package testClass;

import java.util.ArrayList;

import cloudComputing.SQSManager;

public class TestSQSManager {
	
	public static void main(String[] args) {
		SQSManager sm = new SQSManager();
		
		ArrayList<String> list = new ArrayList<>();
		list.add("Hello !!!");
		list.add("How are you !!");
		sm.sendMessageBatch("Dummy2", list);
	}

}
