package testClass;

import java.io.IOException;
import java.util.ArrayList;

import cloudComputing.DynamoDBDataAccess;
import cloudComputing.S3Manager;

public class TestS3Manager {

	public static void main(String[] args) throws IOException {
		
		S3Manager sm = new S3Manager();
		
		/*boolean status = sm.isFileEmpty("try-your-skill-log", "Employee.txt");
		
		System.out.println("Status : " + status);
		
		boolean fileEmpty = sm.isFileExist("try-your-skill-log", "Employee.txt");
		
		System.out.println("File Exist : " + fileEmpty);
*/		
		//sm.deleteObjectFromNVBucket("try-your-skill-log", "DummyFiles/");
		
		//sm.deleteObjectsFromBucket("testyourskill");
		
		/*File file = new File("C://Users//surajit//Google Drive//Hadoop_Spark//S3Files//Employee.txt");
		
		InputStream is = new FileInputStream(file);
		
		
		sm.writeToS3File("testyourskill", "Emp.txt", is);*/
		
		//
		//sm.copyS3Files("try-your-skill-log", "DummyFiles/", "testyourskill", "Dummy/");
		
		//System.out.println(sm.getCountS3File("try-your-skill-log", "DummyFiles/Employee.txt"));
		
		//sm.testMethod();
		
		DynamoDBDataAccess db = new DynamoDBDataAccess();
		//db.getFieldDynamoDB("Dummy", "ID", "1", "Name,Salary,Location");
		//db.createDynamoDBTable("DummyTest","EmpId","S");
		//db.getValueFromDynamoDBIindex("DummyTest", "DeptID-index", "DeptID", "5", "EName");
		//db.createIndexDynamoDB("DummyTest", "DeptID", null, null);
		//db.dropIndex("DummyTest", "DeptID-ColumnIndex");
		
		//db.getFieldDynamoDB("DummyTest","EmpId","2","tatus");
		//db.insertKeyValue("DummyTest", "EmpId", "2", "StatusLogged", "JobDone", "Failed");
		//ArrayList<String> listElement = new ArrayList<>();
		//listElement.add("Add this logg here and there !!!!");
		//db.addOrAppendtoList("DummyTest", "EmpId", "4", "LogsRec", listElement);
		
		//db.deleteItem("DummyTest", "EmpId", "2");
		//db.deleteTable("DummyTest");
		System.out.println(db.concatenateMultipleItem());
		
	}

}
