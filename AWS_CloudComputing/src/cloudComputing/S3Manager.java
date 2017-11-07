package cloudComputing;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;

public class S3Manager {
	
/*	 File configFile = new File(System.getProperty("user.home"), ".aws/credentials");
	    AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(
	        new ProfilesConfigFile(configFile), "default");
	    AmazonS3 s3client = AmazonS3ClientBuilder.standard().withCredentials(new ProfileCredentialsProvider()).withRegion(Regions.US_EAST_1).build();*/
	    
		BasicAWSCredentials credentials = new BasicAWSCredentials("XXXXXXXXXXXXXXXX", "XXXXXXXXXXXXXXXXXXXXXXX");
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).
				withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
	    
	public boolean isFileEmpty(String bucketName, String key){
		
        long fileContent = s3Client.getObjectMetadata(bucketName, key).getContentLength();
        if (fileContent == 0) {
			System.out.println("File is empty !!!!");
			return true;
		} else {
			System.out.println("File is non empty !!!!");
			return false;
		}
		
	}
	
	/*
	 * In S3 there is no directory and folder concept here every files are
	 * objects and directories are 0kb object suffixed with /, because of that
	 * even if you delete a directory the objects below this will still be exist
	 */
	public boolean isFileExist(String bucketName, String key) {
		
		boolean objectExist = s3Client.doesObjectExist(bucketName, key);

		return objectExist;
	}

	public void deleteObjectFromNVBucket(String bucketName, String key) {
		//For best practice you can check if the file is exist or not before you go to delete the file.
		s3Client.deleteObject(bucketName, key);
	}
	
	public boolean writeToS3File(String bucketName, String key, InputStream inputStream){
		
		ObjectMetadata objectMetaDta = new ObjectMetadata();
		try {
			byte[] inputByte = IOUtils.toByteArray(inputStream);
			objectMetaDta.setContentLength(inputByte.length);
			ByteArrayInputStream byteInputStream = new ByteArrayInputStream(inputByte);
			PutObjectRequest putRequest = new PutObjectRequest(bucketName, key, byteInputStream,objectMetaDta);
			s3Client.putObject(putRequest);
			
			inputStream.close();
			byteInputStream.close();
			return true;
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		return false;
	}
	
	public boolean copyS3Files(String srcBucket,String srcKey, String tgtBucket, String tgtKey){
		
		if(tgtKey == null || tgtKey.trim() == "/"){
			tgtKey = srcKey.trim().substring(srcKey.lastIndexOf("/")+1);
		}
		if(tgtKey.endsWith("/") && tgtKey.trim().length() > 1){
			tgtKey += srcKey.trim().substring(srcKey.lastIndexOf("/")+1);
		}
		//If you have a server side encryption in s3 bucket make sure to call the withDestinationSSECustomerKey otherwise it will throw an access denied error
		CopyObjectResult copyResult = s3Client.copyObject(new CopyObjectRequest(srcBucket, srcKey, tgtBucket, tgtKey));
		//Need to check the output of the copyResult
		System.out.println(copyResult);
		
		return false;
	}

	public boolean copyS3FilesWithCLI(String srcBucket,String srcKey, String tgtBucket, String tgtKey){
		
		String srcFilePath = "s3://" + srcBucket + "/" + srcKey;
		String tgtFilePath = "s3://" + tgtBucket + "/" + tgtKey;
		
		try {
			Process proc = Runtime.getRuntime().exec("aws s3 cp " + srcFilePath + " " + tgtFilePath);
			proc.waitFor();
			if(proc.exitValue() == 0){
				return true;
			} else {
				BufferedReader errorStream = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
				String line;
				while ((line = errorStream.readLine()) != null) {
					System.out.println(line);
				}
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	public boolean moveS3FileWithCLI(String srcBucket, String srcKey, String tgtBucket, String tgtKey){
		
		String srcFilePath = "s3://" + srcBucket + "/" + srcKey;
		String tgtFilePath = "s3://" + tgtBucket + "/" + tgtKey;
		
		try {
			Process proc = Runtime.getRuntime().exec("aws s3 mv " + srcFilePath + " " + tgtFilePath + "--recursive");
			proc.waitFor();
			if(proc.exitValue() == 0){
				return true;
			} else {
				BufferedReader errorStream = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
				String line;
				while ((line = errorStream.readLine()) == null) {
					System.out.println(line);
				}
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		
		return false;
}
	
	public boolean deleteAllObjectFromS3Directory(String bucketName, String key){
		
		ObjectListing listObject =  s3Client.listObjects(bucketName, key);
		for (S3ObjectSummary objectSummary : listObject.getObjectSummaries()) {
			String objectPath = objectSummary.getKey();
			//System.out.println(objectPath.hashCode() + "           " + key.hashCode());
			if(objectPath.endsWith("/") && objectPath.equals(key)){
				System.out.println("It's a directory");
			} else {
				s3Client.deleteObject(bucketName, objectPath);
				System.out.println(objectPath + " is deleted successfully from bucket " + bucketName);
			}
		}
		return false;
	}
	
	public void readConentS3Files(String bucketName, String key){
	
		S3Object fileObject = s3Client.getObject(bucketName, key);
		BufferedReader contentReader =  new BufferedReader(new InputStreamReader(fileObject.getObjectContent()));
		String line;
		try {
			while((line = contentReader.readLine()) != null){
				System.out.println(line);
			}
			contentReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public long getCountS3File(String bucketName, String key){
		
		S3Object fileObject = s3Client.getObject(bucketName, key);
		long recordCount = 0;
		BufferedReader contentReader =  new BufferedReader(new InputStreamReader(fileObject.getObjectContent()));
		try {
			while(contentReader.readLine() != null){
				recordCount++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return recordCount;
	}
	//When your S3 file is a hive table then you can get the count using running a hive query
	public long getCount(String tableName){
		long rowCount = 0;
		String line;
		String query = "select count(1) from " + tableName;
		
		try {
			Process proc = Runtime.getRuntime().exec((new String[]{"hive", "-e", query}));
			BufferedReader input = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			line = input.readLine();
			proc.waitFor();
			rowCount =  Long.parseLong(line);
			input.close();
		} catch (IOException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
		return rowCount;
	}

	//Create a method too generate a pre-signed url
}

