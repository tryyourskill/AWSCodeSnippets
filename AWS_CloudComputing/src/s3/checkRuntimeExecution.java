package s3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class checkRuntimeExecution {

	public static void main(String[] args) throws IOException, InterruptedException {
		System.out.println("Can you ");
		String cmd = "cmd.exe /c echo 2";
		String line;
		String line2 = "";
		//System.out.println(cmd);
		Process process = Runtime.getRuntime().exec(cmd);
		BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
		while((line=input.readLine()) != null){
			line2 = line;
		    System.out.println(line);
		}
		
		System.out.println(line2);
		
		File f = new File("G:\\Test_file.txt");
		
		BufferedReader fileInput = new BufferedReader(new InputStreamReader(new FileInputStream(f)));

		while((line = fileInput.readLine()) != null){
			System.out.println(line);
		}
		fileInput.close();
	}

}
