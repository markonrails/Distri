package com.markonrails.distri.test;

import java.io.File;

import com.markonrails.distri.DistriResult;
import com.markonrails.distri.test.utils.DistriTestFileHelper;

public class DistriResultTest {
	
	public static final String CURLEES_FOLDER = "/curlees";

	public static void main(String[] args) {
		File curleesFolder = new File(
				DistriResultTest.class.getResource(CURLEES_FOLDER).getPath());
		File[] curleesFiles = curleesFolder.listFiles();
		
		for (int i = 0; i < curleesFiles.length; i++) {
			if (!curleesFiles[i].isFile()) {
				continue;
			}
			
			try {
				System.out.println(String.format("--- Start of curlee: %s ---",
						curleesFiles[i].getName()));
				
				String curlee = DistriTestFileHelper.readFile(
						DistriResultTest.class.getResource(
								String.format("%s/%s", 
										CURLEES_FOLDER, 
										curleesFiles[i].getName())).getPath());
				DistriResult distriResult = new DistriResult(curlee);
				
				System.out.println(distriResult.getLocations().toString());
				System.out.println(distriResult.getFields().toString());
				System.out.println(distriResult.getParams().toString());
				System.out.println(distriResult.getCode());
				System.out.println(distriResult.getMessage());
				System.out.println(distriResult.getContent());
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				System.out.println(String.format("--- End of curlee: %s ---", 
						curleesFiles[i].getName()));
			}
		}
	}
	
}
