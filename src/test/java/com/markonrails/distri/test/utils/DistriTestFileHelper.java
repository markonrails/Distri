package com.markonrails.distri.test.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DistriTestFileHelper {

	public static String readFile(String fileName) throws IOException {
		byte[] bytes = Files.readAllBytes(Paths.get(fileName));
		return new String(bytes);
	}
	
}
