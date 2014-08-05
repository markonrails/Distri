package com.markonrails.distri.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Date;

import com.markonrails.distri.DistriControl;
import com.markonrails.distri.DistriHandle;
import com.markonrails.distri.DistriResult;
import com.markonrails.distri.DistriTask;

public class DistriControlTest {

	public static final String URLS_FILE = "/urls/control_urls.txt";
	public static final String HANDLES_FILE = "/handles/planetlab_handles.txt";
	public static final String RESULTS_FOLDER = "/results";
	
	public static void main(String[] args) {
		try {
			DistriControl control = new DistriControl();
			
			FileInputStream handlesFile = new FileInputStream(
					DistriControlTest.class.getResource(HANDLES_FILE).getPath());
			BufferedReader handlesReader = new BufferedReader(
					new InputStreamReader(handlesFile));
			
			String handleHost = handlesReader.readLine();
			while (handleHost != null) {
				System.out.println(handleHost);
				control.CreateHandle(handleHost, "tamu_ecologyLab");
				
				DistriHandle handle = control.getHandleByHost(handleHost);
				handle.setKeyPath("/Users/Chen/.ssh/id_rsa");
				handle.setKnownHosts("/Users/Chen/.ssh/known_hosts");
				
				handleHost = handlesReader.readLine();
				if (handleHost == null) break;
			}
			
			handlesReader.close();
			
			FileInputStream urlsFile = new FileInputStream(
					DistriControlTest.class.getResource(URLS_FILE).getPath());
			BufferedReader urlsReader = new BufferedReader(
					new InputStreamReader(urlsFile));
			
			String url = urlsReader.readLine();
			for (int i = 1; url != null; i++) {
				final PrintWriter resultWriter = new PrintWriter(String.format("%s/%s%d.txt",
						DistriHandleTest.class.getResource(RESULTS_FOLDER).getPath(), "result", i));
				
				final DistriTask task = new DistriTask(url);
				task.setFinishCallback(new DistriTask.FinishCallback() {
					
					public void execute(DistriResult result) {
						resultWriter.println(result.getLocations().toString());
						resultWriter.println(result.getFields().toString());
						resultWriter.println(result.getParams().toString());
						resultWriter.println(result.getCode());
						resultWriter.println(result.getMessage());
						resultWriter.println(result.getContent());
					}
				});
				task.setFailCallback(new DistriTask.FailCallback() {
					
					public void execute(Exception e, String s) {
						e.printStackTrace();
						resultWriter.println(String.format("Curlee: %s", s));
					}
					
				});
				task.setTerminateCallback(new DistriTask.TerminateCallback() {
					
					public void execute() {
						resultWriter.println(String.format("Task init: %s", 
								task.getInitialTime().toString()));
						resultWriter.println(String.format("Task takes %d ms",
								task.timeSinceInitial()));
						resultWriter.println(String.format("Task done: %s", 
								(new Date()).toString()));
						resultWriter.close();
					}
					
				});
				
				control.addTask(task);
				
				url = urlsReader.readLine();
				if (url == null) break;
			}
			
			urlsReader.close();
			
			System.out.println("Going to sleep");
			Thread.sleep(60000);
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
