package com.markonrails.distri.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;

import com.markonrails.distri.DistriControl;
import com.markonrails.distri.DistriHandle;
import com.markonrails.distri.DistriResult;
import com.markonrails.distri.DistriTask;

public class DistriThroughPutTest {

	public static final String URLS_FILE = "/urls/2k_urls.txt";
	public static final String HANDLES_FILE = "/handles/planetlab_handles.txt";
	public static final String RESULTS_FOLDER = "/results";
	
	public static int taskCount;
	public static Object lock;

	public static int totalLatency = 0;
	
	public static void main(String[] args) {
		try {
			DistriControl control = new DistriControl();
			
			FileInputStream handlesFile = new FileInputStream(
					DistriControlTest.class.getResource(HANDLES_FILE).getPath());
			BufferedReader handlesReader = new BufferedReader(
					new InputStreamReader(handlesFile));
			
			String handleHost = handlesReader.readLine();
			while (handleHost != null) {
				control.createHandle(handleHost, "tamu_ecologyLab");
				
				DistriHandle handle = control.getHandleByHost(handleHost);
				handle.setKeyPath("/Users/Chen/.ssh/id_rsa");
				handle.setKnownHosts("/Users/Chen/.ssh/known_hosts");
				handle.connect();
				
				handleHost = handlesReader.readLine();
				if (handleHost == null) break;
			}
			
			handlesReader.close();
			
			FileInputStream urlsFile = new FileInputStream(
					DistriControlTest.class.getResource(URLS_FILE).getPath());
			BufferedReader urlsReader = new BufferedReader(
					new InputStreamReader(urlsFile));
			
			ArrayList<String> urls = new ArrayList<String>();
			String url = urlsReader.readLine();
			while (url != null) {
				urls.add(url);
				url = urlsReader.readLine();
				if (url == null) break;
			}
			urlsReader.close();
			
			final int total = urls.size();
			taskCount = urls.size();
			lock = new Object();
			
			final Date starttime = new Date();
			
			for (String theUrl : urls) {
				final DistriTask task = new DistriTask(theUrl, 1);
				task.setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36");
				
				task.setFinishCallback(new DistriTask.FinishCallback() {
					
					public void execute(DistriResult result) {
						// TODO Auto-generated method stub
						
					}
				});
				task.setFailCallback(new DistriTask.FailCallback() {
					
					public void execute(Exception e, String s) {
						e.printStackTrace();
					}
				});
				task.setTerminateCallback(new DistriTask.TerminateCallback() {
					
					public void execute() {
						totalLatency += task.timeSinceInitial();
						
						synchronized (lock) {
							if (--taskCount == 0) {
								Date endtime = new Date();
								long diff = endtime.getTime() - starttime.getTime();
								double throughput = (double)total / (diff/1000.);
								double latency = totalLatency / (double)total;
								System.out.println(String.format(
										"Finished %d tasks in %d ms", 
										total, diff));
								System.out.println(String.format(
										"Throughtput: %f /s", throughput));
								System.out.println(String.format(
										"Average latency: %f ms", latency));
								System.exit(0);
							}
						}
					}
				});
				
				control.addTask(task);
			}
			
			while (true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
