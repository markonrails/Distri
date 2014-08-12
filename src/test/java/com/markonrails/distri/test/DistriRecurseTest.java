package com.markonrails.distri.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ConcurrentSkipListSet;

import com.markonrails.distri.DistriControl;
import com.markonrails.distri.DistriHandle;
import com.markonrails.distri.DistriResult;
import com.markonrails.distri.DistriTask;

public class DistriRecurseTest {

	public static final String URLS_FILE = "/urls/patent_urls.txt";
	public static final String HANDLES_FILE = "/handles/planetlab_handles.txt";

	public static ConcurrentSkipListSet<DistriTask> originalTasks;
	
	public static long totalLatency = 0;
	public static int totalCount = 0;
	public static int remainCount = 0;

	public static boolean start = false;
	public static Object lock;

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
				try {
					handle.connect();
				} catch (Exception e) {
					e.printStackTrace();
				}

				handleHost = handlesReader.readLine();
				if (handleHost == null)
					break;
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
				if (url == null)
					break;
			}
			urlsReader.close();
			
			totalCount = urls.size();
			remainCount = totalCount;
			lock = new Object();
			System.out.println(String.format("Remain: %d", remainCount));
			
			final Date startTime = new Date();
			
			for (String theUrl : urls) {
				final DistriTask task = new DistriTask("", theUrl, 10, 1);
				task.setLinkSelector("a[href~=^\\/patents\\/[A-Z0-9]+$]");

				task.setFinishCallback(new DistriTask.FinishCallback() {

					public void execute(DistriResult result) {
						synchronized (lock) {
							remainCount += result.getQueryLinks().size();
						}
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
						totalCount++;

						synchronized (lock) {
							System.out.println(String.format("Remain: %d", remainCount));
							if (--remainCount == 0) {
								Date endTime = new Date();
								long diff = endTime.getTime()
										- startTime.getTime();
								double throughput = (double) totalCount
										/ (diff / 1000.);
								double latency = totalLatency
										/ (double) totalCount;
								System.out.println(String.format(
										"Finished %d tasks in %d ms",
										totalCount, diff));
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

			while (true)
				;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
