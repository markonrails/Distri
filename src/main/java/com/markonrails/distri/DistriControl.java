package com.markonrails.distri;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.collections.map.LRUMap;

public class DistriControl {
	
	public static final int DEFAULT_MIN_DELAY = 5;
	
	private HashMap<String, Integer> hostsMinDelays;

	private ArrayList<DistriHandle> handles;
	private HashMap<String, DistriHandle> handlesByHost;
	private PriorityBlockingQueue<DistriHandle> handlesQueue;
	
	private PriorityBlockingQueue<DistriTask> tasksQueue;
	
	private Object queuesLock;
	private boolean queuesChanged;
	
	private Map<String, DistriResult> cache;
	
	public DistriControl() {
		hostsMinDelays = new HashMap<String, Integer>();
		
		handles = new ArrayList<DistriHandle>();
		handlesByHost = new HashMap<String, DistriHandle>();
		HandleAvailTaskCountComp handleComp = new HandleAvailTaskCountComp();
		handlesQueue = new PriorityBlockingQueue<DistriHandle>(10, handleComp);
		
		TaskExhaustionComp taskComp = new TaskExhaustionComp();
		tasksQueue = new PriorityBlockingQueue<DistriTask>(10, taskComp);
		
		queuesLock = new Object();
		queuesChanged = false;
		
		Thread controlTasksThread = new Thread() {
			public void run() {
				while (true) {
					synchronized (queuesLock) {
						try {
							if (!queuesChanged) {
								queuesLock.wait(1000);
							} 
							controlTasks();
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							queuesChanged = false;
						}
					}
				}
			}
		};
		controlTasksThread.start();
	}
	
	public DistriHandle createHandle(String host, String user) {
		return createHandle(DistriHandle.DEFAULT_MAX_TASK_COUNT,
				host, DistriHandle.DEFAULT_PORT, user);
	}
	
	public DistriHandle createHandle(String host, int port, String user) {
		return createHandle(DistriHandle.DEFAULT_MAX_TASK_COUNT, host, port, user);
	}
	
	public DistriHandle createHandle(int maxTaskCount, String host, String user) {
		return createHandle(maxTaskCount, host, DistriHandle.DEFAULT_PORT, user);
	}
	
	public DistriHandle createHandle(int maxTaskCount, 
			String host, int port, String user) {
		if (handlesByHost.containsKey(host)) {
			return getHandleByHost(host);
		}
		DistriHandle handle = new DistriHandle(this, maxTaskCount, host, port, user);
		handles.add(handle);
		handlesByHost.put(host, handle);
		handlesQueue.add(handle);
		return handle;
	}
	
	public synchronized void controlTasks() throws Exception {
		DistriTask task = tasksQueue.peek();
		while (task != null && task.isTerminated()) {
			tasksQueue.poll();
			task = tasksQueue.peek();
		}
		if (task == null) return;
		
		ArrayList<DistriHandle> handlesPolled = new ArrayList<DistriHandle>();
		DistriHandle handle = handlesQueue.peek();
		while (handle != null && !handle.canExecTask(task)) {
			handlesQueue.poll();
			handlesPolled.add(handle);
			handle = handlesQueue.peek();
		}
		for (DistriHandle handlePolled : handlesPolled) {
			handlesQueue.add(handlePolled);
		}
		if (handle == null || handle.getAvailTaskCount() <= 0) return;
		
		handlesQueue.poll();
		handle.incCurrTaskCount();
		handlesQueue.add(handle);
		
		tasksQueue.poll();
		
		handle.execTask(task);
	}
	
	public void addTask(DistriTask task) {
		if (task == null) return;
		
		if (cache.containsKey(task.getUrlString())) {
			DistriResult result = cache.get(task.getUrlString());
			task.finishTask(result);
		}
		
		synchronized (queuesLock) {
			tasksQueue.add(task);
			task.setInitialTime();
			
			queuesChanged = true;
			queuesLock.notifyAll();
		}
	}
	
	public void handleCallback(DistriHandle handle) {
		synchronized (queuesLock) {
			handlesQueue.remove(handle);
			handle.decCurrTaskCount();
			handlesQueue.add(handle);
			
			queuesChanged = true;
			queuesLock.notifyAll();
		}
	}
	
	public void recurseTask(DistriTask task) throws Exception {
		for (String link : task.getResult().getQueryLinks()) {
			String recurseUrl = null;
			if (link == null || link.isEmpty() || link.equals("/")) {
				recurseUrl = task.getUrl().getHost();
			} else {
				if (link.charAt(0) == '/' && link.charAt(1) != '/') {
					recurseUrl = String.format("%s://%s%s", 
							task.getUrl().getProtocol(),
							task.getUrl().getHost(), link);
				} else {
					recurseUrl = link;
				}
			}
			
			DistriTask recurseTask = task.createRecurseTask(recurseUrl);
			addTask(recurseTask);
		}
	}
	
	public void addToCache(String url, DistriResult result) {
		cache.put(url, result);
	}
	
	@SuppressWarnings("unchecked")
	public boolean setCacheSize(int size) {
		if (hasCache()) {
			return ((LRUMap)cache).maxSize() == size;
		} else {
			cache = (Map<String, DistriResult>)Collections.synchronizedMap(new LRUMap(size));
			return cache != null;
		}
	}
	
	public boolean hasCache() {
		return cache != null;
	}
	
	public ArrayList<DistriHandle> getHandles() {
		return handles;
	}
	
	public DistriHandle getHandleByHost(String host) {
		return handlesByHost.get(host);
	}
	
	public Integer getHostMinDelay(String host) {
		Integer ret = hostsMinDelays.get(host);
		return ret == null ? DEFAULT_MIN_DELAY : ret;
	}
	
	public void setHostMinDelay(String host, int minDelay) {
		hostsMinDelays.put(host, minDelay);
	}

	public HashMap<String, Integer> getHostsMinIntervals() {
		return hostsMinDelays;
	}

	public void setHostsMinIntervals(HashMap<String, Integer> hostsMinIntervals) {
		this.hostsMinDelays = hostsMinIntervals;
	}

	private class HandleAvailTaskCountComp implements Comparator<DistriHandle> {

		public int compare(DistriHandle handle1, DistriHandle handle2) {
			return handle2.getAvailTaskCount() - handle1.getAvailTaskCount();
		}
		
	}
	
	private class TaskExhaustionComp implements Comparator<DistriTask> {

		public int compare(DistriTask task1, DistriTask task2) {
			long exhaustion1 = task1.getExhaustion();
			long exhaustion2 = task2.getExhaustion();
			if (exhaustion1 > exhaustion2) return -1;
			if (exhaustion1 < exhaustion2) return 1;
			return 0;
		}
		
	}
	
}
