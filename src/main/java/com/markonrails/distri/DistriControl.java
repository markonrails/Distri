package com.markonrails.distri;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.PriorityBlockingQueue;

public class DistriControl {
	
	private HashMap<String, Integer> hostsMinIntervals;

	private ArrayList<DistriHandle> handles;
	private HashMap<String, DistriHandle> handlesByHost;
	private PriorityBlockingQueue<DistriHandle> handlesQueue;
	
	private PriorityBlockingQueue<DistriTask> tasksQueue;
	
	private Object queuesLock;
	private boolean queuesChanged;
	
	public DistriControl() {
		hostsMinIntervals = new HashMap<String, Integer>();
		
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
							while (!queuesChanged) {
								queuesLock.wait();
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
		
		synchronized (queuesLock) {
			tasksQueue.add(task);
			
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
	
	public ArrayList<DistriHandle> getHandles() {
		return handles;
	}
	
	public DistriHandle getHandleByHost(String host) {
		return handlesByHost.get(host);
	}
	
	public Integer getHostMinInterval(String host) {
		return hostsMinIntervals.get(host);
	}
	
	public void setHostMinInterval(String host, int minInterval) {
		hostsMinIntervals.put(host, minInterval);
	}

	public HashMap<String, Integer> getHostsMinIntervals() {
		return hostsMinIntervals;
	}

	public void setHostsMinIntervals(HashMap<String, Integer> hostsMinIntervals) {
		this.hostsMinIntervals = hostsMinIntervals;
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
