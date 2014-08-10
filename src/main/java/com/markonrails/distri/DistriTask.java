package com.markonrails.distri;

import java.net.URL;
import java.util.Date;

public class DistriTask {
	
	public static final int DEFAULT_TOTAL_TRIES = 10;
	public static final int DEFAULT_MIN_WAIT_TIME = 1000;
	
	private Date initialTime;
	private Date lastTryTime;
	
	private String userAgent;
	private URL url;
	private DistriResult result;
	
	private final int maxTries;
	private int timesTried;
	private int minWaitTime;
	
	private String linkSelector;
	private final int recurseDepth;
	
	private FinishCallback finishCallback;
	private FailCallback failCallback;
	private TerminateCallback terminateCallback;
	
	public interface FinishCallback {
		public void execute(DistriResult result);
	}
	
	public interface FailCallback {
		public void execute(Exception e, String s);
	}
	
	public interface TerminateCallback {
		public void execute();
	}
	
	public DistriTask(String url) throws Exception {
		this("", url);
	}
	
	public DistriTask(String url, int maxTries) throws Exception {
		this("", url, maxTries, 0);
	}
	
	public DistriTask(String userAgent, String url) throws Exception {
		this(userAgent, url, DEFAULT_TOTAL_TRIES, 0);
	}
	
	public DistriTask(String userAgent, String url, int maxTries) throws Exception {
		this(userAgent, url, maxTries, 0);
	}
	
	public DistriTask(String userAgent, String url, int maxTries, int recurseDepth) throws Exception {
		this.initialTime = new Date();
		this.userAgent = userAgent;
		this.url = new URL(url);
		this.maxTries = maxTries;
		this.timesTried = 0;
		this.minWaitTime = DEFAULT_MIN_WAIT_TIME;
		this.recurseDepth = recurseDepth;
	}
	
	public DistriTask createRecurseTask(String url) throws Exception {
		DistriTask recurseTask = new DistriTask(userAgent, url, maxTries, recurseDepth - 1);
		recurseTask.setLinkSelector(linkSelector);
		recurseTask.setFinishCallback(finishCallback);
		recurseTask.setFailCallback(failCallback);
		recurseTask.setTerminateCallback(terminateCallback);
		return recurseTask;
	}
	
	public void finishTask(DistriResult result) {
		this.lastTryTime = new Date();
		this.result = result;
		if (finishCallback != null) {
			finishCallback.execute(result);
		}
		if (terminateCallback != null) {
			terminateCallback.execute();
		}
	}
	
	public void failTask(Exception e, String s) {
		this.lastTryTime = new Date();
		this.timesTried++;
		if (failCallback != null) {
			failCallback.execute(e, s);
		}
		if (isTerminated() && terminateCallback != null) {
			terminateCallback.execute();
		}
	}
	
	public long getExhaustion() {
		if (timeSinceLastTry() <= minWaitTime) return 0;
		return timeSinceInitial() / (timesTried + 1);
	}
	
	public boolean isTerminated() {
		return timesTried >= maxTries || result != null;
	}
	
	public void setMinWaitTime(int minWaitTime) {
		this.minWaitTime = minWaitTime;
	}
	
	public void setTerminateCallback(TerminateCallback terminateCallback) {
		this.terminateCallback = terminateCallback;
	}

	public void setFinishCallback(FinishCallback finishCallback) {
		this.finishCallback = finishCallback;
	}

	public void setFailCallback(FailCallback failCallback) {
		this.failCallback = failCallback;
	}
	
	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	public void setLinkSelector(String linkSelector) {
		this.linkSelector = linkSelector;
	}

	public Date getInitialTime() {
		return initialTime;
	}

	public Date getLastTryTime() {
		return lastTryTime;
	}
	
	public long timeSinceInitial() {
		Date curTime = new Date();
		return curTime.getTime() - initialTime.getTime();
	}
	
	public long timeSinceLastTry() {
		if (lastTryTime == null) return Long.MAX_VALUE;
		Date curTime = new Date();
		return curTime.getTime() - lastTryTime.getTime();
	}

	public String getUserAgent() {
		return userAgent;
	}
	
	public URL getUrl() {
		return url;
	}
	
	public String getUrlString() {
		return url.toString();
	}
	
	public DistriResult getResult() {
		return result;
	}

	public int getMaxTries() {
		return maxTries;
	}

	public int getTimesTried() {
		return timesTried;
	}
	
	public int getMinWaitTime() {
		return minWaitTime;
	}

	public String getLinkSelector() {
		return linkSelector;
	}

	public int getRecurseDepth() {
		return recurseDepth;
	}

}
