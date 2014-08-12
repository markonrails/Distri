package com.markonrails.distri;

import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.jcraft.jsch.*;
import com.markonrails.distri.exceptions.*;

public class DistriHandle {
	
	public static final String DEFAULT_CURLER_PATH = "/home/distri/curler/curler.sh";
	public static final int DEFAULT_PORT = 22;
	public static final int DEFAULT_SSH_SLEEP_TIME = 100;
	public static final int DEFAULT_MAX_TASK_COUNT = 10;
	
	private DistriControl control;
	
	private ExecutorService curlerThreadPool;

	private JSch jsch;
	private Session session;
	private String curlerPath;
	
	private String host;
	private int port;
	
	private String user;
	private String password;
	private String keyPath;
	private String passPhrase;
	
	private int sshSleepTime;
	
	private final int maxTaskCount;
	private int currTaskCount;
	
	private HashMap<String, Date> hostsVisitTimes;
	
	private class CurlerThread implements Runnable {
		
		private DistriHandle handle;
		private DistriTask task;
		private DistriControl control;
		
		public CurlerThread(DistriHandle handle, DistriTask task) {
			this.handle = handle;
			this.task = task;
			this.control = handle.getControl();
		}

		public void run() {
			String userAgent = task.getUserAgent();
			String url = task.getUrlString();
			String command = String.format("%s \"%s\" \"%s\"", 
					curlerPath, userAgent, url);
			String curlee = "";
			
			try {
				curlee = handle.execCommand(command);
				
				DistriResult result = null;
				if (task.getRecurseDepth() > 0) {
					result = new DistriResult(curlee, task.getLinkSelector());
				} else {
					result = new DistriResult(curlee);
				}
				task.finishTask(result);
				
				if (control != null) {
					if (control.hasCache()) {
						control.addToCache(task.getUrlString(), result);
					}
					if (task.getRecurseDepth() > 0) {
						control.recurseTask(task);
					}
				}
			} catch (InvalidResultException e) {
				task.failTask(e, curlee);
				if (control != null && !task.isTerminated()) {
					System.out.println(String.format(
							"Add task for %d: %s", task.getTimesTried(), task.getUrlString()));
					control.addTask(task);
				}
			} catch (Exception e) {
				task.failTask(e, curlee);
				if (curlee.isEmpty() && control != null && !task.isTerminated()) {
					System.out.println(String.format(
							"Add task for %d: %s", task.getTimesTried(), task.getUrlString()));
					control.addTask(task);
				}
			} finally {
				if (control != null) {
					System.out.println("Callback handle");
					control.handleCallback(handle);
				}
			}
		}
		
	}
	
	public DistriHandle(DistriControl control, int maxTaskCount, 
			String host, int port, String user) {
		this.control = control;
		
		jsch = new JSch();
		
		this.curlerPath = DEFAULT_CURLER_PATH;
		
		assert(maxTaskCount > 0);
		this.maxTaskCount = maxTaskCount;
		currTaskCount = 0;
		
		this.curlerThreadPool = Executors.newFixedThreadPool(maxTaskCount);
		
		this.host = host;
		this.port = port;
		this.user = user;
		
		sshSleepTime = DEFAULT_SSH_SLEEP_TIME;
		
		hostsVisitTimes = new HashMap<String, Date>();
	}
	
	public void execTask(DistriTask task) {
		setHostVisitTime(task.getUrl().getHost());
		Runnable curlerThread = new CurlerThread(this, task);
		curlerThreadPool.execute(curlerThread);
	}
	
	public boolean canExecTask(DistriTask task) throws Exception {
		if (!connect()) {
			return false;
		}
		
		if (control == null) {
			return true;
		}
		
		Date visitTime = hostsVisitTimes.get(task.getUrl().getHost());
		if (visitTime == null) {
			return true;
		}
		
		Date curTime = new Date();
		long timePassed = curTime.getTime() - visitTime.getTime();
		
		Integer minDelay = control.getHostMinDelay(task.getUrl().getHost());
		
		return timePassed >= minDelay;
	}
	
	public boolean connect() throws Exception {
		Date startTime = new Date();
		
		if (session != null && session.isConnected()) {
			return true;
		}
		
		if (host == null || user == null) {
			throw new MissingInfoException(MissingInfoException.Info.HOST_USER);
		}
		if (password == null && keyPath == null) {
			throw new MissingInfoException(MissingInfoException.Info.AUTHENTICATION);
		}
		
		if (password == null) {
			if (passPhrase == null || passPhrase.isEmpty()) {
				jsch.addIdentity(keyPath);
			} else {
				jsch.addIdentity(keyPath, passPhrase);
			}
			
			session = jsch.getSession(user, host, port);
		} else {
			session = jsch.getSession(user, host, port);
			session.setPassword(password);
		}
		
		session.connect();
		
		String curlerPathList = execCommand(String.format("ls %s", curlerPath));
		if (curlerPathList.indexOf(curlerPath) == -1) {
			throw new WrongCurlerPathException();
		}
		
		Date endTime = new Date();
		System.out.println(String.format("Connect takes %d ms", 
				endTime.getTime() - startTime.getTime()));
		
		return session.isConnected();
	}
	
	public String execCommand(String command) throws Exception {
		Date startTime = new Date();
		
		StringBuilder sb = new StringBuilder();
		
		Channel channel = session.openChannel("exec");
		((ChannelExec)channel).setCommand(command);
		channel.setInputStream(null);
		
		InputStream in = channel.getInputStream();
		
		channel.connect();
		
		byte[] tmp = new byte[1024];
		while (true) {
			int i = in.read(tmp, 0, 1024);
			if (i > 0) sb.append(new String(tmp, 0, i));
			if (channel.isClosed()) {
				if (in.available() > 0) continue;
				break;
			}
		}
		
		channel.disconnect();
		
		Date endTime = new Date();
		System.out.println(String.format("%s exec %s takes %d ms", 
				host, command, endTime.getTime() - startTime.getTime()));
		
		return sb.toString();
	}
	
	public void setKnownHosts(String filename) throws Exception {
		jsch.setKnownHosts(filename);
	}
	
	public DistriControl getControl() {
		return control;
	}

	public int getAvailTaskCount() {
		return maxTaskCount - currTaskCount;
	}
	
	public String getCurlerPath() {
		return curlerPath;
	}

	public int getPort() {
		return port;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public String getKeyPath() {
		return keyPath;
	}

	public String getPassPhrase() {
		return passPhrase;
	}
	
	public int getSshSleepTime() {
		return sshSleepTime;
	}

	public int getMaxTaskCount() {
		return maxTaskCount;
	}

	public void incCurrTaskCount() {
		currTaskCount++;
	}
	
	public void decCurrTaskCount() {
		currTaskCount--;
	}
	
	public int getCurrTaskCount() {
		return currTaskCount;
	}
	
	public HashMap<String, Date> getHostsVisitTimes() {
		return hostsVisitTimes;
	}
	
	public Date getHostVisitTime(String host) {
		return hostsVisitTimes.get(host);
	}

	public void setCurlerPath(String curlerPath) {
		this.curlerPath = curlerPath;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	public void setKeyPath(String keyPath) {
		this.keyPath = keyPath;
	}
	
	public void setPassPhrase(String passPhrase) {
		this.passPhrase = passPhrase;
	}
	
	public void setSshSleepTime(int sshSleepTime) {
		this.sshSleepTime = sshSleepTime;
	}
	
	public void setHostVisitTime(String host) {
		hostsVisitTimes.put(host, new Date());
	}
	
}
