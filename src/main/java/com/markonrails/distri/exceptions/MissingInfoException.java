package com.markonrails.distri.exceptions;

@SuppressWarnings("serial")
public class MissingInfoException extends Exception {
	
	public enum Info {
		HOST_USER("User or host"),
		AUTHENTICATION("Authentications");
		
		private final String info;
		
		private Info(String info) {
			this.info = info;
		}
		
		public String toString() {
			return info;
		}
	}
	
	public MissingInfoException() {
		super("Some information is missing");
	}
	
	public MissingInfoException(Info info) {
		super(String.format("%s information if missing", info));
	}
}
