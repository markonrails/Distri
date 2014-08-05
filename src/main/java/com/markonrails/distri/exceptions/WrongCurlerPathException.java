package com.markonrails.distri.exceptions;

@SuppressWarnings("serial")
public class WrongCurlerPathException extends Exception {
	public WrongCurlerPathException() {
		super("Couldn't find the curler.sh file on the curler path provided");
	}
}
