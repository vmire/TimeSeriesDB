package com.mireau.timeseries;

public class LockTimeoutException extends Exception {
	
	private static final long serialVersionUID = 4139984902863969824L;

	public LockTimeoutException() {
	}

	public LockTimeoutException(String message) {
		super(message);
	}

	public LockTimeoutException(Throwable cause) {
		super(cause);
	}

	public LockTimeoutException(String message, Throwable cause) {
		super(message, cause);
	}

	public LockTimeoutException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
