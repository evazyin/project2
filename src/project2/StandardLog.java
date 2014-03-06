package project2;


import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * The log implementation that uses standard output for events and standard error for errors.
 * It adds a timestamp to every message.
 */
public class StandardLog implements ILog {

	/**
	 * Logs an event.
	 */
	public void Log(String message) {
		System.out.println(getTimestamp() + message);
	}

	/**
	 * Logs an error.
	 */
	public void LogError(String message) {
		System.err.println(getTimestamp() + message);
	}
	
	private static String getTimestamp() {
    	SimpleDateFormat format = new SimpleDateFormat("[yyyy.MM.dd HH:mm:ss.SSS] ");
    	return format.format(new Date());
	}
}
