package project2;


/**
 * Contains methods for logging events and errors.
 */
public interface ILog {
	
	/**
	 * Logs an event.
	 * @param message the message
	 */
	void Log(String message);
	
	/**
	 * Logs an error.
	 * @param message the error message
	 */
	void LogError(String message);
}
