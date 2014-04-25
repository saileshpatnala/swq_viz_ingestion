package com.carlstahmer.estc.recordimport.daemon;

/**
 * <p>A logging object for handling system messaging.</p>
 */
public class Logger {
	
	Conf config;
	SqlModel sqlObj;
	
	/**
	 * <p>Constructor class that assigns passed Config object and SqlModel to 
	 * class instances.</p>
	 *
	 * @param  configObj    an instance of the Conf class
	 * @param  sqlModObj  	an instance of the sqlModel class
	 */
	public Logger(Conf configObj) {
		config = configObj;
		sqlObj = new SqlModel(config);
	}
	
	/**
	 * <p>Initiates process of writing a message to the log.
	 * Determines whether to log to console or sql and sends
	 * message to appropriate helper method.</p>
	 *
	 * @param  	messagetype    	[1] = error, [2] = info, [3] = debug
	 * @param 	filename		the filename of the file making the call
	 * @param 	linenumber		the line number of the call
	 * @param 	message			the message to log
	 */
	public void log(int messagetype, String filename, int linenumber, String message) {
		boolean shouldPrint = false;
		if (config.debug) {
			shouldPrint = true;
		} else if (messagetype == 1 || messagetype == 2) {
			shouldPrint = true;
		}
		if (shouldPrint) {
			if (config.console) {
				printToConsole(messagetype, filename, linenumber, message);
			} else {
				printToSql(messagetype, filename, linenumber, message);
			}
		}	
	}
	
	
	/**
	 * <p>Prints a log message to the console.</p>
	 *
	 * @param  	messagetype    	[1] = error, [2] = info, [3] = debug
	 * @param 	filename		the filename of the file making the call
	 * @param 	linenumber		the line number of the call
	 * @param 	message			the message to log
	 */
	private void printToConsole(int messagetype, String filename, int linenumber, String message) {
		String strMessageType;
		if (messagetype == 1) {
			strMessageType = "ERROR";
		} else if (messagetype == 2) {
			strMessageType = "INFO";
		} else if (messagetype == 3) {
			strMessageType = "DEBUG";
		} else {
			strMessageType = "OTHER";
		}
		System.out.println(strMessageType + ":\t" + filename + "\t" + linenumber + "\t" + message);
	}
	
	/**
	 * <p>Prints log message to sql</p>
	 *
	 * @param  	messagetype    	[1] = error, [2] = info, [3] = debug
	 * @param 	filename		the filename of the file making the call
	 * @param 	linenumber		the line number of the call
	 * @param 	message			the message to log
	 */
	private void printToSql(int messagetype, String filename, int linenumber, String message) {
		boolean submitSuccess = false;
		sqlObj.openConnection();
		if (sqlObj.connOpen) {
			int retID = sqlObj.insertLogMessage(messagetype, filename, linenumber, message);
			if (retID > 0) {
				submitSuccess = true;
			}
			sqlObj.closeConnection();
		}
		if (!submitSuccess) {
			printToConsole(1, filename, linenumber, "Logger has no active SQL connection.  Original message ["+message+"]");
		}
	}
	

}
