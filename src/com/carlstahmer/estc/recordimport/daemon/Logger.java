/**
 *	<p>Copyright (c) 2014, Carl Stahmer - <a href="http://www.carlstahmer.com">www.carlstahmer.com</a>.</p>
 *	
 *	<p>This file is part of the ESTC Record Importer package, a server 
 *	daemon that processes incoming MARC cataloging data stored in binary
 *	MARC, .csv, and .txt formats, checks the records for scope on date,
 *	language, and place of publication, and the makes the filtered
 *	records available to other services via OAI-PMH.</p>
 *
 *	<p>The ESTC Record Importer is free software: you can redistribute it 
 *	and/or modify it under the terms of the GNU General Public License 
 *	as published by the Free Software Foundation, either version 3 of 
 *	the License, or (at your option) any later version.</p>
 *
 *	<p>The ESTC Record Importer is distributed in the hope that it will 
 *	be useful, but WITHOUT ANY WARRANTY; without even the implied warranty 
 *	of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 *	GNU General Public License for more details.</p>
 *
 *	<p>You should have received a copy of the GNU General Public License  
 *	along with the ESTC Record Importer distribution.  If not, 
 *	see <a href="http://www.gnu.org/licenses/">http://www.gnu.org/licenses/</a>.</p>
 *
 *	<p>Development of this software was made possible through funding from 
 *	the Andrew W. Mellon Foundation.</p>
 */
package com.carlstahmer.estc.recordimport.daemon;

/**
 * @author cstahmer
 * 
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
		int retID = sqlObj.insertLogMessage(messagetype, filename, linenumber, message);
		if (retID > 0) {
			submitSuccess = true;
		}
		if (!submitSuccess) {
			printToConsole(1, filename, linenumber, "Logger has no active SQL connection.  Original message ["+message+"]");
		}
	}
	

}
