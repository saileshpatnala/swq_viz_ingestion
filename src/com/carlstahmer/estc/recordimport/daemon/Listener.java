/**
 *	<p>Copyright (c) 2016, Carl Stahmer - <a href="http://www.carlstahmer.com">www.carlstahmer.com</a>.</p>
 *	
 *	<p>This file is part of the ESTC Record Importer package, a server 
 *	daemon that processes incoming MARC cataloging data stored in binary
 *	MARC, .csv, and .txt formats, checks the records for scope on date,
 *	language, and place of publication, and exports the filtered
 *	records as RDF suitable for linked data exchange.</p>
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
 *	the Andrew W. Mellon Foundation which maintains a nonexclusive, 
 *  royalty-free, worldwide, perpetual, irrevocable license to distribute 
 *  this software either in wholoe or in part for scholarly and educational purposes.</p>
 */

package com.carlstahmer.estc.recordimport.daemon;

/**
 * @author cstahmer
 * 
 * <p>The main Listener Class launches the server/daemon.  Once running,
 * the application continuously monitors the incoming directory tree
 * for new files to import and also launches an OAI-PMH server that exposes
 * the imported data set.</p>
 * 
 * <p>Requires the presence of a config.yml file in the application root to run.</p>
 */
public class Listener {
	
	static Logger logger;
	
	/**
	 * <p>The main class that launches the process.</p>
	 * 
	 * <p>Requires the presence of a config.yml file in the application root to run. 
	 * Values in config.yml can be overwritten at runtime via unix style command line
	 * arguments as defined in the parameters section of this documentation.  The
	 * config.yml file must be in place even if you supply other configuration 
	 * information via the command line.</p>
	 *
	 * @param  	-listendir		/full/directory/path/of/listen/directory
	 * @param  	-writedir  		/full/directory/path/of/output/directory (Deprecated)
	 * @param 	-orgcode		the marc org code to use in constructing the new records
	 * @param	-runinterval	the interval to wait between listening attempts
	 * @param 	-dbserver		the name of the database server
	 * @param 	-dbname			the database name
	 * @param 	-dbuser			the database user
	 * @param 	-dbpass			the database password
	 * @param	-debug			flag to run in debug mode
	 * @param	-console		flag to run log output to console instead of database
	 * @param 	-help			flag to return help text
	 */
	public static void main(String[] args) {
		
		// Welcome Message
		System.out.println("Starting ESTC Marc Import/Export Server...");
		
		// Load required configuration YML file
		Conf config = new Conf();
		if (!config.loadConf()) {
			System.out.println("Unable to load configuration file");
			System.out.println("Aborting operation!");
			System.exit(0);
		} else {
			System.out.println("Configuration YML successfully loaded...");
		}	
		
	    // Instantiate db object and model
	    SqlModel sqlObj = new SqlModel(config);
	    sqlObj.openConnection();
	    System.out.println("DB object and model successfully intantiated...");
	    
	    // create a logger object
	    logger = new Logger(config);
	    System.out.println("Logger successfully initiated...");
	    if (!config.console) {
	    	System.out.println("Logging to database runlog...");
	    }
	    logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Application successfully loaded with config, sqlObj, and logger");
	    
	    
		// Override YML configuration with command line args if present
		if (!config.checkArgs(args)) {
			logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Error  processing command line arguments");
			logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Aborting applicaiton!");
			System.out.println("Aborting operation!");
			System.exit(0);
		} else {	
			logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Command line overrides successfully processed");
		}
		
		// Check listen directory for proper formatting of trailing slash	
	    if (config.listenDir.substring(config.listenDir.length() - 1).equals("/")) {
	    	config.listenDir = config.listenDir.substring(0, config.listenDir.length() - 1);
	    }
	    
	    // Start the process manager
	    ProcessManager pm = new ProcessManager(config, sqlObj);
	    logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Listening at: "+config.listenDir);
		
	    // I'm currently calling the runOnce() method.  I will ultimately
	    // need to fix the actual daemon listener and change this.
	    // I should probably make it a config / command line option
	    pm.runOnce();
	    		
		sqlObj.closeConnection();
		logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Process Completed");
		System.out.println("Goodby World!");	
		
	}

}
