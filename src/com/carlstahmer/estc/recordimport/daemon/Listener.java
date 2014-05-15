package com.carlstahmer.estc.recordimport.daemon;

/**
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
		pm.runOnce();
		
		sqlObj.closeConnection();
		logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Process Completed");
		System.out.println("Goodby World!");	
	}

}
