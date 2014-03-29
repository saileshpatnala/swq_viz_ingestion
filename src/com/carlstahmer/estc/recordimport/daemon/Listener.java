package com.carlstahmer.estc.recordimport.daemon;

public class Listener {
	
	// Requires the presence of a config.yml in the application root to run.
	// Values in the config can be overwritten at runtime via command line
	// arguments as follows:
	//
	// -listendir /dirctory/path/of/listen/directory
	// -writedir /directory/path/of/output/directory
	// -runinterval the interval to wait between listinging attempts
	// -orgcode the marc org code to use in constucting the new records
	//
	// Config.yml file must be in place even if you are supplying information
	// Via the command line.

	public static void main(String[] args) {
		
		// Welcome Message
		System.out.println("Starting ESTC Marc Processing Server...");
		
		// Load required configuration YML file
		Conf config = new Conf();
		if (!config.loadConf()) {
			System.out.println("Unable to load configuration file.");
			System.out.println("Aborting operation!");
			System.exit(0);
		} else {
			System.out.println("Configuration YML successfully loaded.");
		}
		
		// Override YML configuration with command line args if present
		if (!config.checkArgs(args)) {
			System.out.println("Error  processing command line arguments.");
			System.out.println("Aborting operation!");
			System.exit(0);
		} else {	
			System.out.println("Command line overrides successfully processed.");
		}
		
		// Check listen directory for proper formatting of trailing slash	
	    if (config.listenDir.substring(config.listenDir.length() - 1).equals("/")) {
	    	config.listenDir = config.listenDir.substring(0, config.listenDir.length() - 1);
	    }
	    
	    // Instantiate db object and model
	    SqlModel sqlObj = new SqlModel(config);
	    System.out.println("DB object and model successfully intantiated.");
	    
	    // Start the process manager
	    System.out.println("Listening at: "+config.listenDir);
	    ProcessManager pm = new ProcessManager(config, sqlObj);
		pm.runOnce();
		
		
		System.out.println("Process completed.");
		System.out.println("Goodby World!");	
	}

}
