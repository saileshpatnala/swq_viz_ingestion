package com.carlstahmer.estc.recordimport.daemon;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.yaml.snakeyaml.Yaml;

/**
 * <p>A configuration class that contains code for processing a .yml configuration file
 * and loading all configuration variables into class objects.  By creating a local
 * instance of this class or being passed an instance, other classes and functions gain
 * access to configuration variable values.</p>
 */
public class Conf {

	String listenDir;
	String writeDir;
	int runInterval;
	String orgcode;
	String dbserver;
	String dbname;
	String dbuser;
	String dbpass;
	boolean debug;
	boolean console;
	
	public Conf() {
		listenDir = "";
		writeDir = "";
		runInterval = 10000000;
		orgcode = "";
		dbserver = "";
		dbname = "";
		dbuser = "";
		dbpass = "";
		debug = false;
		console = false;
	}
	
	/**
	 * <p>An initialization class that tells the object to read the .yml configuration file
	 * and load all values.  Must be called before trying to access any class properties.</p>
	 * 
	 * @return      			a boolean value indicating whether the configuration .yml has been successfully loaded.
	 */
	public boolean loadConf() {
			
		boolean loaded = false;
		// Load Configuration YAML
		try {
			
			InputStream yamlInput = new FileInputStream(new File("config.yml"));
			System.out.println("Found configuration file...");
			Yaml yaml = new Yaml();
			@SuppressWarnings("unchecked")
			Map<String, Object> map = (Map<String, Object>) yaml.load(yamlInput);
			listenDir = (String) map.get("listendir");
			writeDir = (String) map.get("writedir");
			runInterval = (Integer) map.get("runinterval");
			orgcode = (String) map.get("orgcode");
			dbserver = (String) map.get("dbserver");
			dbname = (String) map.get("dbname");
			dbuser = (String) map.get("dbuser");
			dbpass = (String) map.get("dbpass");
			
			loaded = true;
		
		} catch (FileNotFoundException e) {
		    System.err.println("Configuration FileNotFoundException: " + e.getMessage());
		    loaded = false;
		}
			
		return loaded;
	}
	
	// checks the command line arguments sting and replaces values from the conf yaml
	// as appropriate.
	
	/**
	 * <p>Checks for passed command line arguments and replaces .yml loaded values with those
	 * entered at the command line as appropriate.</p>
	 *
	 * @param  args    	command line String[] args array
	 * @return      	a boolean value indicate whether or not method executed successfully
	 */
	public boolean checkArgs(String[] args) {
		
		boolean loaded = false;
	
		try {
			Options options = new Options();
			options.addOption("listendir", true, "full directory path to listen directory");
			options.addOption("writedir", true, "full directory path to write output records");
			options.addOption("orgcode", true, "the marc orgcode of the cataloge owner");
			options.addOption("runinterval", true, "the time to wait between process spawning");
			options.addOption("dbserver", true, "the sql server");
			options.addOption("dbname", true, "the sql server database name");
			options.addOption("dbuser", true, "the sql user");
			options.addOption("dbpass", true, "the sql user");
			options.addOption("debug", false, "run in debug mode - verbose logging");
			options.addOption("console", false, "write log to console instead of database");
			options.addOption("help", false, "get help");
							
			
			CommandLineParser parser = new BasicParser();
			CommandLine cmd = parser.parse( options, args);
			if(cmd.hasOption("listendir")) {
				String ldirVal = cmd.getOptionValue("listendir");
				if(ldirVal != null) {
					listenDir = ldirVal;
				}
			} 
			if (cmd.hasOption("writedir")) {
				String wdirVal = cmd.getOptionValue("writedir");
				if(wdirVal != null) {
					writeDir = wdirVal;
				}
			}
			if (cmd.hasOption("orgcode")) {
				String ocVal = cmd.getOptionValue("orgcode");
				if(ocVal != null) {
					orgcode = ocVal;
				}
			} 
			if (cmd.hasOption("runinterval")) {
				String riVal = cmd.getOptionValue("runinterval");
				if(riVal != null) {
					runInterval = Integer.parseInt(riVal);
				}
			}
			
			if (cmd.hasOption("dbserver")) {
				String srvVal = cmd.getOptionValue("dbserver");
				if(srvVal != null) {
					dbserver = srvVal;
				}
			}
			if (cmd.hasOption("dbname")) {
				String dbnameVal = cmd.getOptionValue("dbname");
				if(dbnameVal != null) {
					dbname = dbnameVal;
				}
			}
			if (cmd.hasOption("dbuser")) {
				String dbuserVal = cmd.getOptionValue("dbuser");
				if(dbuserVal != null) {
					dbuser = dbuserVal;
				}
			}
			if (cmd.hasOption("dbpass")) {
				String dbpassVal = cmd.getOptionValue("dbpass");
				if(dbpassVal != null) {
					dbpass = dbpassVal;
				}
			}
			if (cmd.hasOption("debug")) {
				debug = true;
			}
			if (cmd.hasOption("console")) {
				console = true;
			}
			if (cmd.hasOption("help")) {
					String HelpString = "Requires the presence of a config.yml in the application root to run correcly. ";
					HelpString = HelpString + "Values in the config can be overwritten at runtime via command line ";
					HelpString = HelpString + "arguments as follows:\n\n";
					HelpString = HelpString + "-listendir [/dirctory/path/of/listen/directory]\n";
					HelpString = HelpString + "-writedir [/directory/path/of/output/directory]\n";
					HelpString = HelpString + "-runinterval [the interval to wait between listinging attempts]\n";
					HelpString = HelpString + "-orgcode [the marc org code to use in constucting the new records]\n";
					HelpString = HelpString + "-dbserver [the sql database server]\n";
					HelpString = HelpString + "-dbname [the sql database name]\n";
					HelpString = HelpString + "-dbuser [the sql database user]\n";
					HelpString = HelpString + "-dbpass [the sql database password]\n";
					HelpString = HelpString + "-debug [runs application in debug mode - verbose logging]\n";
					HelpString = HelpString + "-console [writes log output to console instead of database]\n";
					HelpString = HelpString + "-help [runs this help message]\n\n";
					HelpString = HelpString + "Config.yml file must be in place even if you are supplying information ";
					HelpString = HelpString + "via the command line.";
					System.out.println(HelpString);
					System.exit(0);
			}

			loaded = true;
			
		}  catch (ParseException e) {
			System.err.println("ERROR:\tCommand Line Argument Error: " + e.getMessage());
			loaded = false;
		}
			
		return loaded;	
			
	}
	
}
