package com.carlstahmer.estc.recordimport.daemon;

import java.io.File;

/**
 * <p>A class for managing the MARC record import function.</p>
 */
public class ProcessManager {
	
	Conf config;
	SqlModel sqlObj;
	Logger logger;

	
	/**
	 * <p>Constructor class that assigns passed Config object and SqlModel to 
	 * class instances.</p>
	 *
	 * @param  configObj    an instance of the Conf class
	 * @param  sqlModObj  	an instance of the sqlModel class
	 */
	public ProcessManager(Conf configObj, SqlModel sqlModObj) {
		config = configObj;
		sqlObj = sqlModObj;
		logger = new Logger(config);
	}
	
	/**
	 * <p>Launches the MARC import process as a daemon that runs repeatedly on the top of the next hour 
	 * after the last completed processing.</p>
	 */
	public void runAsDaemon() {
		// this should setup a kid of constantly running process that re-calls runOnce() 
		// continuously waiting the designated runInterval between each execution
		
		
		
	}
	
	/**
	 * <p>Launches the MARC import process a single time.</p>
	 */	
	public void runOnce() {
		// see code in current_mac_working_folder/estc_java_oldcode.txt for rest of process code
		FileUtils fileUts = new FileUtils(config, sqlObj);
		fileUts.listFoldersRecursive();
		if (fileUts.directoryList.size() > 0) {
			for (int id=0;id<fileUts.directoryList.size();id++) {
				String curCode = fileUts.directoryList.get(id);
				String curDir = config.listenDir+"/"+curCode;
				logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Working in directory: "+curDir);
				fileUts.listFilesForFolder(curDir);
				if (fileUts.fileList.size() > 0) {
					
					for (int i=0; i < fileUts.fileList.size(); i++) {

						// construct a filename
						String fileToProcess = curDir + "/" + fileUts.fileList.get(i);
						
						
						// instantiate file object to get last processed date
						File fileInfo = new File(fileToProcess);
						long fileModDate = fileInfo.lastModified();

						// now check to see if this is existing file that has not been modified.
						// create or load SQL file record
						// selectFileRecord(curCode, fileName, fileModDate)
						sqlObj.openConnection();
						if (sqlObj.connOpen) {
							logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Sql connection opened");
							logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Checking whether file has been previously processed");
							int fileRecordId = sqlObj.selectFileRecordStrict(curCode, fileUts.fileList.get(i), fileModDate);
							if (fileRecordId == 0) {

								logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), fileUts.fileList.get(i)+" is a new or modified file");
							
								// get a file type and process accordingly
								int intFileType = fileUts.fileType(fileUts.fileList.get(i));
								
								if (intFileType == 1) {
									// this is a marc file
									logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Processing MARC file "+fileToProcess);
									LoadMarc myMarcInstance = new LoadMarc(config, sqlObj);
									myMarcInstance.loadMarcFile(fileToProcess, curCode);
									
								} else if (intFileType == 2) {
									// this is a text file
									logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Processing text file "+fileToProcess);
								
								
								} else if (intFileType == 3) {
									// this is an excel file
									logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Processing Excel file "+fileToProcess);
									
									
								} else if (intFileType == 4) {
									// this is a csv file
									logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Processing CSV file "+fileToProcess);
									
									
								} else if (intFileType == 5) {
									// this is an xml file
									logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Processing XML file "+fileToProcess);
									
									
								} else {
									// this is an unrecognized file
									logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Unable to process file "+fileToProcess+"--Unrecognized file type");
								}
							
							
							
							
							} else {
								logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Skipping file " + fileUts.fileList.get(i) + " -- already exists in system with last modification date of " + fileModDate + " -- file ID " + fileRecordId);
							}
								
						} else {
							logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Unable to open SQL connection for file check");
						}
								
						// now that I'm here, whatever kind of file we started with,
						// the data has been written to the system so now I can process
						// it by checking if it is date and language relevant, if a holding
						// record, does it have the correct association with a bib record
						// does it have an ESTC number anywhere in the data that can 
						// be extracted and placed in the proper place
						
						// need to do some more research here and figure out
						// if I want to go to holding/bib record format or collapse
						// everything to bib records.  
						
						
						
						
					}
				} else {
					logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "No files found in folder");
				}
			
				// when I've gotten to here, I have broken down all the files from this directory,
				// so now I can go ahead and write the good MARC out
				//
				// filter for date and language
				// check for ESTC numbers
				
			
			}
			
		} else {
			logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "No instituional folders found in read directory");
		}
		
	}
	
	
}
