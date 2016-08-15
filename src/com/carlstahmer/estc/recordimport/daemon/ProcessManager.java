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

import java.io.File;


/**
 * @author cstahmer
 * 
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
		
		System.out.println("I'm in runOnce");
		
// importer part	
/*		
		// This part of the code imports the marc
		
		FileUtils fileUts = new FileUtils(config, sqlObj);
		fileUts.listFoldersRecursive();
		if (fileUts.directoryList.size() > 0) {
			
			System.out.println("Found Files");
			
			for (int id=0;id<fileUts.directoryList.size();id++) {
				String curCode = fileUts.directoryList.get(id);
				//if (Arrays.asList(config.estcCodes).contains(curCode)) {
					curCode = "estcstar";
				//}
				String curDir = config.listenDir+"/"+curCode;
				logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Working in directory: "+curDir);
				fileUts.listFilesForFolder(curDir);
				if (fileUts.fileList.size() > 0) {
					
					for (int i=0; i < fileUts.fileList.size(); i++) {

						// construct a filename
						String fileToProcess = curDir + "/" + fileUts.fileList.get(i);
						
						System.out.println("File" + fileToProcess);
						
						// instantiate file object to get last processed date
						File fileInfo = new File(fileToProcess);
						long fileModDate = fileInfo.lastModified();

						// now check to see if this is existing file that has not been modified.
						// create or load SQL file record
						// selectFileRecord(curCode, fileName, fileModDate)
						logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Sql connection opened");
						logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Checking whether file has been previously processed");
						int fileRecordId = sqlObj.selectFileRecordStrict(curCode, fileUts.fileList.get(i), fileModDate);
						if (fileRecordId == 0) {

							String fileName = fileInfo.getName();
							logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Processing File "+fileName+" Last Modified "+fileModDate);
							logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Current Institutional Code "+curCode);
							
							// create or load SQL file record
							logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Sql connection opened");
							int dupFileRecordId = sqlObj.selectFileRecord(curCode, fileName);	
							boolean newFile = false;
							if (dupFileRecordId == 0) {
								newFile = true;
								fileRecordId = sqlObj.insertFileRecord(curCode, fileName, fileModDate, 1);
								logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "File " + fileName + " entered in system as new file with ID " + fileRecordId);
							} else {
								fileRecordId = dupFileRecordId;
								logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "File " + fileName + " already exists in system with ID " + fileRecordId + ".  Timestamp will be updated at conclusion of record processing." );
							}
							
							// get a file type and process accordingly
							int intFileType = fileUts.fileType(fileUts.fileList.get(i));
							
							if (intFileType == 1) {
								// this is a marc file
								logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Processing MARC file "+fileToProcess);
								LoadMarc myMarcInstance = new LoadMarc(config, sqlObj);
								myMarcInstance.loadMarcFile(fileToProcess, curCode, fileRecordId);
								
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
							
							if (!newFile) {
					        	// update file modification date for file in db so that it reflects
					        	// the latest mod date on system correctly
					        	boolean dateUpdated = sqlObj.updateFileModDate(fileRecordId, fileModDate);
					        	if (dateUpdated) {
					        		logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Updated modification date of file " + fileName + " with system ID "+fileRecordId+" to "+fileModDate);
					        	} else {
					        		logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Unable to update modification date of file "+fileRecordId+" with system modification date of "+fileModDate);
					        	}

					        }
							
							
							
						
						} else {
							logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Skipping file " + fileUts.fileList.get(i) + " -- already exists in system with last modification date of " + fileModDate + " -- file ID " + fileRecordId);
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
			
			}
			
		} else {
			System.out.println("Didn't find any files");
			logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "No instituional folders found in read directory");
		}
		
		System.out.println("End of Import");
		// THIS IS THE END OF THE IMPORT PART


// IMPORTER TEMPORARY COMMENT OUT END
*/
		
		// NEXT COMES THE OUTPUT PART
		

		// when I've gotten to here, I have broken down all the files from this directory,
		// so now I can go ahead and write the good MARC out
		//
		// filter for date and language
		// check for ESTC numbers
		
		System.out.println("I'm here");
		
		// The scope checker code below has been checked and is working
		// ScopeChecker myScopeCheck = new ScopeChecker(config, sqlObj);
		// boolean thisScopeCheck = myScopeCheck.applyScopeFilter();
		
		ExportRDF rdfExporter = new ExportRDF(config, sqlObj);
		rdfExporter.makeRDFAllBibs("estc.bl.uk");
		
		
		// MergeHoldings myMergeHoldings = new MergeHoldings(config, sqlObj);
		// boolean thisMergeHoldings = myMergeHoldings.doMerge();
		
		//System.out.println("And now I'm here");
		
		
	}
	
	
}
