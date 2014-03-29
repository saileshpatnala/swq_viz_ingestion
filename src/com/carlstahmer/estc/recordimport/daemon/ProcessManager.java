package com.carlstahmer.estc.recordimport.daemon;

public class ProcessManager {
	
	Conf config;
	SqlModel sqlObj;

	public ProcessManager(Conf configObj, SqlModel sqlModObj) {
		config = configObj;
		sqlObj = sqlModObj;
	}
	
	
	public void runServer() {
		// this should setup a kid of constantly running process that re-calls runOnce() 
		// continuously waiting the designated runInterval between each execution
		
		
		
	}
	
	
	public void runOnce() {
		
		// see code in current_mac_working_folder/estc_java_oldcode.txt for rest of process code
		FileUtils fileUts = new FileUtils();
		fileUts.listFolders(config.listenDir);
		if (fileUts.directoryList.size() > 0) {
			for (int id=0;id<fileUts.directoryList.size();id++) {
				String curCode = fileUts.directoryList.get(id);
				String curDir = config.listenDir+"/"+curCode;
				System.out.println("Processing institutional directory: "+curDir);
				fileUts.listFilesForFolder(curDir);
				if (fileUts.fileList.size() > 0) {
					
					for (int i=0; i < fileUts.fileList.size(); i++) {
						
						// get a file type and process accordingly
						int intFileType = fileUts.fileType(fileUts.fileList.get(i));	
						
						if (intFileType == 1) {
							// this is a marc file
							
							String marcToProcess = curDir + "/" + fileUts.fileList.get(i);
							System.out.println("Processing MARC file "+marcToProcess);
							LoadMarc myMarcInstance = new LoadMarc(config, sqlObj);
							myMarcInstance.loadMarcFile(marcToProcess, curCode);
							
						} else if (intFileType == 2) {
							// this is a text file
							
						} else if (intFileType == 3) {
							// this is an excel file
							
						} else if (intFileType == 4) {
							// this is a csv file
							
						} else if (intFileType == 5) {
							// this is an xml file
							
						} else {
							// this is an unrecognized file
						}
					}
				} else {
					System.out.println("The institutional folder is empty.");
				}
			
			
			
			}
			
		} else {
			System.out.println("No institution folders found.\nAborting operation!");
		}
		
	}
	
	
}
