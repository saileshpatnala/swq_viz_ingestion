package com.carlstahmer.estc.recordimport.daemon;

import java.io.FileInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;

import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Record;
import org.marc4j.marc.ControlField;


public class LoadMarc {
	
	Conf config;
	SqlModel sqlObj;

	public LoadMarc(Conf configObj, SqlModel sqlModObj) {
		config = configObj;
		sqlObj = sqlModObj;
	}
	

	public void loadMarcFile(String strFile, String curCode) {
		
		RecordsCollectionBib bibRecords = new RecordsCollectionBib();
		RecordsCollectionHolding holdingRecords = new RecordsCollectionHolding();
		
		
		boolean connOpened;
		boolean connClosed;
		
		try {
			
			// instantiate file objects
			File fileInfo = new File(strFile);
			long fileModDate = fileInfo.lastModified();
			String fileName = fileInfo.getName();
			System.out.println("Filename: " + fileName);
			System.out.println("fileModDate: " + fileModDate);
			System.out.println("curCode: " + curCode);
			
			// create or load SQL file record
			// selectFileRecord(curCode, fileName, fileModDate)
			connOpened = sqlObj.openConnection();
			if (connOpened) {
				System.out.println("Sql connection initiated.");
				int fileRecordId = sqlObj.selectFileRecord(curCode, fileName, fileModDate);
				
				if (fileRecordId == 0) {
					fileRecordId = sqlObj.insertFileRecord(curCode, fileName, fileModDate, 1);
					System.out.println("New file entered in system with ID " + fileRecordId + ".");
				} else {
					System.out.println("File already exists in system with ID " + fileRecordId + ".");
				}
				
				connClosed = sqlObj.closeConnection();
				if (connClosed) {
					System.out.println("Sql connection successfully closed.");
				} else {
					System.out.println("Failed to close sql connection.");
				}
			} else {
				System.out.println("Failed to initiate sql connection.");
			}
			
			
			InputStream input = new FileInputStream(strFile);
			MarcReader reader = new MarcStreamReader(input);
	        while (reader.hasNext()) {
	        	//System.out.println("Checking Record "+intRecordNum);
	            Record record = reader.next();
	            List<ControlField> controlFields = record.getControlFields();
	            String strControlNumKey = "";
	            int recType = 1;
	            for (int i=0;i<controlFields.size();i++) {
	            	ControlField thisControl = controlFields.get(i);
	            	if (thisControl.getTag().equals("004")) {
	            		recType = 2;
	            	}
	            	if (thisControl.getTag().equals("001")) {
	            		strControlNumKey = thisControl.getData();
	            	}
	            }
	            
	            // Now check and see if this record already exists if we got a control key
	            if (strControlNumKey.length() > 0) {
	            	int intRecordId = sqlObj.selectRecordRecord(curCode, recType, strControlNumKey);
	            	if (intRecordId > 0) {
	            		// this is an existing record
	            	} else {
	            		//this is a new record
	            		
	            	}
	            } else {
	            	System.out.println("Unable to process record due to missing or blank conrole field [001].");
	            }

	        }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			System.out.println("Failed to load MARC file");
		}
		
	}
	
	private HashMap<String, String> addRecordToCollection(Record workingRecord) {
		List<ControlField> controlFields = workingRecord.getControlFields();
		List<DataField> dataFields = workingRecord.getDataFields();
		HashMap<String, String> recordsHash = new HashMap<String, String>();
        for (int i=0;i<controlFields.size();i++) {
        	ControlField thisControl = controlFields.get(i);
        	recordsHash.put(thisControl.getTag(), thisControl.getData());
        }
        for (int i=0;i<dataFields.size();i++) {
        	DataField thisDataField = dataFields.get(i);
        	recordsHash.put(thisDataField.getTag(), thisDataField.toString());
        }
        return recordsHash;
	}
	
	
	
}
