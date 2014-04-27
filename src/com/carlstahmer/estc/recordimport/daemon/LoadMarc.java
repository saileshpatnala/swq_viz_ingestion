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
import org.marc4j.marc.Subfield;

/**
 * <p>A class that contains methods for loading mars files from the listen directory,
 * parsing data, and inserting it into the working database.</p>
 */
public class LoadMarc {
	
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
	public LoadMarc(Conf configObj, SqlModel sqlModObj) {
		config = configObj;
		sqlObj = sqlModObj;
		logger = new Logger(config);
	}
	
	/**
	 * <p>Loads a marc file from the file system and inserts the data into the working, local
	 * mySql database.</p>
	 *
	 * @param  strFile    	The full file path to the marc file to load
	 * @param  curCode  	The MARC institutional code for the organization that created the record
	 */
	public void loadMarcFile(String strFile, String curCode) {
		
		try {
			
			// instantiate file objects
			File fileInfo = new File(strFile);
			long fileModDate = fileInfo.lastModified();
			String fileName = fileInfo.getName();
			logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Processing File "+fileName+" Last Modified "+fileModDate);
			logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Current Institutional Code "+curCode);
			
			// create or load SQL file record
			// selectFileRecord(curCode, fileName, fileModDate)
			sqlObj.openConnection();
			if (sqlObj.connOpen) {
				logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Sql connection opened");
				int fileRecordId = sqlObj.selectFileRecord(curCode, fileName);	
				boolean newFile = false;
				if (fileRecordId == 0) {
					newFile = true;
					fileRecordId = sqlObj.insertFileRecord(curCode, fileName, fileModDate, 1);
					logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "New file entered in system with ID " + fileRecordId);
				} else {
					logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "File already exists in system with ID " + fileRecordId );
				}				
				
				
				
				
				InputStream input = new FileInputStream(strFile);
				MarcReader reader = new MarcStreamReader(input);
				boolean blnHasControlIdent = false;
		        while (reader.hasNext()) {
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
		            	if (thisControl.getTag().equals("003")) {
		            		blnHasControlIdent = true;
		            	}
		            }
		            
		            // Now check and see if this record already exists if we got a control key
		            if (strControlNumKey.length() > 0) {
		            	int intRecordId = sqlObj.selectRecordRecord(curCode, recType, strControlNumKey);
		            	if (intRecordId > 0) {
		            		// this is an existing record
		            		// if I'm here, I need to delete all the fields associated with this record
		            		// and mark the record as unprocessed
		            		
		            		logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "This is an existing record--deleting all asssociated fields and subfields and marking record as unprocessed");
		      
		            		boolean setUnprocessed = false;
		            		
		            		// get all associated fields
		            		ArrayList<Integer> assocFields = sqlObj.selectAssocfieldIds(intRecordId);
		            		
		            		// delete all subfields associated with each field
		            		for (int fieldId : assocFields) {
		            			boolean subfielddel = sqlObj.deleteSubFields(fieldId);
		            			if (subfielddel) {
		            				logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Subfield deletion successfull"); 
		            			} else {
		            				logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Subfield deletion faild"); 
		            			}
		            		}
		            		
		            		// now delete the associated fields
		            		boolean delFields = sqlObj.deleteRecordFields(intRecordId);
		            		if (delFields) {
		            			setUnprocessed = sqlObj.setRecordRecordUnProcessed(intRecordId);
		            		}
		            		
		            		if (setUnprocessed) {
		            			logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Existing Record "+intRecordId+" Data Reset and Set as Unprocessed"); 
		            		} else {
		            			logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Error deleting record data for record "+intRecordId); 
		            		}
		            		
		            		
		            	} else {
		            		//this is a new record so I need to insert it
		            		
		            		intRecordId = sqlObj.insertRecordRecord(fileRecordId, recType, strControlNumKey);
		            		logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Inserted New Record With Control Number "+strControlNumKey+" and System ID "+intRecordId); 
		            	}
		            	
		            	// now here I write all of the field data for the record to the system
		            	logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Writing new data for record "+intRecordId); 

		            	List<ControlField> controlFieldsAll = record.getControlFields();
			            for (int ivf=0;ivf<controlFieldsAll.size();ivf++) {
			            	ControlField thisControlField = controlFieldsAll.get(ivf);
			            	String thisControlTag = thisControlField.getTag();
			            	String thisControlData = thisControlField.getData();
			            	int fieldId = sqlObj.insertFieldRecord(intRecordId, thisControlTag, thisControlData, 1);	
	            			if (fieldId > 0) {
	            				logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Successfully saved field with id "+fieldId); 

	            			} else {
	            				logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Error saving field"); 
	            			}
			            }
			            // add a 003 control identifer if there isn't one
			            if (!blnHasControlIdent) {
			            	int insertControlIdent = sqlObj.insertFieldRecord(intRecordId, "003", curCode, 1);
	            			if (insertControlIdent > 0) {
	            				logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Successfully set missing institutional code in 003 field"); 
	            			} else {
	            				logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Error setting missing institutional code in 003 field"); 
	            			}
			            }
			            
			            List<DataField> dataFieldsAll = record.getDataFields();
			            for (int idf=0;idf<dataFieldsAll.size();idf++) {
			            	DataField thisDataField = dataFieldsAll.get(idf);
			            	String thisDataTag = thisDataField.getTag();
			            	String fieldToString = thisDataField.toString();
			            	int datafieldId = sqlObj.insertFieldRecord(intRecordId, thisDataTag, fieldToString, 2);
			            	
			            	// Handle Subfileds. 
			            	List<Subfield> subFields = thisDataField.getSubfields();
			            	if (subFields.size() > 0) {
					            for (int isf=0;isf<subFields.size();isf++) {
					            	Subfield thisSubfield = subFields.get(isf);
					            	String thisSubfieldTag = Character.toString(thisSubfield.getCode());
					            	String thisSubfieldData = thisSubfield.getData();
					            	int intSubfieldId = sqlObj.insertSubfieldRecord(datafieldId, thisSubfieldTag, thisSubfieldData);
			            			if (intSubfieldId > 0) {
			            				logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Successfully saved subfield with id "+intSubfieldId); 
			            			} else {
			            				logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Failed to save subfield"); 
			            			}
					            }
			            	}
			            }
		            		
		            	
		            } else {
		            	logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Unable to process record due to missing or blank conrole field [001]");
		            }

		        }
		        
		        
		        if (!newFile) {
		        	// update file modification date for file in db so that it reflects
		        	// the latest mod date on system correctly
		        	boolean dateUpdated = sqlObj.updateFileModDate(fileRecordId, fileModDate);
		        	if (dateUpdated) {
		        		logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Updated modification date of file "+fileRecordId+" to "+fileModDate);
		        	} else {
		        		logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Unable to update modification date of file "+fileRecordId+" with system modification date of "+fileModDate);
		        	}
	
		        }
				
				sqlObj.closeConnection();
				if (!sqlObj.connOpen) {
					logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Sql connection successfully closed");
				} else {
					logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Failed to close sql connection");
				}
			} else {
				logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "SQL connection not open");
			}
			
		} catch (FileNotFoundException e) {
			logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Failed to load MARC file");
		}
		
	}
	
	
	
}
