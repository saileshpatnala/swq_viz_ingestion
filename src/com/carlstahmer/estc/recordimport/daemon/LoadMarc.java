package com.carlstahmer.estc.recordimport.daemon;

import java.io.FileInputStream;
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
	RecordUtils recs;
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
		recs = new RecordUtils(config, sqlObj);
		logger = new Logger(config);
	}
	
	/**
	 * <p>Loads a marc file from the file system and inserts the data into the working, local
	 * mySql database.</p>
	 *
	 * @param  strFile    	The full file path to the marc file to load
	 * @param  curCode  	The MARC institutional code for the organization that created the record
	 */
	public void loadMarcFile(String strFile, String curCode, int fileRecordId) {
		
		try {			
			
			InputStream input = new FileInputStream(strFile);
			MarcReader reader = new MarcStreamReader(input);
			boolean blnHasControlIdent = false;
	        while (reader.hasNext()) {
	            Record record = reader.next();
	            List<ControlField> controlFields = record.getControlFields();
	            String strControlNumKey = "";
	            String strLastChange = "";
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
	            	if (thisControl.getTag().equals("005")) {
	            		strLastChange = thisControl.getData();
	            	}
	            }
	            
	            if (strLastChange.length() < 1) {
	            	strLastChange = "0";
	            }
	            double moddate;
	            moddate = Double.parseDouble(strLastChange);

	            
	            // Now check and see if this record already exists if we got a control key
	            if (strControlNumKey.length() > 0) {
	            	
	            	int intRecordId = recs.duplicateRecordCheck(curCode, strControlNumKey, moddate, recType);
	            	
	            	if (intRecordId > -1) {
	            	
	            	
		            	if (intRecordId == 0) {
		            		intRecordId = sqlObj.insertRecordRecord(fileRecordId, recType, strControlNumKey, moddate);
		            		logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Inserted New Record With Control Number "+strControlNumKey+" and System ID "+intRecordId); 
		            	} else {
		            		logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Modifying existing record with system ID "+intRecordId);
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
			            	if (datafieldId > 0) {
			            		logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Successfully inserted Field " + thisDataTag + " into database with ID " + datafieldId); 
			            	} else {
			            		logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Failed to insert Field " + thisDataTag + " into database."); 
			            	}
			            	
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
	            		String skipRecType = "holding";
	            		if (recType == 1) {
	            			skipRecType = "bibliographic";
	            		}
	            		logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Skipping duplicate " + skipRecType + " record with control " + strControlNumKey + " and modification datetimestamp " + moddate);
	            	}
	            	
	            } else {
	            	logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Unable to process record due to missing or blank conrole field [001]");
	            }

	        }
	        	
		} catch (FileNotFoundException e) {
			logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Failed to load MARC file");
		}
		
	}

}
