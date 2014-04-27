package com.carlstahmer.estc.recordimport.daemon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;


/**
 * <p>A class that acts as the SQL query model for the application.  All SQL queries 
 * for the entire application are constructed here.  There should be no SQL anywhere 
 * else in the application.</p>
 */
public class SqlModel {
	
	public Connection conn = null;
	public boolean connOpen = false;
	String dbserver;
	String dbname;
	String dbuser;
	String dbpass;
	
	/**
	 * <p>Constructor class that assigns values from passed Config object 
	 * to local variables needed to communicate with the database.</p>
	 *
	 * @param  config    an instance of the Conf class
	 */
	public SqlModel(Conf config) {
		dbserver = config.dbserver;
		dbname = config.dbname;
		dbuser = config.dbuser;
		dbpass = config.dbpass;
	}
	
////////////////////////////
// Open and Close Methods //
////////////////////////////
	
	/**
	 * <p>Opens a SQL Connection.</p>
	 */
	public boolean openConnection() {
		
		try {
			
			String strConnString = "jdbc:mysql://"+dbserver+"/"+dbname+"?user="+dbuser+"&password="+dbpass;
	        conn = DriverManager.getConnection(strConnString);
	        connOpen = true;
			return true;
	    } catch (SQLException ex) {
	        // handle any errors
	        System.out.println("SQLException SqlModel.java openConnection(): " + ex.getMessage());
	        System.out.println("SQLState: " + ex.getSQLState());
	        System.out.println("VendorError: " + ex.getErrorCode());
	        return false;
	    }
		
	}
	
	/**
	 * <p>Closes a SQL Connection.</p>
	 */
	public boolean closeConnection() {
		try {
			conn.close();
			connOpen = false;
			return true;
		} catch (SQLException ex) {
			return false;
		}
		
		
	}
	
/////////////////////////
// Data Model Methods //
////////////////////////
	
	/**
	 * <p>Returns a file type id based upon a filename suffix 
	 * and mapping provided in database.</p>
	 *
	 * @param  	suffix		the suffix to lookup
	 * @return				A file_types ID from the db.  Returns 0 if not found.
	 */
	public int selectFileTypeID(String suffix) {
		String strSql = "SELECT file_type_id FROM file_type_suffixes WHERE suffix LIKE '" + suffix + "'";
		int fileTypeID = qSelectInt(strSql);
		return fileTypeID;
	}
	
	/**
	 * <p>Selects a "file" record from the database.  Checks the institutional code, 
	 * filename, and file system file modification date to see if this is a file that 
	 * has already been processed.  If it finds a record for this file, it returns 
	 * the file list id from the database.  If it does not find the file in the database,
	 * it returns 0.</p>
	 *
	 * @param  	currCode	the MARC institutional code for the creator of the file
	 * @param  	fileName  	the filename of the file
	 * @param	modDate		the system last modified time stamp of the file
	 * @return				A file ID if found or 0 if not found
	 */
	public int selectFileRecordStrict(String currCode, String fileName, long modDate) {
		String strSql = "SELECT id FROM files" +
				" WHERE institution_code LIKE '" + currCode +
				"' AND filename LIKE '" + fileName + 
				"' AND modification_date >= " + String.valueOf(modDate) + ";";
		int recordId = qSelectInt(strSql);	
		return recordId;
	}
	
	/**
	 * <p>Selects a "file" record from the database.  Checks the institutional code
	 * and filename only to see if this is a file that is already in the system.  Ignores
	 * file modification date.  Simply a check of filename and institution.  If it does 
	 * not find the file in the database it returns 0.</p>
	 *
	 * @param  	currCode	the MARC institutional code for the creator of the file
	 * @param  	fileName  	the filename of the file
	 * @return				A file ID if found or 0 if not found
	 */
	public int selectFileRecord(String currCode, String fileName) {
		String strSql = "SELECT id FROM files" +
				" WHERE institution_code LIKE '" + currCode +
				"' AND filename LIKE '" + fileName + "';";
		int recordId = qSelectInt(strSql);	
		return recordId;
	}
	
	
	/**
	 * <p>Inserts a new File record into the database.</p>
	 *
	 * @param  	currCode	the MARC institutional code for the creator of the file
	 * @param  	fileName  	the filename of the file
	 * @param	modDate		the system last modified time stamp of the file
	 * @param	fileType	1=marc, 2=csv, 3=text, 4=excel
	 * @return				A file ID if found or 0 if not found
	 */
	public int insertFileRecord(String currCode, String fileName, long modDate, int fileType) {
		// define query
		String strSql = "INSERT INTO files " +
				"(institution_code, filename, modification_date, type)" +
				" VALUES" + 
				" ('" + currCode + "', '" + fileName + "', " + modDate + ", " + fileType + ");";
		int recordId = qInsert(strSql);
		return recordId;
	}
	
	/**
	 * <p>Updates the system last modified date of a file record.</p>
	 *
	 * @param  	strFileId		the file id to update
	 * @param  	fileModDate  	the new mod date
	 * @return				1 = success, 0 = failure
	 */
	public boolean updateFileModDate(int intFileId, long fileModDate) {
		String strSql = "UPDATE files " +
				"SET modification_date = " + fileModDate +
				" WHERE id = " + intFileId+ ";";
		boolean retFlag = qUpdate(strSql);
		return retFlag;
	}	
	
	/**
	 * <p>Selects a record from the records table and returns the id.  Records are
	 * matched on the institutional code, the type of record, and the control identifier.</p>
	 *
	 * @param  	currCode			the MARC institutional code for the creator of the file
	 * @param  	recType  			the type of record.  1 = bib, 2 = holding
	 * @param	controlIdentifier	the record control identifier
	 * @return						A record id.  Returns 0 if no match found.
	 */
	public int selectRecordRecord(String currCode, int recType, String controlIdentifier) {
		String strSql = "SELECT records.id FROM records" +
				" JOIN files ON files.id = records.file_id" +
				" WHERE files.institution_code LIKE '" + currCode +
				"' AND records.control_identifier LIKE '" + controlIdentifier + 
				"' AND records.type LIKE '" + String.valueOf(recType) + "';";
		int recordId = qSelectInt(strSql);		
		return recordId;
	}	
	
	/**
	 * <p>Inserts a marc-record record entry into the records table.</p>
	 *
	 * @param  	fileId				the id from the files table for the file that the marc record was read from
	 * @param  	recType  			the type of record.  1 = bib, 2 = holding
	 * @param	controlIdentifier	the record control identifier
	 * @return						The record if of the inserted record.  Returns 0 on failure.
	 */
	public int insertRecordRecord(int fileId, int recType, String controlIdentifier) {
		String strSql = "INSERT INTO records " +
				"(file_id, control_identifier, type)" +
				" VALUES" + 
				" (" + fileId + ", '" + controlIdentifier + "', " + recType + ");";
		int recordId = qInsert(strSql);
		return recordId;
	}
	
	
	/**
	 * <p>Flags a record entry in the records table as having been processed, meaning
	 * that all of the data contained in the original marc recrod has been saved to
	 * the fields and subfields tables.  This identifies the record as suitable for 
	 * output publishing.</p>
	 *
	 * @param  	recordId	the id of the record to flag
	 * @return				true on success, false on failure
	 */
	public boolean setRecordRecordProcessed(int recordId) {
		String strSql = "UPDATE records" +
				" SET processed = 1" +
				" WHERE id = " + recordId;
		boolean marked = qUpdate(strSql);
		return marked;
	}	
	

	/**
	 * <p>Flags a record entry in the records table as having not been processed, meaning
	 * that all of the data contained in the original marc recrod has yet to be saved to
	 * the fields and subfields tables.  This identifies the record as not yet ready for 
	 * output publishing.</p>
	 *
	 * @param  	recordId	the id of the record to flag
	 * @return				true on success, false on failure
	 */
	public boolean setRecordRecordUnProcessed(int recordId) {
		String strSql = "UPDATE records" +
				" SET processed = 0" +
				" WHERE id = " + recordId;
		boolean marked = qUpdate(strSql);
		return marked;
	}
	
	
	/**
	 * <p>Select all fields in the records_has_fields table that are associated 
	 * with a record from the records table.</p>
	 *
	 * @param  	recordId	the id of the record whose fields you want
	 * @return				an Array of field ids
	 */
	public ArrayList<Integer> selectAssocfieldIds(int recordId) {		
		
		// initialize required objects
		ResultSet resultSet = null;
		ArrayList<Integer> recordRows = new ArrayList<Integer>();
		
		// define query
		String strSql = "SELECT id FROM records_has_fields" +
				" WHERE record_id = " + recordId;
		
		// run query and process results
		resultSet = qSelectGeneric(strSql);
        try {
			if (resultSet.next()) {
				recordRows.add(resultSet.getInt("id"));
			}
		} catch (SQLException e) {
		    System.out.println("SQLException SqlModelJava selectAssocfieldIds: " + e.getMessage());
		    System.out.println("SQLState: " + e.getSQLState());
		    System.out.println("VendorError: " + e.getErrorCode());
		}
        
        // return results
		return recordRows;
	}
	
	
	/**
	 * <p>Select all fields in the records_has_fields table that are associated 
	 * with a record from the records table.  The return is a List of Hashes.  
	 * For each hash slice [0] = the field tag and slice [1] = the string value.</p>
	 *
	 * @param  	recordId	the id of the record whose fields you want
	 * @return				an ArrayList of Hashes. For each hash slice [0] = the field tag and slice [1] = the string value.
	 */
	public ArrayList<HashMap<String,String>> selectRecordFields(int recordId) {
				
		// initialize required objects
		ResultSet resultSet = null;
		ArrayList<HashMap<String,String>> recordRows = new ArrayList<HashMap<String,String>>();
		HashMap<String,String> recordColumns = new HashMap<String, String>();
		
		// define query
		String strSql = "SELECT records_has_fields.* FROM records_has_fields" +
				" WHERE records_has_fields.record_id = " + recordId +
				" ORDER BY records_has_fields.field";
		
		// run query and process results
		resultSet = qSelectGeneric(strSql);
        try {
	        if (resultSet.next()) {
	    		recordColumns.put("field", resultSet.getString("field"));
	    		recordColumns.put("subfield", resultSet.getString("subfield"));
	    		recordColumns.put("value", resultSet.getString("value"));
	    		recordRows.add(recordColumns);
	        }
		} catch (SQLException e) {
		    System.out.println("SQLException SqlModelJava selectRecordFields: " + e.getMessage());
		    System.out.println("SQLState: " + e.getSQLState());
		    System.out.println("VendorError: " + e.getErrorCode());
		}		
		
        // return result
		return recordRows;
	}	
	
	
	/**
	 * <p>Insert a field associated with a record into the records_has_fields table.</p>
	 *
	 * @param  	recordId	the id of the associated record
	 * @param  	fieldVal	the marc tag for the field
	 * @param  	valueVal	the data contained in the field as string
	 * @param  	fieldType	an id from the field_typeds table indicated whether a control or data field.  1 = control, 2 = data.
	 * @return				the id of the inserted field
	 */
	public int insertFieldRecord(int recordId, String fieldVal, String valueVal, int fieldType) {
		valueVal = valueVal.replace("'", "''");
		String strSql = "INSERT INTO records_has_fields " +
				"(record_id, field, value, type)" +
				" VALUES" + 
				" (" + recordId + ", '" + fieldVal + "', '" + valueVal + "', " + fieldType + ");";
		int insertId = qInsert(strSql);
		return insertId;
	}	
	
	
	/**
	 * <p>Deletes all fields associated with a designated record</p>
	 *
	 * @param  	recordId	the id of the record whose fields you want to delete
	 * @return				true on success, false on failure
	 */
	public boolean deleteRecordFields(int recordId) {
		String strSql = "DELETE FROM records_has_fields" +
				" WHERE record_id = " + recordId;
		boolean success = qUpdate(strSql);
		return success;
	}

	
	
	/**
	 * <p>Insert a subfield associated with a field into the fields_has_subfields table.</p>
	 *
	 * @param  	fieldId			the id of the associated field from the fields table
	 * @param  	subfieldTag		the marc tag for the sub-field
	 * @param  	subfieldVal		the data contained in the sub-field as string
	 * @return					the id of the inserted sub-field
	 */
	public int insertSubfieldRecord(int fieldId, String subfieldTag, String subfieldVal) {
		subfieldVal = subfieldVal.replace("'", "''");
		String strSql = "INSERT INTO fields_has_subfields " +
				"(field_id, subfield, value)" +
				" VALUES" + 
				" (" + fieldId + ", '" + subfieldTag + "', '" + subfieldVal + "');";
		int insertId = qInsert(strSql);
		return insertId;
	}
	
	
	/**
	 * <p>Deletes all sub-fields associated with a designated field</p>
	 *
	 * @param  	fieldId		the id of the field whose sub-fields you want to delete
	 * @return				true on success, false on failure
	 */
	public boolean deleteSubFields(int fieldId) {
		String strSql = "DELETE FROM fields_has_subfields" +
				" WHERE field_id = " + fieldId;
		boolean success = qUpdate(strSql);
		return success;
	}
	
	
//////////////////////
// Logging  methods //
//////////////////////
		
	/**
	 * <p>Writes a new log message to the database runlog table.</p>
	 *
	 * @param  	messageType	[1] = error, [2] = info, [3] = debug
	 * @param  	fileName  	the filename of the file initiating the message
	 * @param	lineNumber	the line number from the file where the message is initiated
	 * @param	messageText	the message
	 */
	public int insertLogMessage(int messageType, String fileName, int lineNumber, String messageText) {
		String strSql = "INSERT INTO runlog " +
				"(type, file, line, message)" +
				" VALUES" + 
				" (" + messageType + ", '" + fileName + "', " + lineNumber + ", '" + messageText + "');";
		int insertId = qInsert(strSql);
		return insertId;
	}
	
			
///////////////////////////////	
// Generic SQL query methods //
///////////////////////////////
	
	/**
	 * <p>A genreic object for querying the db for a single numeric
	 * value such as an id field, etc.  Field select list must
	 * contain only a single field.</p>
	 *
	 * @param  	strSql	A well formed SQL SELECT query with a single SELECT field of type INTEGER
	 * @return			The integer value of the return column
	 */	
	private int qSelectInt(String strSql) {
		// initialize required objects
		Statement stmt = null;
		ResultSet resultSet = null;
		int retId = 0;

		
		// run query
		try {
			stmt = conn.createStatement();
	        resultSet = stmt.executeQuery(strSql);
	        if (resultSet.next()) {
	        	retId = resultSet.getInt(1);
	        }
	        try {
	        	resultSet.close();
	        } catch (SQLException sqlEx) { } // ignore
		    
		
		} catch (SQLException ex){
		    // handle any errors
		    System.out.println("SQLException SqlModel.java qGetSingleNumericValue: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
		    try {
		    	stmt.close();
		    } catch (SQLException sqlEx) { } // ignore

		}	
		
		return retId;
	}
	
	/**
	 * <p>A genreic object for querying the db for a single String
	 * value. Field select list must contain only a single field.</p>
	 *
	 * @param  	strSql	A well formed SQL SELECT query with a single SELECT field of type STRING
	 * @return			The string value of the return column
	 */		
	private String qSelectString(String strSql) {
		// initialize required objects
		Statement stmt = null;
		ResultSet resultSet = null;
		String retString = "";

		
		// run query
		try {
			stmt = conn.createStatement();
	        resultSet = stmt.executeQuery(strSql);
	        if (resultSet.next()) {
	        	retString = resultSet.getString(1);
	        }
	        try {
	        	resultSet.close();
	        } catch (SQLException sqlEx) { } // ignore
		    
		
		} catch (SQLException ex){
		    // handle any errors
		    System.out.println("SQLException SqlModel.java qGetSingleStringValue: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
		    try {
		    	stmt.close();
		    } catch (SQLException sqlEx) { } // ignore

		}	
		
		return retString;
	}
	
	/**
	 * <p>A generic object for executing a SELECT querying 
	 * against the db.</p>
	 *
	 * @param  	strSql	A well formed SQL SELECT query
	 * @return			A resultSet object containing the result of the query
	 */	
	private ResultSet qSelectGeneric(String strSql) {
		// initialize required objects
		Statement stmt = null;
		ResultSet resultSet = null;

		
		// run query
		try {
			stmt = conn.createStatement();
	        resultSet = stmt.executeQuery(strSql);		
		} catch (SQLException ex){
		    // handle any errors
		    System.out.println("SQLException SqlModel.java qGetRecords: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
		    try {
		    	stmt.close();
		    } catch (SQLException sqlEx) { } // ignore

		}	
		
		return resultSet;
	}
	
	/**
	 * <p>A generic object for executing an UPDATE 
	 * against the db.</p>
	 *
	 * @param  	strSql	A well formed SQL SELECT query
	 * @return			A boolean value indicating success or failure
	 */		
	private boolean qUpdate(String strSql) {
		
		// initialize required objects
		boolean success = false;
		Statement stmt = null;
		
		try {

		    stmt = conn.createStatement();
		    stmt.executeUpdate(strSql);
		    success = true;   
		} catch (SQLException ex){
			    // handle any errors
			    System.out.println("SQLException SqlModel.java qInsert: " + ex.getMessage());
			    System.out.println("SQLState: " + ex.getSQLState());
			    System.out.println("VendorError: " + ex.getErrorCode());

		} finally {

		    if (stmt != null) {
		        try {
		            stmt.close();
		        } catch (SQLException ex) {
		            // ignore
		        }
		    }
		}
		return success;
	}
	
	
	/**
	 * <p>A generic object for executing an INSERT 
	 * against the db.</p>
	 *
	 * @param  	strSql	A well formed SQL SELECT query
	 * @return			A boolean value indicating success or failure
	 */
	private int qInsert(String strSql) {
		
		// initialize required objects
		Statement stmt = null;
		ResultSet rs = null;
		int insertId = 0;

		try {

		    stmt = conn.createStatement();
		    stmt.executeUpdate(strSql);
		    rs = stmt.executeQuery("SELECT LAST_INSERT_ID()");
		    if (rs.next()) {
		    	insertId = rs.getInt(1);
		    }
		    rs.close();
		    
		} catch (SQLException ex){
			    // handle any errors
			    System.out.println("SQLException SqlModel.java insertLogMessage: " + ex.getMessage());
			    System.out.println("SQLState: " + ex.getSQLState());
			    System.out.println("VendorError: " + ex.getErrorCode());

		} finally {

		    if (stmt != null) {
		        try {
		            stmt.close();
		        } catch (SQLException ex) {
		            // ignore
		        }
		    }
		}
		return insertId;
	}
	
}
