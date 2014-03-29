package com.carlstahmer.estc.recordimport.daemon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;

public class SqlModel {
	
	public Connection conn = null;
	String dbserver;
	String dbname;
	String dbuser;
	String dbpass;
	
	public SqlModel(Conf config) {
		dbserver = config.dbserver;
		dbname = config.dbname;
		dbuser = config.dbuser;
		dbpass = config.dbpass;
	}
	
	public boolean openConnection() {
		
		try {
			
			String strConnString = "jdbc:mysql://"+dbserver+"/"+dbname+"?user="+dbuser+"&password="+dbpass;
	        conn = DriverManager.getConnection(strConnString);
			return true;
	    } catch (SQLException ex) {
	        // handle any errors
	        System.out.println("SQLException: " + ex.getMessage());
	        System.out.println("SQLState: " + ex.getSQLState());
	        System.out.println("VendorError: " + ex.getErrorCode());
	        return false;
	    }
		
	}
	
	public boolean closeConnection() {
		try {
			conn.close();
			return true;
		} catch (SQLException ex) {
			return false;
		}
		
		
	}
	
	public int selectFileRecord(String currCode, String fileName, long modDate) {
		
		// initialize required objects
		int recordId = 0;
		Statement stmt = null;
		ResultSet resultSet = null;
		
		// define query
		String strSql = "SELECT id FROM files" +
				" WHERE institution_code LIKE '" + currCode +
				"' AND filename LIKE '" + fileName + 
				"' AND modification_date LIKE '" + String.valueOf(modDate) + "';";
		
		// run query
		try {
			
			stmt = conn.createStatement();
	        resultSet = stmt.executeQuery(strSql);
	        if (resultSet.next()) {
	        	recordId = resultSet.getInt("id");
	        }
	        try {
	        	resultSet.close();
	        } catch (SQLException sqlEx) { } // ignore
		    
		
		} catch (SQLException ex){
		    // handle any errors
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
		    try {
		    	stmt.close();
		    } catch (SQLException sqlEx) { } // ignore

		}		
		return recordId;
	}
	
	
	public int insertFileRecord(String currCode, String fileName, long modDate, int fileType) {
		
		// initialize required objects
		int recordId = 0;
		Statement stmt = null;
		ResultSet rs = null;
		
		// define query
		String strSql = "INSERT INTO files " +
				"(institution_code, filename, modification_date, type)" +
				" VALUES" + 
				" ('" + currCode + "', '" + fileName + "', " + modDate + ", " + fileType + ");";

		try {

		    stmt = conn.createStatement();
		    stmt.executeUpdate(strSql);
		    rs = stmt.executeQuery("SELECT LAST_INSERT_ID()");
		    if (rs.next()) {
		    	recordId = rs.getInt(1);
		    }
		    rs.close();
		    
		} catch (SQLException ex){
			    // handle any errors
			    System.out.println("SQLException: " + ex.getMessage());
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
		return recordId;
	}
	
	
	

	public int selectRecordRecord(String currCode, int recType, String controlIdentifier) {
		
		// initialize required objects
		int recordId = 0;
		Statement stmt = null;
		ResultSet resultSet = null;
		
		// define query
		String strSql = "SELECT records.id FROM records" +
				" JOIN files ON files.id = records.file_id" +
				" WHERE files.institution_code LIKE '" + currCode +
				"' AND records.control_identifier LIKE '" + controlIdentifier + 
				"' AND records.type LIKE '" + String.valueOf(recType) + "';";
		
		// run query
		try {
			
			stmt = conn.createStatement();
	        resultSet = stmt.executeQuery(strSql);
	        if (resultSet.next()) {
	        	recordId = resultSet.getInt("id");
	        }
	        try {
	        	resultSet.close();
	        } catch (SQLException sqlEx) { } // ignore
		    
		
		} catch (SQLException ex){
		    // handle any errors
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
		    try {
		    	stmt.close();
		    } catch (SQLException sqlEx) { } // ignore

		}		
		return recordId;
	}	
	
	
	
	public int insertRecordRecord(String fileId, String recType, String controlIdentifier) {
		
		// initialize required objects
		int recordId = 0;
		Statement stmt = null;
		ResultSet rs = null;
		
		// define query
		String strSql = "INSERT INTO files " +
				"(institution_code, filename, modification_date, type)" +
				" VALUES" + 
				" ('" + currCode + "', '" + fileName + "', " + modDate + ", " + fileType + ");";

		try {

		    stmt = conn.createStatement();
		    stmt.executeUpdate(strSql);
		    rs = stmt.executeQuery("SELECT LAST_INSERT_ID()");
		    if (rs.next()) {
		    	recordId = rs.getInt(1);
		    }
		    rs.close();
		    
		} catch (SQLException ex){
			    // handle any errors
			    System.out.println("SQLException: " + ex.getMessage());
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
		return recordId;
	}

}
