/**
 *	<p>Copyright (c) 2014, Carl Stahmer - <a href="http://www.carlstahmer.com">www.carlstahmer.com</a>.</p>
 *	
 *	<p>This file is part of the ESTC Record Importer package, a server 
 *	daemon that processes incoming MARC cataloging data stored in binary
 *	MARC, .csv, and .txt formats, checks the records for scope on date,
 *	language, and place of publication, and the makes the filtered
 *	records available to other services via OAI-PMH.</p>
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
 *	the Andrew W. Mellon Foundation.</p>
 */
package com.carlstahmer.estc.recordimport.daemon;

import java.util.ArrayList;

/**
 * @author cstahmer
 * 
 * <p>Class for checking records that have already been imported
 * into the database to make sure that they are in scope for 
 * date, language, and country of origin.</p>
 * 
 * <p>Loops through all records on the "records" table where
 * records.processed == 0 and performs various checks on the
 * record to insure that it is an in-scope record.  If the
 * record is in scope, then records.processed is set to 1.
 * If the record is not in scope, then it is deleted.</p>
 *
 */
public class ScopeChecker {
	
	Conf configObj;
	SqlModel sqlObj;
	Logger logger;

	/**
	 * <p>Constructor class that assigns values from passed Config object 
	 * to local variables needed to communicate with the database.</p>
	 *
	 * @param  config    an instance of the Conf class
	 * 	 * @param  sqlModObj  	an instance of the sqlModel class
	 */
	public ScopeChecker(Conf config, SqlModel sqlModObj) {
		configObj = config;
		sqlObj = sqlModObj;
		logger = new Logger(config);
	}
	
	
	/**
	 * <p>EMI-8</p>
	 * 
	 * <p>Create a controller method applyScopeFilter(int recordId) 
	 * method on FilterRecords.java class. When called, this will do the following:</p>
	 * 
	 * <p>1. SELECT all records where scoped = 0</p>
	 * 
	 * <p>2. For each record, run the date scope test. If not pass, delete, if pass, then continue.</p>
	 * 
	 * <p>3. Run the regions and language scope tests. If either of these passes true, then mark the 
	 * record as processed. If neither passes true, then delete the record and all associated fields.</p>
	 * 
	 * <p>Remember that when you delete you need to add all IDs to the recycle-bin. I believe that 
	 * there are already delete functions in the RecordUtils.class that you send an id to and they 
	 * correctly handle the delete, adding things to recycle bin, etc.</p>
	 *
	 */
	public boolean applyScopeFilter() {
		boolean ret = false;
		
		// first, do a select to get all records where scoped = 0 and load the ID's
		// into a list
		ArrayList<Integer> recordsToCheck = sqlObj.selectRecordsToScope();
		if (recordsToCheck.size() > 10) {
			
			// This for loop temprarily limits
			// the number of records being tested.
			// When testing is complete, remove the
			// for (keeping all enclosed code)
			// to run on all records that should be processed
			for (int i=0;i<10;i++) {
				
				boolean languageCheck = languageScope(configObj.languageScope, recordsToCheck.get(i));
				
				if (languageCheck) {
					System.out.println("Record " + recordsToCheck.get(i) + " passed record check.");
				} else {
					System.out.println("Record " + recordsToCheck.get(i) + " failed record check.");
				}
				
			// end tem for loop bracket
			}
		}
		return ret;
	}
	
	
	/**
	 * <p>EMI-7</p>
	 * 
	 * <p>Create a dateScope(int recordID) method that checks a record to see if it is in 
	 * scope. MARC fields to check are:</p>
	 * 
	 * <p><ul>
	 * <li>008</li>
	 * <li>130</li>
	 * <li>240</li>
	 * <li>260</li>
	 * <li>362</li>
	 * <li>500 (this one will be a fuzzy search for anything imbedded in the note that looks 
	 * like a date within the appropriate range. If it finds one, it will assume that the 
	 * record is at least worth having a human look at.)</li>
	 *
	 * @param  recordID    the records.id from the db for the record to check
	 */
	public boolean dateScope(int recordID) {
		boolean ret = false;
		
		return ret;
	}
	
	
	
	/**
	 * <p>EMI-5</p>
	 * 
	 * <p>Create a languageScope(String language, int recordID) class that checks the language 
	 * of the object being described to see if it is in scope. Fields to look in are:</p>
	 * 
	 * <p>008 (Bib=35-37, Hold=22-24), 041</p>
	 * 
	 * @param	langCodes	a list of in-scope MARC language codes.  See http://www.loc.gov/marc/languages/language_code.html 
	 * @param  	recordID    the records.id from the db for the record to check
	 */
	public boolean languageScope(ArrayList<String> langCodes, int recordID) {
		boolean ret = false;
		
		// TODO: START WORKING HERE
		String langString;
		String zeroZeroEight = sqlObj.getZeroZeroEight(recordID);
		int recordType = sqlObj.getRecordType(recordID);
		if (recordType == 1) {
			if (zeroZeroEight.length() > 36) {
				langString = extractCharacters(zeroZeroEight, 35, 37);
				ret = matchLangScope(langCodes, langString);
				System.out.println("Bib Language: " + langString);
			}		
		} else if (recordType ==2) {
			if (zeroZeroEight.length() > 23) {
				langString = extractCharacters(zeroZeroEight, 22, 24);
				ret = matchLangScope(langCodes, langString);
				System.out.println("Holding Language: " + langString);
			}
		} else {
			if (configObj.liberal) {
				ret = true;
			}
		}
		if (ret) {
			System.out.println("This is a match!");
		} else {
			System.out.println("This is not a match!");
		}
		return ret;
	}	
	
	/**
	 * <p>EMI-6</p>
	 * 
	 * <p>Create pubPlaceScope(int recordID) method on FilterRecords.java Class.  Fields 
	 * to look in are:</p>
	 * 
	 * <p>008 (Bib=15-17, Hold=N/A), 260, 043(?), 044, and 752</p>
	 *
	 * @param  recordID    the records.id from the db for the record to check
	 */	
	public boolean pubPlaceScope(int recordID) {
		boolean ret = false;
		
		return ret;
	}	
	
	/**
	 * <p>Retrieve a subsection of a string between designated positions.
	 * 1 = first character in string.</p>
	 *
	 * @param  	haystack    the string to search
	 * @param	startIndex	the position of the first character to include
	 * @param	endIndex	the position of the last character to include
	 */	
	public String extractCharacters(String haystack, int startIndex, int endIndex) {
		String ret = "";
		for (int i=startIndex;i<(endIndex + 1);i++) {
			ret = ret + haystack.charAt(i);
		}
		return ret;
	}	
	
	
	/**
	 * <p>Retrieve a subsection of a string between designated positions.
	 * 1 = first character in string.</p>
	 *
	 * @param  	langCodes   the array of language codes that are in scope
	 * @param	langcode	the language code from the record
	 */	
	private boolean matchLangScope(ArrayList<String> langCodes, String langcode) {
		boolean retVal = false;
		for (int i=0;i<langCodes.size();i++) {
			if (langcode.toLowerCase().equals(langCodes.get(i).toLowerCase())) {
				retVal = true;
			}
		}
		return retVal;
	}

}
