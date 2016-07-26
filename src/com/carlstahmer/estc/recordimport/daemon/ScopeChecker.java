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
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	 * @param  sqlModObj  	an instance of the sqlModel class
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
			// When testing is complete, alter this loop
			// to run on all records that should be processed
			for (int i=0;i<(recordsToCheck.size()-1);i++) {
			//for (int i=0;i<10;i++) {
				
				
				boolean languageCheck = languageScope(configObj.languageScope, recordsToCheck.get(i));
				if (languageCheck) {
					System.out.println("Record " + recordsToCheck.get(i) + " passed language check.");
				} else {
					
				}
				
				boolean dateCheck = dateScope(recordsToCheck.get(i));
				if (dateCheck) {
					System.out.println("Record " + recordsToCheck.get(i) + " passed date check.");
				} else {
					
				}
				
				// get the information I need to write good log messages
				String strRecordControlNumber = sqlObj.selectRecordControlId(recordsToCheck.get(i));
				int recordFileId = sqlObj.selectRecordFileId(recordsToCheck.get(i));
				ArrayList<HashMap<String,String>> fileInfo = sqlObj.selectFileInfoById(recordFileId);
				HashMap<String,String> recordInfoRecord = fileInfo.get(0);
				String instCode = recordInfoRecord.get("institution_code");
				String recordFileName = recordInfoRecord.get("filename");
				
				String recordFileInfo = "Record with system ID " + recordsToCheck.get(i) +
						" from file " + instCode + " " + recordFileName + " local control number " +
						strRecordControlNumber + " ";
				
				
				if (languageCheck && dateCheck) {
					
					//TODO: UPDATE THE RECORD SO THAT 'scoped' = 1
					
					logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), recordFileInfo + " passed scope check for language and date.");
					System.out.println("Record " + recordsToCheck.get(i) + " passed scope check.");
				} else {
					// This is where I should write out to the log that the record was
					// rejected on scope
					String failedScopeItem = "";
					if (!languageCheck) {
						failedScopeItem = "language";
					}
					if (!dateCheck) {
						if (failedScopeItem.length() > 0) {
							failedScopeItem = failedScopeItem + " and date";
						} else {
							failedScopeItem = "date";
						}
					}
					
					logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), recordFileInfo + " failed scope check for " + failedScopeItem + ".");
					
					// first get all fields for the record than loop
					// through and delete the sub-fields
					ArrayList<Integer> fieldsToGo = sqlObj.selectAssocfieldIds(recordsToCheck.get(i));
					for (int dr : fieldsToGo) {
						sqlObj.deleteSubFields(dr);
					}
					
					// then delete the fields
					sqlObj.deleteRecordFields(recordsToCheck.get(i));
					
					// then delete the record
					boolean recordgone = sqlObj.deleteRecordRecord(recordsToCheck.get(i));
					
					//TODO: PUT CODE HERE TO ADD RECORD ID TO THE 
					
					
					if (recordgone) {
						logger.log(2, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), recordFileInfo + " was removed from db as out of scope.");
					}

					
				}
				
				
				
				
				//if (configObj.liberal) {
				//	ret = true;
				//}
				
				
				
			// end temp for loop bracket
			}
		}
		return ret;
	}
	
	
	/**
	 * <p>EMI-7</p>
	 * 
	 * <p>Create a dateScope(int recordID) method that checks a record to see if it is in 
	 * scope. The function is very robust.  It basically looks for anything sequence of 
	 * four consecutive numbers that could be treated as a date range, extracts thes, and
	 * the scopes the extracted potential dates.  MARC fields checked are:</p>
	 * 
	 * <p><ul>
	 * <li>008</li>
	 * <li>240</li>
	 * <li>260 $c</li>
	 * <li>264 $c</li>
	 * <li>362 $c</li>
	 * <li>500</li>
	 * <li>501</li>
	 * <li>504</li>
	 * <li>518</li>
	 * <li>520 $a</li>
	 * <li>520 $b</li>
	 * <li>524</li>
	 * <li>534 $a</li>
	 * <li>534 $c</li>
	 * <li>545</li>
	 * <li>561</li>
	 * <li>590 - 599</li>
	 *
	 * @param  recordID    	the records.id from the db for the record to check
	 * 
	 * @return				Boolean true or false
	 */
	public boolean dateScope(int recordID) {
		boolean ret = false;
		
		ArrayList<String> years = new ArrayList<String>();
		ArrayList<String> returnedYears = new ArrayList<String>();
		
		int recordType = sqlObj.getRecordType(recordID);
		if (recordType == 1) {
			
			// check 008 for header lookups bibs
			String zeroZeroEight = sqlObj.getZeroZeroEight(recordID);
			if (zeroZeroEight.length() > 36) {
				System.out.println("008: " + zeroZeroEight);
				String dateStringOne = extractCharacters(zeroZeroEight, 7, 10);
				String dateStringTwo = extractCharacters(zeroZeroEight, 11, 14);
				String dateStringOneFill = fillYear(dateStringOne);
				String dateStringTwoFill = fillYear(dateStringTwo);
				if (isYear(dateStringOneFill)) {
					System.out.println("008a: " + dateStringOneFill);
					years.add(dateStringOneFill);
				}
				if (isYear(dateStringTwoFill)) {
					System.out.println("008b: " + dateStringTwoFill);
					years.add(dateStringTwoFill);
				}
			}
			
			// do all the single field lookups
			ArrayList<String> singleFields = new ArrayList<String>();
			singleFields.add("240");
			singleFields.add("500");
			singleFields.add("501");
			singleFields.add("504");
			singleFields.add("518");
			singleFields.add("524");
			singleFields.add("545");
			singleFields.add("561");
			singleFields.add("590");
			singleFields.add("591");
			singleFields.add("592");
			singleFields.add("593");
			singleFields.add("594");
			singleFields.add("595");
			singleFields.add("596");
			singleFields.add("597");
			singleFields.add("598");
			singleFields.add("599");
			for (String singfield : singleFields) {
				String fieldVal = sqlObj.getFieldByNumber(recordID, singfield);
				if (fieldVal.length() > 3) {
					returnedYears.clear();
					System.out.println(singfield + ": " + fieldVal);
					returnedYears = extractYearString(fieldVal);				
					for (String td : returnedYears) {
						if (isYear(td)) {
							System.out.println(singfield + ": " + td);
							years.add(td);
						}
					}
				}
				
			}
			
			// do all sub-field lookups
			Hashtable<String,String> sf = new Hashtable<String,String>();
			sf.put("260", "c");
			sf.put("264", "c");
			sf.put("362", "c");
			sf.put("520", "a");
			sf.put("520", "c");
			sf.put("534", "a");
			sf.put("234", "c");
			Set<String> keys = sf.keySet();
			for(String key: keys){
				returnedYears.clear();
				returnedYears = extractYearsFromSubFields(recordID, key, sf.get(key));
				if (!returnedYears.isEmpty()) {
					years.addAll(returnedYears);			
				}
	        }
			
			// use this code to match a data in range.  Loop through
			// all members of the years lest, check each one, and if
			// it scopes, set ret = true
			
			for (String allyears : years) {
				System.out.println("Record " + recordID + " has date " + allyears);
				if (matchDateScope(configObj.lDateScopeBound, configObj.uDateScopebound, allyears)) {
					//System.out.println("Found in scope date " + allyears);
					ret = true;
				}
			}
			
		// automatically pass holding records since they have no original pub date
		// info.  I'll merge them wit their appropriate bib in another process before
		// outputting the MARC XML.
		} else {
			ret = true;
		}
		
		// check 130 for bib
		if (!ret) {
			
			
		}
		
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
		
		String langString;
		String zeroZeroEight = sqlObj.getZeroZeroEight(recordID);
		int recordType = sqlObj.getRecordType(recordID);
		if (recordType == 1) {
			if (zeroZeroEight.length() > 36) {
				langString = extractCharacters(zeroZeroEight, 35, 37);
				ret = matchLangScope(langCodes, langString);
				System.out.println("Record " + recordID + " has Bib Language: " + langString);
			}		
		} else if (recordType ==2) {
			if (zeroZeroEight.length() > 23) {
				langString = extractCharacters(zeroZeroEight, 22, 24);
				ret = matchLangScope(langCodes, langString);
				System.out.println("Record " + recordID + " has Holding Language: " + langString);
			}
		} else {
			if (configObj.liberal) {
				ret = true;
			}
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
	
	/**
	 * <p>Check a year to see if it is within a data range.</p>
	 *
	 * @param  	lowerDateBound   the low-end date scope (int)
	 * @param 	upperDateBound	 the upper date bound (int)
	 * @param	dateString		 the date to check (string)
	 */	
	private boolean matchDateScope(int lowerDateBound, int upperDateBound, String dateString) {
		boolean retVal = false;
		if (dateString.matches("[0-9]+") && dateString.length() > 3) {
			int intDate = Integer.valueOf(dateString);
			if (intDate >= lowerDateBound && intDate <= upperDateBound) {
				retVal = true;
			}
		}
		return retVal;
	}
	
	/**
	 * <p>Extract needles from a haystack.</p>
	 *
	 * @param  	haystack   	The text string to search in
	 */	
	private ArrayList<String> extractYearString(String haystack) {
		ArrayList<String> years = new ArrayList<String>();
		
		String patternString = "\\d{4}";

		Pattern pattern = Pattern.compile(patternString);
		Matcher matcher = pattern.matcher(haystack);

		while(matcher.find()) {
			//int beginIndex = matcher.start() + 1;
			int beginIndex = matcher.start();
			//int endIndex = matcher.end() + 1;
			int endIndex = matcher.end();
			years.add(haystack.substring(beginIndex, endIndex));
		}
		
		
		return years;
	}
	
	/**
	 * <p>Check that a string looks like a year.</p>
	 *
	 * @param  	haystack   	The text string to search in
	 */	
	private boolean isYear(String token) {
		boolean ret = false;
		
		String patternString = "\\d{4}";

		Pattern pattern = Pattern.compile(patternString);
		Matcher matcher = pattern.matcher(token);

		while(matcher.find()) {
			ret = true;
		}
		
		return ret;
	}
	
	/**
	 * <p>Extract needles from a haystack.</p>
	 *
	 * @param  	haystack   	The text string to search in
	 */	
	private ArrayList<String> extractYearsFromSubFields(int recordID, String mainfield, String subfield) {
		// check the 260 field (subfield c)
		ArrayList<String> years = new ArrayList<String>();
		ArrayList<String> rawValues = sqlObj.selectSubFieldValues(recordID, mainfield, subfield);
		if (!rawValues.isEmpty()) {
			for (String x : rawValues) {
				if (isYear(x)) {
					ArrayList<String> returnedYears = new ArrayList<String>();
					returnedYears = extractYearString(x);				
					for (String xx : returnedYears) {
						years.add(xx);
					}
				}
	
			}			
		}
		return years;
	}
	
	/**
	 * <p>adds trailing zeros to a 008 date field so that
	 * it is four digits long.  Needed because a catalgouer
	 * can enter "12  " designate something published
	 * sometime in the 1200s.</p>
	 *
	 * @param  	haystack   	The text string to search in
	 * 
	 * @return				The full, four integer date representation
	 */	
	private String fillYear(String token) {
		
		/// need to replace blank spaces with 0s not add them to the end
		
		String ret = token;
		
		String removPipe = ret.replaceAll("\\|", "0");
		String removSpace = removPipe.replaceAll("\\s", "0");
		String removNonDigits = removSpace.replaceAll("\\D", "0");
		String blankTest = "0000";
		if (removNonDigits.equals(blankTest)) {
			ret = "";
		} else {
			ret = removNonDigits;
		}
		
		return ret;
	}
	
}
