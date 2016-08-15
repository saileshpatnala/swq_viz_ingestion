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

import java.util.ArrayList;


/**
 * @author cstahmer
 * 
 * <p>A utility class for performing various record based operations.</p>
 */

public class RecordUtils {

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
	public RecordUtils(Conf configObj, SqlModel sqlModObj) {
		config = configObj;
		sqlObj = sqlModObj;
		logger = new Logger(config);
	}
	
	/**
	 * <p>Determine if this record should be processed or not by 
	 * determining whether it is completely new or newer than 
	 * some version of the record that may exist in the system.</p>
	 *
	 * @param  	curCode    	The institutional code of the record originator
	 * @param 	control		The record control code
	 * @param	timeStamp	The timestamp for the record
	 * @param	recordType	0 = any, 1 = bib, 2 = holding
	 * @return				The ID of an oder found record, 0 = new record needs to be inserted, -1 = duplicate
	 */
	public int duplicateRecordCheck(String currCode, String control, double timeStamp, int recordType) {
		int retDup = 0;
		
		if (recordType == 1) {
			retDup = sqlObj.selectRecordRecord(currCode, 1, control);
		} else if (recordType == 2) {
			retDup = sqlObj.selectRecordRecord(currCode, 2, control);
		} else {
			int contId = sqlObj.selectRecordRecord(currCode, 1, control);
			int bibId = sqlObj.selectRecordRecord(currCode, 2, control);
			if (contId > 0 || bibId > 0) {
				if (contId > 0) {
					retDup = contId;
				} else {
					retDup = bibId;
				}
			}
		}
		
		if (retDup > 0) {
			
			logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Processing existing record with id : "+retDup);
			
			double dbmoddate = sqlObj.selectRecordMod(retDup);
			if (dbmoddate < timeStamp) {
				
				// if here, then the record in the db is older than the one in the file
				// so we need to clear the one in the DB so that we can put the new
				// data in place
				
				// select all fields for a record
				ArrayList<Integer> assocFields = sqlObj.selectRecordFieldIds(retDup);
				System.out.println("Size of Deletion Associated Fields Array: [" + assocFields.size() + "]");
				
				// For each field
				if (assocFields.size() > 0) {
					for (int iaf=0; iaf < assocFields.size(); iaf++) {
						// delete all subfields
						sqlObj.deleteSubFields(assocFields.get(iaf));
					}
					// delete the field
					sqlObj.deleteRecordFields(retDup);
				}
				
				sqlObj.updateRecordRecordModdate(retDup, timeStamp);
				sqlObj.setRecordRecordUnProcessed(retDup);
				sqlObj.setRecordRecordUnScoped(retDup);
				sqlObj.setRecordRecordNotExported(retDup);
			} else {
				retDup = -1;
				logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Skipping record beacause db version is the same or newer than file version: "+retDup);
			}
			
			
		}
		
		return retDup;
	}
	
}
