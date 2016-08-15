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
import java.util.HashMap;



/**
 * @author cstahmer
 * 
 * <p>A class that contains methods for folding separate Holding 
 * records into their associated Bib records.</p>
 */
public class MergeHoldings {
	
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
	public MergeHoldings(Conf configObj, SqlModel sqlModObj) {
		config = configObj;
		sqlObj = sqlModObj;
		recs = new RecordUtils(config, sqlObj);
		logger = new Logger(config);	
	}
	
	
	/**
	 * <p>Starting point and traffic cop for merge process.  This goes
	 * through all holding records and makes sure they can be matched to
	 * a bib record through a 004 field.  If the 004 is empty, then try
	 * to match the control number and institution of this holding record
	 * to a bib record.  If this bib is found, then add a 004 and put the 
	 * correct control number in it. If none is found, then do nothing.
	 * If one is found, then mark the record as processed. 
	 * </p>
	 * 
	 * <p>If a 004 is found, try to match this number in the bibs.  If none
	 * found then do nothing.  If found, then mark the record as processed.</p>
	 * 
	 * <p>In the above scenario, we leave un-matched holdings in the 
	 * db in case their match comes later.  Then, when we do scope checks,
	 * we process bibs first?????
	 *
	 * @return  true when merge completed, false if an error occurred
	 */
	public boolean doMerge () {
		// setup the return
		boolean ret = false;
		
		// grab the holding records in the system
		ArrayList<HashMap<String,String>> holdingRecords = sqlObj.selectHoldingRecords();
		
		System.out.println("Size of returned records: " + holdingRecords.size());
		
		
		// loop through the holding records
		int i = 1;
		for (HashMap<String,String> row : holdingRecords) {
			String thisId = row.get("id");
			System.out.println("Processing row " + i + "; it has " + row.size() + " elements and an id of " + thisId);
			i++;
		}
		
		return ret;
	}
	

}
