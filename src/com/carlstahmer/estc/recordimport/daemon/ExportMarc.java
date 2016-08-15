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

/**
 * @author cstahmer
 * 
 * <p>Class for exporting all previously un-exported or
 * changed records from db to MARC files.  Currently
 * only exports as MARC-XML</p>
 * 
 * <p>Loops through all records on the "records" table where
 * records.processed == 1 and  records.exported == 0 and
 * exports a MARC-XML file for the record to the directory
 * designated in the the config.yml file.  One the record
 * has been successfully output as MARC-XML the 
 * records.exported field is set to 1.</p>
 *
 */
public class ExportMarc {
	
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
	public ExportMarc(Conf config, SqlModel sqlModObj) {
		configObj = config;
		sqlObj = sqlModObj;
		logger = new Logger(config);
		
		// TODO Put Class Methods Here
		
		
		
			
		
	}

}
