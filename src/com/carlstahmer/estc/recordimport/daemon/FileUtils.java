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

import java.io.File;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * @author cstahmer
 * 
 * <p>A utility class for performing various operations on file-system objects.</p>
 */
public class FileUtils {
	
	Conf config;
	SqlModel sqlObj;
	Logger logger;
	public ArrayList<String> fileList = new ArrayList<String>();
	public ArrayList<String> directoryList = new ArrayList<String>();
	
	/**
	 * <p>Constructor class that assigns passed Config object and SqlModel to 
	 * class instances.</p>
	 *
	 * @param  configObj    an instance of the Conf class
	 * @param  sqlModObj  	an instance of the sqlModel class
	 */
	public FileUtils(Conf configObj, SqlModel sqlModObj) {
		config = configObj;
		sqlObj = sqlModObj;
		logger = new Logger(config);
		directoryList.clear();
		fileList.clear();
	}
	
	
	/**
	 * <p>Traverses a designated directory recursively and returns a 
	 * list of all files found in the directory path.</p>
	 *
	 * @param  folderDir    The directory to search
	 */
	public void listFilesRecursive(String folderDir) {
		final File folder = new File(folderDir);
		fileList.clear();
	    for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	            listFilesForFolder(fileEntry.getName());
	        } else {
	        	String thisFileName = fileEntry.getName();
	        	if (!thisFileName.equals(".") && !thisFileName.equals("..")) {
	        		fileList.add(fileEntry.getName());
	        	}
	        }
	    }
	}
	
	/**
	 * <p>Initiates process of generating list of institutional directories to traverse.</p>
	 *
	 * @param  folderDir    The directory to search
	 */
	public void listFoldersRecursive() {
		directoryList.clear();
		String folderToListen = config.listenDir;
		System.out.println("Reading Folder: " + folderToListen);
		listFolders(folderToListen);
	}
	
	/**
	 * <p>Produces a recursive list of directories found in a designated directory path.</p>
	 *
	 * @param  folderDir    The directory to search
	 */
	public void listFolders(String folderDir) {
		final File folder = new File(folderDir);
	    for (final File dirEntry : folder.listFiles()) {
	        if (dirEntry.isDirectory()) {
	        	String thisFileName = dirEntry.getName();
	        	if (!thisFileName.equals(".") && !thisFileName.equals("..")) {
	        		directoryList.add(thisFileName);
	        		listFolders(folderDir+"/"+thisFileName);
	        	}
	        }
	    }
	    logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Size of directoryList array: "+String.valueOf(directoryList.size()));
	}
	
	/**
	 * <p>Produces a non-recursive list of files found in a designated directory.</p>
	 *
	 * @param  folderDir    The directory to search
	 */	
	public void listFilesForFolder(String folderDir) {
		final File folder = new File(folderDir);
		fileList.clear();
	    for (final File fileEntry : folder.listFiles()) {
	        if (!fileEntry.isDirectory()) {
	        	String thisFileName = fileEntry.getName();
	        	if (!thisFileName.equals(".") && !thisFileName.equals("..")) {
	        		fileList.add(fileEntry.getName());
	        	}
	        }
	    }
	}
	

	/**
	 * <p>Examines a file name and returns a numeric value indicating the type of 
	 * file as indicated by its extension.</p>
	 *
	 * @param  	strFileName	the filename to check
	 * @return				An integer designating the file type
	 */	
	public int fileType(String strFileName) {
		int intRet = 0;
		
		
		String[] parts = strFileName.split("\\.");
		int sizeParts = parts.length;
		String fileSuffix = parts[(sizeParts - 1)];
		if (fileSuffix.length() > 0) {
			
			logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "SQL connection opened");
			intRet = sqlObj.selectFileTypeID(fileSuffix);
			logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Retrieving File Type");
			if (fileSuffix.equals("mrc")) {
				intRet = 1;
			}
			
			
		}
		return intRet;
	}
	
	/**
	 * <p>A simple utility for comparing two strings.</p>
	 *
	 * @param	testString	The hay-stack to search
	 * @param	testCase	The needle to search for
	 * @return				A boolean comparison result [true/false]	
	 */		
	public boolean matchRegex(String testString, String testCase) {
		boolean blnMatch = false;
		Pattern pattern = Pattern.compile("[.]"+testCase+"$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(testString);

        while (matcher.find()) {
        	blnMatch = true;
        }
		return blnMatch;
	}
	

}
