package com.carlstahmer.estc.recordimport.daemon;

import java.io.File;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
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
	 * <p>Traverses a designated directory recursively and returns a list of all files found in the directory path.</p>
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
			
			sqlObj.openConnection();
			if (sqlObj.connOpen) {
				logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "SQL connection opened");
				intRet = sqlObj.selectFileTypeID(fileSuffix);
				logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Retrieving File Type");
				boolean connClosed = sqlObj.closeConnection();
				if (connClosed) {
					logger.log(3, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "SQL connection closed");
				} else {
					logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "Error closing SQL connection");
				}
			} else {
				logger.log(1, Thread.currentThread().getStackTrace()[1].getFileName(), Thread.currentThread().getStackTrace()[1].getLineNumber(), "No active SQL connection");
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
