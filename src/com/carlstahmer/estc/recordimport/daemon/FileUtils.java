package com.carlstahmer.estc.recordimport.daemon;

import java.io.File;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class FileUtils {
	
	public ArrayList<String> fileList = new ArrayList<String>();
	public ArrayList<String> directoryList = new ArrayList<String>();
	
	public void listFilesRecursive(String folderDir) {
		final File folder = new File(folderDir);
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
	}
	
	
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
	
	public int fileType(String strFileName) {
		int intRet = 0;
		if(matchRegex(strFileName, "prd")) {
			intRet = 1;
		} else if(matchRegex(strFileName, "mrc")) {
			intRet = 1;
		} else if(matchRegex(strFileName, "txt")) {
			intRet = 2;
		} else if(matchRegex(strFileName, "xls")) {
			intRet = 3;
		} else if(matchRegex(strFileName, "csv")) {
			intRet = 4;
		} else if(matchRegex(strFileName, "xml")) {
			intRet = 5;
		}
		return intRet;
	}
	
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
