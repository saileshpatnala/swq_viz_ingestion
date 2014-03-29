package com.carlstahmer.estc.recordimport.daemon;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;

public class RecordsCollectionHolding {
	
	public Map<String, HashMap<String, String>> recordCollection = new HashMap<String, HashMap<String, String>>();
	
	
	public void put(String thisKey, HashMap<String, String> thisHash) {
		recordCollection.put(thisKey, thisHash);	
	}
	
	public Set<Map.Entry<String, HashMap<String, String>>> getEntrySet() {
		Set<Map.Entry<String, HashMap<String, String>>> retMapSet = recordCollection.entrySet();  
		return retMapSet;
	}
	
	public int getSize() {
		int ret;
		try {
			ret = recordCollection.size();
		} catch(NullPointerException e) {
			ret = 0;
		}
		return ret;
	}

}
