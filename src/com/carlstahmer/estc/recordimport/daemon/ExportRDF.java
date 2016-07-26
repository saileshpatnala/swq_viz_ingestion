package com.carlstahmer.estc.recordimport.daemon;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author cstahmer
 * 
 * <p>Class for exporting a records from the SQL DB
 * to an RDF file.</p>
 */

public class ExportRDF {
	
	Conf configObj;
	SqlModel sqlObj;
	Logger logger;
	String rdfString;

	public ExportRDF(Conf config, SqlModel sqlModObj) {
		configObj = config;
		sqlObj = sqlModObj;
		logger = new Logger(config);
	}
	
	public boolean makeRDFAllBibs(String domainURI) {
		boolean success = false;
		
		// loop through all bib records and send to makeRDF for each
		ArrayList<Integer> recordsQueue = sqlObj.selectUnprocessedBibs();
		for (int i=0;i < recordsQueue.size();i++) {
			makeRDF(recordsQueue.get(i), domainURI);
		}
		
		return success;
	}
	
	// create an RDF string for a resource
	public boolean makeRDF(int recordID, String domainURI) {
		boolean ret = false;
		
		//TODO: here I need to read in the record and construct various values
		// String libCode = GET VALUE

		
		
		// get the record type holding/bib
		int recordType = sqlObj.getRecordType(recordID);
		
		// set the id for the item.  ESTCID for bibs and record IDs for holdings
		String itemID;
		if (recordType == 1) {
			itemID = sqlObj.selectRecordControlId(recordID);
		} else {
			itemID = String.valueOf(recordID);
		}
		
		// get the library code for the record
		ArrayList<HashMap<String,String>> tableResults = sqlObj.selectFileInfoById(sqlObj.selectRecordFileId(recordID));
		HashMap<String,String> recordInfoRecord = tableResults.get(0);
		String instCode = recordInfoRecord.get("institution_code");
		
		
		//String itemID = "use an ESTC ID if this is a bib record, otherwise use the record ID";
		
		// make header
		rdfString = "<rdf:RDF xmlns:gl=\"http://bl.uk.org/schema#\"\n";
		rdfString = rdfString + "    xmlns:estc=\"http://estc21.ucr.edu/schema#\"\n";
		rdfString = rdfString + "    xmlns:dct=\"http://purl.org/dc/terms/#\"\n";
		rdfString = rdfString + "    xmlns:dc=\"http://purl.org/dc/elements/1.1/#\"\n";
		rdfString = rdfString + "    xmlns:foaf=\"http://xmlns.com/foaf/0.1/#\"\n";
		rdfString = rdfString + "    xmlns:geo=\"http://www.w3.org/2003/01/geo/wgs84_pos/#\"\n";
		rdfString = rdfString + "    xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n";
		rdfString = rdfString + "    xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema/#\"\n";
		rdfString = rdfString + "    xmlns:role=\"http://www.loc.gov/loc.terms/relators/\"\n";
		rdfString = rdfString + "    xmlns:bf=\"http://bibframe.org/vocab/\"\n";
		rdfString = rdfString + "    xmlns:relators=\"http://id.loc.gov/vocabulary/relators/\"\n";
		rdfString = rdfString + "    xmlns:collex=\"http://www.collex.org/schema#\"\n";
		rdfString = rdfString + "    xmlns:scm=\"http://schema.org/\" >\n";
		
		// make the record itself
		rdfString = rdfString + "    <estc:estc rdf:about=\"http://" + domainURI + "/" + itemID + "\">\n";
		rdfString = rdfString + "        <collex:federation>ESTC</collex:federation>\n";
		rdfString = rdfString + "        <collex:archive>" + instCode + "</collex:archive>\n";
		
		// setup needed output variables
		String finalTitle = "Utitled or Title not Known";
		String finalDate = "";
		ArrayList<String> subjectTerms = new ArrayList<String>();
		String genre = "";
		ArrayList<String> coverage = new ArrayList<String>();
		ArrayList<ArrayList<String>> authorArray = new ArrayList<ArrayList<String>>();
		ArrayList<String> fiveHundNotes = new ArrayList<String>();
		
		// Get all of the fields associated with this record
		ArrayList<Integer> fieldsArray = sqlObj.selectRecordFieldIds(recordID);
		for (int i=0;i < fieldsArray.size();i++) {
			// loop through the fields and process
			Integer fieldID = fieldsArray.get(i);
			String fieldType = sqlObj.selectFieldType(fieldID);
			
			
			
			
			//TO: Everything below is broken because each time I loop
			//through a field I just grab the field that matches the record
			//and field type, rather than grabbing by the field ID.  This
			// means that it pulls the first record every time for fields that
			// have  multiple entries.  I need to change this so that
			// I'm retrieving field data by the actual id
			
			
			/*
			need to change
			
			getFieldByNumber(recordID, "008")
			
			to getFieldByID(fieldID)  [I need to create getFieldByID(String fieldID)]
					
			Then I need to change ever call to work this way		
			*/
			
			
			
			
			
			
			
			
			
			
			
			// if 245 title
			if (fieldType.equals("245")) {
				// get raw value
				String rawValue = sqlObj.getFieldByID(fieldID);;
				// get subfields
				String titleA = "";
				String titleB = "";
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValues(recordID, "245", "a");
				for (int ia=0;ia < subFieldA.size();ia++) {
					titleA = subFieldA.get(ia);
				}
				ArrayList<String> subFieldB = sqlObj.selectSubFieldValues(recordID, "245", "b");
				for (int ia=0;ia < subFieldB.size();ia++) {
					titleB =  subFieldB.get(ia);
				}
				
				if (titleA.length() > 0) {
					finalTitle = titleA;
					if (titleB.length() > 0) {
						finalTitle = finalTitle + " " + titleB;
					}
				} else if (rawValue.length() > 0) {
					finalTitle = rawValue;
				} else {
					finalTitle = "Utitled or Title not Known";
				}
			}
			
			// if 008 date
			if (fieldType.equals("008")) {
				// get raw value
				String rawZeroZeroEight = sqlObj.getFieldByNumber(recordID, "008");
				if (rawZeroZeroEight.length() > 13 ) {
					String one = String.valueOf(rawZeroZeroEight.charAt(7));
					String two = String.valueOf(rawZeroZeroEight.charAt(8));
					String three = String.valueOf(rawZeroZeroEight.charAt(9));
					String four = String.valueOf(rawZeroZeroEight.charAt(10));
					String five = String.valueOf(rawZeroZeroEight.charAt(11));
					String six = String.valueOf(rawZeroZeroEight.charAt(12));
					String seven = String.valueOf(rawZeroZeroEight.charAt(13));
					String eight = String.valueOf(rawZeroZeroEight.charAt(14));
					String startDate = one + two + three + four;
					startDate = startDate.replaceAll("[^\\d.]", "");
					String endDate = five + six + seven + eight;
					endDate = endDate.replaceAll("[^\\d.]", "");
					if (startDate.length() == 4) {
						finalDate = startDate;
						if (endDate.length() == 4) {
							finalDate = finalDate + "-" + endDate;
						}
					}
				}
			}
			
			// if 100 author
			if (fieldType.equals("100")) {
				String thisAuthor = "";
				// get raw value
				String rawValue = sqlObj.getFieldByID(fieldID);;
				// get subfields
				String subA = "";
				String subB = "";
				String subC = "";
				String subD = "";
				String subE = "";
				
				ArrayList<String> retVal = new ArrayList<String>();
				
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValues(recordID, "100", "a");
				for (int ia=0;ia < subFieldA.size();ia++) {
					subA = subFieldA.get(ia);
				}
				ArrayList<String> subFieldB = sqlObj.selectSubFieldValues(recordID, "100", "b");
				for (int ia=0;ia < subFieldB.size();ia++) {
					subB =  subFieldB.get(ia);
				}
				ArrayList<String> subFieldC = sqlObj.selectSubFieldValues(recordID, "100", "c");
				for (int ia=0;ia < subFieldC.size();ia++) {
					subB =  subFieldC.get(ia);
				}
				ArrayList<String> subFieldD = sqlObj.selectSubFieldValues(recordID, "100", "d");
				for (int ia=0;ia < subFieldD.size();ia++) {
					subB =  subFieldD.get(ia);
				}
				ArrayList<String> subFieldE = sqlObj.selectSubFieldValues(recordID, "100", "e");
				for (int ia=0;ia < subFieldE.size();ia++) {
					subB =  subFieldE.get(ia);
				}
				
				String seperator = " ";
				if (subA.length() > 0) {
					if (subB.length() == 0 && subC.length() == 0 && subD.length() == 0) {
						String trimmed = removeFinalComma(subA);
						thisAuthor = trimmed;
					} else {
						thisAuthor = subA;
						if (subB.length() > 0) {
							if (subC.length() == 0 && subD.length() == 0) {
								String trimmedB = removeFinalComma(subB);
								thisAuthor = thisAuthor + seperator + trimmedB;
							} else {
								thisAuthor = thisAuthor + seperator + subB;
							}
						}
						if (subC.length() > 0) {
							if (subD.length() == 0) {
								String trimmedC = removeFinalComma(subC);
								thisAuthor = thisAuthor + seperator + trimmedC;
							} else {
								thisAuthor = thisAuthor + seperator + subC;
							}
						}	
						if (subD.length() > 0) {
								thisAuthor = thisAuthor + seperator + subD;
						}
					}
				} else if (rawValue.length() > 0) {
					thisAuthor = rawValue;
				}
				
				if (subE.length() == 0) {
					subE = "AUT";
				}
				
				String upperE = subE.toUpperCase();
				retVal.add(upperE);
				retVal.add(thisAuthor);

				authorArray.add(retVal);
			}
			
			// do 600 (subject term - personal name)
			if (fieldType.equals("600")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				subFieldsToInclude.add("c");
				subFieldsToInclude.add("d");
				String thisSubjectString = getSubject(recordID, "600", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// do 610 (subject term - corporate name)
			if (fieldType.equals("610")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "610", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// do 611 (subject term - meeting)
			if (fieldType.equals("611")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				subFieldsToInclude.add("b");
				subFieldsToInclude.add("c");
				String thisSubjectString = getSubject(recordID, "611", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// do 630 (subject term - Uniform Title)
			if (fieldType.equals("630")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "630", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// do 648 (subject term - Chronological Term)
			if (fieldType.equals("648")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "648", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// do 650 (subject term - Topical Term)
			if (fieldType.equals("650")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				subFieldsToInclude.add("b");
				subFieldsToInclude.add("c");
				subFieldsToInclude.add("d");
				String thisSubjectString = getSubject(recordID, "650", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// do 651 (subject term - Geographic Name)
			if (fieldType.equals("651")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "651", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// do 653 (subject term - Uncontrolled)
			if (fieldType.equals("653")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "653", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// do 654 (subject term - Faceted topical term)
			if (fieldType.equals("654")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				subFieldsToInclude.add("b");
				String thisSubjectString = getSubject(recordID, "654", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// do 656 (subject term - Occupation)
			if (fieldType.equals("656")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "656", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// do 657 (subject term - Function)
			if (fieldType.equals("657")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "657", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// do 658 (subject term - Curriculum Objective)
			if (fieldType.equals("658")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				subFieldsToInclude.add("b");
				String thisSubjectString = getSubject(recordID, "658", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// do 662 (subject term - Hierarchical place name)
			if (fieldType.equals("662")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				subFieldsToInclude.add("b");
				subFieldsToInclude.add("c");
				subFieldsToInclude.add("d");
				subFieldsToInclude.add("f");
				subFieldsToInclude.add("g");
				subFieldsToInclude.add("h");
				String thisSubjectString = getSubject(recordID, "662", subFieldsToInclude, " ");
				if (thisSubjectString.length() > 0) {
					subjectTerms.add(thisSubjectString);
				}

			}
			
			// collex:genre
			if (fieldType.equals("655")) {
				String workingValue = "";
				// get raw value
				String rawValue = sqlObj.getFieldByID(fieldID);;
				// get subfields
				String subA = "";
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValues(recordID, "655", "a");
				for (int iag=0;iag < subFieldA.size();iag++) {
					subA = subFieldA.get(iag);
				}
				if (subA.length() > 0) {
					workingValue = subA;
				} else if (rawValue.length() > 0) {
					workingValue = rawValue;
				}
				genre = workingValue;
			}
			
			// dc:coverage
			if (fieldType.equals("751")) {
				String workingValue = "";
				// get raw value
				String rawValue = sqlObj.getFieldByID(fieldID);;
				// get subfields
				String subA = "";
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValues(recordID, "751", "a");
				for (int iag=0;iag < subFieldA.size();iag++) {
					subA = subFieldA.get(iag);
				}
				if (subA.length() > 0) {
					workingValue = subA;
				} else if (rawValue.length() > 0) {
					workingValue = rawValue;
				}
				coverage.add(workingValue);
			}
			
			// dc:coverage - hierarchical
			if (fieldType.equals("752")) {
				String workingValue = "";
				// get raw value
				String rawValue = sqlObj.getFieldByID(fieldID);;
				// get subfields
				String subA = "";
				String subB = "";
				String subC = "";
				String subD = "";
				String subF = "";
				String subG = "";
				String subH = "";
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValues(recordID, "752", "a");
				for (int iag=0;iag < subFieldA.size();iag++) {
					subA = subFieldA.get(iag);
				}
				ArrayList<String> subFieldB = sqlObj.selectSubFieldValues(recordID, "752", "b");
				for (int iag=0;iag < subFieldB.size();iag++) {
					subB = subFieldB.get(iag);
				}
				ArrayList<String> subFieldC = sqlObj.selectSubFieldValues(recordID, "752", "c");
				for (int iag=0;iag < subFieldC.size();iag++) {
					subC = subFieldC.get(iag);
				}	
				ArrayList<String> subFieldD = sqlObj.selectSubFieldValues(recordID, "752", "d");
				for (int iag=0;iag < subFieldD.size();iag++) {
					subD = subFieldD.get(iag);
				}
				ArrayList<String> subFieldF = sqlObj.selectSubFieldValues(recordID, "752", "f");
				for (int iag=0;iag < subFieldF.size();iag++) {
					subF = subFieldF.get(iag);
				}
				ArrayList<String> subFieldG = sqlObj.selectSubFieldValues(recordID, "752", "g");
				for (int iag=0;iag < subFieldG.size();iag++) {
					subG = subFieldG.get(iag);
				}
				ArrayList<String> subFieldH = sqlObj.selectSubFieldValues(recordID, "752", "h");
				for (int iag=0;iag < subFieldH.size();iag++) {
					subH = subFieldH.get(iag);
				}
				
				String seperator = "--";
				boolean notFirst = false;
				if (subA.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subA;
					notFirst = true;
				}
				if (subB.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subB;
					notFirst = true;
				}
				if (subC.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subC;
					notFirst = true;
				}
				if (subD.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subD;
					notFirst = true;
				}
				if (subF.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subF;
					notFirst = true;
				}
				if (subG.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subG;
					notFirst = true;
				}
				if (subH.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subH;
					notFirst = true;
				}
					
					
				if ((workingValue.length() == 0) && (rawValue.length() > 0)) {
					workingValue = rawValue;
				}
				
				if (workingValue.length() > 0) {
					coverage.add(workingValue);
				}

			}
			
			// 500 notes
			if (fieldType.equals("500")) {
				// get raw value
				String rawValue = sqlObj.getFieldByID(fieldID);
				// get subfields
				String note = "";
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int in=0;in < subFieldA.size();in++) {
					note = subFieldA.get(in);
					System.out.println(note);
				}
				
				if (note.length() > 0) {
					fiveHundNotes.add(note);
				} else if (rawValue.length() > 0) {
					fiveHundNotes.add(rawValue);
				}
			}
			
		}
		
		// build the content part of the RDF
		rdfString = rdfString + "        <dct:title>" + finalTitle + "</dct:title>\n";
		
		for (int iat=0;iat < authorArray.size();iat++) {
			ArrayList<String> thisCont = authorArray.get(iat);
			rdfString = rdfString + formatContributor(thisCont.get(0), thisCont.get(1));
		}
		
		if (finalDate.length() > 0) {
			rdfString = rdfString + "        <dc:date>" + finalDate + "</dc:date>\n";
		}
		if (coverage.size() > 0) {
			for (int ic=0;ic < coverage.size();ic++) {
				rdfString = rdfString + "        <dc:coverage>" + coverage.get(ic) + "</dc:coverage>\n";
			}
		}
		if (genre.length() > 0) {
			rdfString = rdfString + "        <collex:genre>" + genre + "</collex:genre>\n";
		}
		if (subjectTerms.size() > 0) {
			for (int ist=0;ist < subjectTerms.size();ist++) {
				rdfString = rdfString + subjectTerms.get(ist);
			}
		}
		if (fiveHundNotes.size() > 0) {
			for (int isn=0;isn < fiveHundNotes.size();isn++) {
				rdfString = rdfString + "        <dct:description>" + fiveHundNotes.get(isn) + "</dct:description>\n";
			}
		}
		
		
		
		
		
		
		
		
		// make closer
		rdfString = rdfString + "    </estc:estc>\n";
		rdfString = rdfString + "</rdf:RDF>";
		
		System.out.println(rdfString);
		
		return ret;
	}
	
	// create an RDF string for a resource
	public String getSubject(int recordID, String field, ArrayList<String> subFields, String separator) {
		String ret = "";
		String workingString = "";
		// get raw value
		
		String rawValue = sqlObj.getFieldByNumber(recordID, field);
		
		for (int ia=0;ia < subFields.size();ia++) {
			String localWorking = "";
			ArrayList<String> subFieldIt = sqlObj.selectSubFieldValues(recordID, field, subFields.get(ia));
			for (int iab=0;iab < subFieldIt.size();iab++) {
				localWorking = subFieldIt.get(iab);
			}
			if (localWorking.length() > 0) {
				if (workingString.length() > 0) {
					workingString = workingString + separator;
				}
				workingString = workingString + localWorking;
			}
		}
		
		if ((workingString.length() == 0) && (rawValue.length() > 0)) {
			workingString = rawValue;
		}
		
		if (workingString.length() > 0) {
			String collapseString = workingString.replaceAll("[^\\s]", "");
			ret = ret + "        <dct:subject>\n";
			ret = ret + "                <scm:about rdfs:resource=\"http://estc.bl.uk/subjects/" + collapseString + "\">\n";
			ret = ret + "                     <rdfs:label>" + workingString + "</rdfs:label>\n";
			ret = ret + "                </scm:about>\n";
			ret = ret + "        </dct:subject>\n";
		}

		return ret;
	}
	
	public String formatContributor(String relator, String value) {
		String ret = "        <role:" + relator + ">" + value + "</role:" + relator + ">\n";		
		return ret;
	}
	
	public String removeFinalComma(String str) {
	    String ret = str.substring(0, str.length()-1);
	    return ret;
	}

}
