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
 *  this software either in whole or in part for scholarly and educational purposes.</p>
 */

package com.carlstahmer.estc.recordimport.daemon;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
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
	String rdfHeader;
	String rdfAbout;
	String rdfString;
	String rdfFooter;

	public ExportRDF(Conf config, SqlModel sqlModObj) {
		configObj = config;
		sqlObj = sqlModObj;
		logger = new Logger(config);
	}
	
	public boolean makeRDFAllBibs(String domainURI) {
		boolean success = false;
		
		// loop through all bib records and send to makeRDF for each
		
		ArrayList<Integer> recordsQueue = sqlObj.selectUnExportedBibs();
		for (int i=0;i < recordsQueue.size();i++) {
			int workingRecordID = recordsQueue.get(i);
			makeRDF(workingRecordID, domainURI);
		}
		
		return success;
	}
	
	// create an RDF string for a resource
	public boolean makeRDF(int recordID, String domainURI) {
		boolean ret = false;
		
		// get the record type holding/bib
		int recordType = sqlObj.getRecordType(recordID);
		
		// set the id for the item.  ESTCID for bibs and record IDs for holdings
		String itemID;
		if (recordType == 1) {
			itemID = sqlObj.selectRecordControlId(recordID);
		} else {
			itemID = String.valueOf(recordID);
		}
		
		System.out.println("Processing record " + recordID + " item " + itemID);

		// get the library code for the record
		ArrayList<HashMap<String,String>> tableResults = sqlObj.selectFileInfoById(sqlObj.selectRecordFileId(recordID));
		HashMap<String,String> recordInfoRecord = tableResults.get(0);
		String instCode = recordInfoRecord.get("institution_code");
	
		//String itemID = "use an ESTC ID if this is a bib record, otherwise use the record ID";
		
		// make header
		rdfHeader = "<rdf:RDF xmlns:gl=\"http://bl.uk.org/schema#\"\n";
		rdfHeader = rdfHeader + "    xmlns:estc=\"http://estc21.ucr.edu/schema#\"\n";
		rdfHeader = rdfHeader + "    xmlns:dct=\"http://purl.org/dc/terms/#\"\n";
		rdfHeader = rdfHeader + "    xmlns:dc=\"http://purl.org/dc/elements/1.1/#\"\n";
		rdfHeader = rdfHeader + "    xmlns:foaf=\"http://xmlns.com/foaf/0.1/#\"\n";
		rdfHeader = rdfHeader + "    xmlns:geo=\"http://www.w3.org/2003/01/geo/wgs84_pos/#\"\n";
		rdfHeader = rdfHeader + "    xmlns:rdau=\"http://rdaregistry.info/Elements/u/#\"\n";
		rdfHeader = rdfHeader + "    xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n";
		rdfHeader = rdfHeader + "    xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema/#\"\n";
		rdfHeader = rdfHeader + "    xmlns:reg=\"http://metadataregistry.org/uri/profile/RegAp/#\"\n";
		rdfHeader = rdfHeader + "    xmlns:role=\"http://www.loc.gov/loc.terms/relators/\"\n";
		rdfHeader = rdfHeader + "    xmlns:bf=\"http://bibframe.org/vocab/\"\n";
		rdfHeader = rdfHeader + "    xmlns:isbdu=\"http://iflastandards.info/ns/isbd/unc/elements/\"\n";
		rdfHeader = rdfHeader + "    xmlns:relators=\"http://id.loc.gov/vocabulary/relators/\"\n";
		rdfHeader = rdfHeader + "    xmlns:collex=\"http://www.collex.org/schema#\"\n";
		rdfHeader = rdfHeader + "    xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\"\n";
		rdfHeader = rdfHeader + "    xmlns:scm=\"http://schema.org/\" >\n";
		
		// make footer
		rdfFooter = "    </estc:estc>\n";
		rdfFooter = rdfFooter + "</rdf:RDF>";
		
		// make unique identifier for about and parent associations
		String uniqueRI = "http://" + domainURI + "/" + itemID;
		String parentAssoc = "        <bf:instanceOf>" + uniqueRI + "</bf:instanceOf>\n";
		
		// make the record itself
		rdfAbout = "    <estc:estc rdf:about=\"" + uniqueRI + "\">\n";
		rdfString = "        <collex:federation>ESTC</collex:federation>\n";
		rdfString = rdfString + "        <collex:archive>" + instCode + "</collex:archive>\n";
		
		// setup needed output variables
		String finalTitle = "Utitled or Title not Known";
		String finalDate = "";
		String finalDateString = "";
		ArrayList<String> subjectTerms = new ArrayList<String>();
		String genre = "";
		ArrayList<String> coverage = new ArrayList<String>();
		ArrayList<ArrayList<String>> authorArray = new ArrayList<ArrayList<String>>();
		ArrayList<String> fiveHundNotes = new ArrayList<String>();
		ArrayList<String> fiveHundTenNotes = new ArrayList<String>();
		ArrayList<String> surrogateSub = new ArrayList<String>();
		String estcID = "";
		
		
		// newly added fields
		String abrvTitle = ""; // rdau:abbreviatedTitle
		String uniformTitleTwoForty = ""; // rdau:titleOfResource
		String seriesUniformTitle = ""; // rdau:titleProperOfSeries
		String variantTitle = ""; // rdau:variantTitle
		String formerTitle = ""; // rdau:earlierTitleProper
		String languageCode = ""; // dc:language
		String editionStatement = ""; // rdau:editionStatement
		String prodInfo = ""; // dct:publisher
		String formerPubFreq = ""; // rdau:noteOnFrequency
		String physDesc = ""; // dct:format
		String contentType = ""; // dct:type
		String mediaType = ""; // dct:format
		String carrierType = ""; // dct:type
		String associatedPlace = ""; // dc:coverage
		String creationEpoch = ""; // dct:created
		String estcThumbnail = ""; // collex:thumbnail rdf:resource=""
		ArrayList<String> seriesStatment = new ArrayList<String>(); //   isbdu:P1041  hasNoteOnSeriesAndMultipartMonographicResources
		ArrayList<String> uniformTitle = new ArrayList<String>(); //   rdau:titleOfResource



					
		
		
		// Get all of the fields associated with this record
		ArrayList<Integer> fieldsArray = sqlObj.selectRecordFieldIds(recordID);
		for (int i=0;i < fieldsArray.size();i++) {
			// loop through the fields and process
			Integer fieldID = fieldsArray.get(i);
			String fieldType = sqlObj.selectFieldType(fieldID);

/* TODO
 * saved as field 1000
 * thumbnail -> <collex:thumbnail rdf:resource="">  
 * String estcThumbnail
 * eg -> <collex:thumbnail rdf:resource="http://YOUR_PUBLICATION.ORG/THUMBNAIL.JPG"/>
 */
			// if 1000 Thumbnail image
			if (fieldType.equals("1000")) {
				// get raw value
				estcThumbnail = sqlObj.getFieldByID(fieldID);;
			}
				
			// if 245 title
			if (fieldType.equals("245")) {
				// get raw value
				String rawValue = sqlObj.getFieldByID(fieldID);;
				// get subfields
				String titleA = "";
				String titleB = "";
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int ia=0;ia < subFieldA.size();ia++) {
					titleA = fixAmper(subFieldA.get(ia));
				}
				ArrayList<String> subFieldB = sqlObj.selectSubFieldValuesByID(fieldID, "b");
				for (int ia=0;ia < subFieldB.size();ia++) {
					titleB =  fixAmper(subFieldB.get(ia));
				}
				
				if (titleA != null && titleA.length() > 0) {
					finalTitle = titleA;
					if (titleB != null && titleB.length() > 0) {
						finalTitle = finalTitle + " " + titleB;
					}
				} else if (rawValue != null && rawValue.length() > 0) {
					finalTitle = rawValue;
				} else {
					finalTitle = "Utitled or Title not Known";
				}

			}
			
			// if 210  abbreviated title - rdau:abbreviatedTitle
			if (fieldType.equals("210")) {
				// get raw value
				String rawAbrevTitleValue = sqlObj.getFieldByID(fieldID);;
				// get subfields
				String abrevTitleA = "";
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int ia=0;ia < subFieldA.size();ia++) {
					abrevTitleA = fixAmper(subFieldA.get(ia));
				}
				if (abrevTitleA != null && abrevTitleA.length() > 0) {
					abrvTitle = abrevTitleA;
				} else if (rawAbrevTitleValue != null && rawAbrevTitleValue.length() > 0) {
					finalTitle = rawAbrevTitleValue;
				}
			}
		
			// if 130 & 730 & 240 - Uniform Title - uniformTitle - rdau:titleOfResource
			if (fieldType.equals("130") || fieldType.equals("730") || fieldType.equals("240")) {
				ArrayList<String> subFieldAut = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int iaut=0;iaut < subFieldAut.size();iaut++) {
					uniformTitle.add(fixAmper(subFieldAut.get(iaut)));
				}
			}

			// if 243 - Collective Uniform Title - String seriesUniformTitle = ""; // rdau:titleProperOfSeries
			if (fieldType.equals("243")) {
				ArrayList<String> subFieldtps = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int itps=0;itps < subFieldtps.size();itps++) {
					seriesUniformTitle = fixAmper(subFieldtps.get(itps));
				}
			}

			// if 246 - Varrying Form of Title - String variantTitle = ""; // rdau:variantTitle
			if (fieldType.equals("246")) {
				// get subfields
				String varTitleA = "";
				String varTitleB = "";
				String finalVTitle = "";
				ArrayList<String> subFieldAv = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int ia=0;ia < subFieldAv.size();ia++) {
					varTitleA = fixAmper(subFieldAv.get(ia));
				}
				ArrayList<String> subFielBv = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int ia=0;ia < subFielBv.size();ia++) {
					varTitleB = fixAmper(subFielBv.get(ia));
				}
				if (varTitleA != null && varTitleA.length() > 0) {
					finalVTitle = varTitleA;
					if (varTitleB != null && varTitleB.length() > 0) {
						finalVTitle = finalVTitle + " - " + varTitleB;
					}
					variantTitle = finalVTitle;
				}
			}

			// if 247 - Former Title - String formerTitle = ""; // rdau:earlierTitleProper
			if (fieldType.equals("247")) {
				// get subfields
				String fTitleA = "";
				String fTitleB = "";
				String finalFTitle = "";
				ArrayList<String> subFieldAf = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int ia=0;ia < subFieldAf.size();ia++) {
					fTitleA = fixAmper(subFieldAf.get(ia));
				}
				ArrayList<String> subFielBf = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int ia=0;ia < subFielBf.size();ia++) {
					fTitleB = fixAmper(subFielBf.get(ia));
				}
				if (fTitleA != null && fTitleA.length() > 0) {
					finalFTitle = fTitleA;
					if (fTitleB != null && fTitleB.length() > 0) {
						finalFTitle = finalFTitle + " - " + fTitleB;
					}
					formerTitle = finalFTitle;
				}
			}

			
			// if 008 date
			if (fieldType.equals("008")) {
				// get raw value
				String rawZeroZeroEight = sqlObj.getFieldByNumber(recordID, "008");
				if (rawZeroZeroEight != null && rawZeroZeroEight.length() > 13 ) {
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

					if (startDate  != null && startDate.length() == 4) {
						finalDate = startDate;
						finalDateString = finalDate;
						if ( endDate != null && endDate.length() == 4) {
							finalDate = finalDate + "," + endDate;
							finalDateString = finalDate + "," + endDate;
						}
					}
				}
			}
			
/* TODO
 * 
 * FIELD 41 & 765 - Language Code
 
$a - Language code of text/sound track or separate title (R)
$b - Language code of summary or abstract (R)
$d - Language code of sung or spoken text (R)
$e - Language code of librettos (R)
$f - Language code of table of contents (R)
$g - Language code of accompanying material other than librettos (R)
$h - Language code of original (R)
$j - Language code of subtitles or captions (R)
$k - Language code of intermediate translations (R)
$m - Language code of original accompanying materials other than librettos (R)
$n - Language code of original libretto (R)

XML ENTITY: dc:language

String languageCode = ""; // dc:language

 * 
 */
			
			// if 100 author
			if (fieldType.equals("100") || fieldType.equals("700")) {
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
				
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int ia=0;ia < subFieldA.size();ia++) {
					subA = subFieldA.get(ia);
				}
				ArrayList<String> subFieldB = sqlObj.selectSubFieldValuesByID(fieldID, "b");
				for (int ia=0;ia < subFieldB.size();ia++) {
					subB =  subFieldB.get(ia);
				}
				ArrayList<String> subFieldC = sqlObj.selectSubFieldValuesByID(fieldID, "c");
				for (int ia=0;ia < subFieldC.size();ia++) {
					subC =  subFieldC.get(ia);
				}
				ArrayList<String> subFieldD = sqlObj.selectSubFieldValuesByID(fieldID, "d");
				for (int ia=0;ia < subFieldD.size();ia++) {
					subD =  subFieldD.get(ia);
				}
				ArrayList<String> subFieldE = sqlObj.selectSubFieldValuesByID(fieldID, "e");
				for (int ia=0;ia < subFieldE.size();ia++) {
					subE =  subFieldE.get(ia);
				}
				
				String seperator = " ";
				if (subA  != null && subA.length() > 0) {
					if (subB != null && subB.length() == 0 && subC != null && subC.length() == 0 && subD != null && subD.length() == 0) {
						String trimmed = removeFinalComma(subA);
						thisAuthor = trimmed;
					} else {
						thisAuthor = subA;
						if (subB != null && subB.length() > 0) {
							if (subC != null && subC.length() == 0 && subD != null && subD.length() == 0) {
								String trimmedB = removeFinalComma(subB);
								thisAuthor = thisAuthor + seperator + trimmedB;
							} else {
								thisAuthor = thisAuthor + seperator + subB;
							}
						}
						if (subC != null && subC.length() > 0) {
							if (subD != null && subD.length() == 0) {
								String trimmedC = removeFinalComma(subC);
								thisAuthor = thisAuthor + seperator + trimmedC;
							} else {
								thisAuthor = thisAuthor + seperator + subC;
							}
						}	
						if (subD != null && subD.length() > 0) {
								thisAuthor = thisAuthor + seperator + subD;
						}
					}
				} else if (rawValue != null && rawValue.length() > 0) {
					thisAuthor = rawValue;
				}
				
				thisAuthor = fixAmper(thisAuthor);
				
				if (subE != null && subE.length() == 0) {
					subE = "AUT";
				}
				
				String upperE = subE.toUpperCase();
				retVal.add(upperE);
				retVal.add(thisAuthor);

				authorArray.add(retVal);
			}
		
			// corporate author
			if (fieldType.equals("110") || fieldType.equals("710")) {
				ArrayList<String> retValc = new ArrayList<String>();
				String subAc = "";
				String subEc = "";
				ArrayList<String> subFieldAc = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int ia=0;ia < subFieldAc.size();ia++) {
					subAc = subFieldAc.get(ia);
				}
				ArrayList<String> subFieldEc = sqlObj.selectSubFieldValuesByID(fieldID, "e");
				for (int ia=0;ia < subFieldEc.size();ia++) {
					subEc =  subFieldEc.get(ia);
				}
				
				String upperEc = subEc.toUpperCase();
				retValc.add(upperEc);
				retValc.add(subAc);
				authorArray.add(retValc);
			}
			
			// meeting as author
			// if 111 or 711 author
			if (fieldType.equals("100") || fieldType.equals("700")) {
				String thisAuthorm = "";
				// get subfields
				String subAm = "";
				String subBm = "";
				String subCm = "";
				String subDm = "";
				String subJm = "";
				ArrayList<String> retValm = new ArrayList<String>();
				
				ArrayList<String> subFieldAm = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int ia=0;ia < subFieldAm.size();ia++) {
					subAm = subFieldAm.get(ia);
				}
				ArrayList<String> subFieldBm = sqlObj.selectSubFieldValuesByID(fieldID, "b");
				for (int ia=0;ia < subFieldBm.size();ia++) {
					subBm =  subFieldBm.get(ia);
				}
				ArrayList<String> subFieldCm = sqlObj.selectSubFieldValuesByID(fieldID, "c");
				for (int ia=0;ia < subFieldCm.size();ia++) {
					subCm =  subFieldCm.get(ia);
				}
				ArrayList<String> subFieldDm = sqlObj.selectSubFieldValuesByID(fieldID, "d");
				for (int ia=0;ia < subFieldDm.size();ia++) {
					subDm =  subFieldDm.get(ia);
				}
				ArrayList<String> subFieldJm = sqlObj.selectSubFieldValuesByID(fieldID, "j");
				for (int ia=0;ia < subFieldJm.size();ia++) {
					subJm =  subFieldJm.get(ia);
				}
				
				String seperatorm = ", ";
				if (subAm  != null && subAm.length() > 0) {
					thisAuthorm = subAm;
					if (subBm != null && subBm.length() > 0 ) {
						thisAuthorm = thisAuthorm + seperatorm + subBm;
					} 
					if (subCm != null && subCm.length() > 0 ) {
						thisAuthorm = thisAuthorm + seperatorm + subCm;
					} 
					if (subDm != null && subDm.length() > 0 ) {
						thisAuthorm = thisAuthorm + seperatorm + subDm;
					} 
				} else {
					thisAuthorm = "Authored at Unknown Meeting";
				}
				
				thisAuthorm = fixAmper(thisAuthorm);
				
				if (subJm != null && subJm.length() == 0) {
					subJm = "AUT";
				}
				
				String upperJm = subJm.toUpperCase();
				retValm.add(upperJm);
				retValm.add(thisAuthorm);

				authorArray.add(retValm);
			}
			
			

			
			
/* TODO
 * 
 * FIELD 250 - Edition Statement

Subfield Codes
$a - Edition statement (NR)
$b - Remainder of edition statement (NR)
$3 - Materials specified (NR)
$6 - Linkage (NR)
$8 - Field link and sequence number (R)

XML ENTITY: rdau:P60329 editionStatement

String editionStatement = ""; // rdau:editionStatement

 * 			
 */
			
			
/* TODO
 * 
 * FIELD 260 - Imprint
 * 
Subfield Codes
$a - Place of publication, distribution, etc. (R)
$b - Name of publisher, distributor, etc. (R)
$c - Date of publication, distribution, etc. (R)
$e - Place of manufacture (R)
$f - Manufacturer (R)
$g - Date of manufacture (R)
$3 - Materials specified (NR)

XML ENTITY: dct:publisher

String editionStatement = ""; // dct:publisher

Subfield b can go into a <role:PBL>

Also see if I can parse into a relater role <role:PBL> 

*
*/
			
/* TODO
 * 
 * Field 264 - Production info (like imprint for manufactured goods)
 * 
 
Subfield Codes
$a - Place of production, publication, distribution, manufacture (R)
$b - Name of producer, publisher, distributor, manufacturer (R)
$c - Date of production, publication, distribution, manufacture, or copyright notice (R)
$3 - Materials specified (NR)

XML ENTITY: dct:publisher

String prodInfo = ""; // dct:publisher

Subfield b can go into a <role:CRE>

 * 
 */
			
/* TODO
 * 
 * Field 321 - Former Publication Frequency
 * 
 
Subfield Codes
$a - Former publication frequency (NR) 
$b - Dates of former publication frequency (NR) 
$6 - Linkage (NR) 
$8 - Field link and sequence number (R) 

XML ENTITY: rdau:P60129 noteOnFrequency

String formerPubFreq = ""; // rdau:noteOnFrequency

 * 
 */	
			
					
/* TODO
 * 
 * Field 300 - Physical Description
 * 
 
Subfield Codes
$a - Extent (R)
$b - Other physical details (NR)
$c - Dimensions (R)
$e - Accompanying material (NR)
$f - Type of unit (R)
$g - Size of unit (R)
$3 - Materials specified (NR)
$6 - Linkage (NR)
$8 - Field link and sequence number (R)

XML ENTITY: dct:format

String physDesc = ""; // dct:format

 * 
 */
			
			
/* TODO
 * 
 * Field 336 - Content Type
 * 
 
Subfield Codes
$a - Content type term (R)
$b - Content type code (R)
$0 - Authority record control number or standard number (R)
$2 - Source (NR)
$3 - Materials specified (NR)
$6 - Linkage (NR)
$8 - Field link and sequence number (R)

XML ENTITY: <dc:type>

String contentType = ""; // dct:type

* 
*/	
			

/* TODO
 * 
 * Field 337 - Media Type
 * 
 
Subfield Codes
$a - Media type term (R)
$b - Media type code (R)
$0 - Authority record control number or standard number (R)
$2 - Source (NR)
$3 - Materials specified (NR)
$6 - Linkage (NR)
$8 - Field link and sequence number (R)

XML ENTITY: dct:format

String mediaType = ""; // dct:format

* 
*/	

			
/* TODO
 * 
 * Field 338 - Carrier Type
 * 

Subfield Codes
$a - Carrier type term (R)
$b - Carrier type code (R)
$0 - Authority record control number or standard number (R)
$2 - Source (NR)
$3 - Materials specified (NR)
$6 - Linkage (NR)
$8 - Field link and sequence number (R)

XML ENTITY: dct:medium

String carrierType = ""; // dct:type

* 
*/	

			
/* TODO
 * 
 * Field 362 - Sequence of Dates
 * 

Subfield Codes
$a - Dates of publication and/or sequential designation (NR) 
$z - Source of information (NR) 
$6 - Linkage (NR) 
$8 - Field link and sequence number (R) 

XML ENTITY:

  <dc:date>
    <collex:date>
      <rdfs:label>1890-99 (circa)</rdfs:label>
      <rdf:value>189u</rdf:value>
    </collex:date>
  </dc:date>

* 
*/	


/* TODO
 * 
 * Field 370 - Associated Place
 * 

Subfield Codes
$c - Associated country (R)
$f - Other associated place (R)
$g - Place of origin of work or expression (R)
$i - Relationship information (R)
$s - Start period (NR)
$t - End period (NR)
$u - Uniform Resource Identifier (R)
$v - Source of information (R)
$0 - Authority record control number or standard number (R)
$2 - Source of term (NR)
$3 - Materials specified (NR)
$4 - Relationship (R)
$6 - Linkage (NR)
$8 - Field link and sequence number (R)

XML ENTITY: <dc:coverage> (make sure indexer is fixed to take multiple)

String associatedPlace = ""; // dc:coverage

* 
*/	

			
/* TODO
 * 
 * Field 388 - Time Period of Creation
 * 

Subfield Codes

$a - Time period of creation term (R)
$0 - Authority record control number or standard number (R)
$2 - Source of term (NR)
$3 - Materials specified (NR)
$6 - Linkage (NR)
$8 - Field link and sequence number (R)

XML ENTITY: dct:created

String creationEpoch = ""; // dct:created

* 
*/
			
/* TODO
 * 
 * Field 490 - Series Statement
 * 

Subfield Codes

$a - Series statement (R)
$l - Library of Congress call number (NR)
$v - Volume/sequential designation (R)
$x - International Standard Serial Number (R)
$3 - Materials specified (NR)
$6 - Linkage (NR)
$8 - Field link and sequence number (R)

XML ENTITY: treat like a 500

Array<String> seriesStatment   isbdu:P1041  hasNoteOnSeriesAndMultipartMonographicResources

* 
*/
			
			
			// do 600 (subject term - personal name)
			if (fieldType.equals("600")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				subFieldsToInclude.add("c");
				subFieldsToInclude.add("d");
				String thisSubjectString = getSubject(recordID, "600", subFieldsToInclude, " ");
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
				}

			}
			
			// do 610 (subject term - corporate name)
			if (fieldType.equals("610")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "610", subFieldsToInclude, " ");
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
				}

			}
			
			// do 611 (subject term - meeting)
			if (fieldType.equals("611")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				subFieldsToInclude.add("b");
				subFieldsToInclude.add("c");
				String thisSubjectString = getSubject(recordID, "611", subFieldsToInclude, " ");
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
				}

			}
			
			// do 630 (subject term - Uniform Title)
			if (fieldType.equals("630")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "630", subFieldsToInclude, " ");
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
				}

			}
			
			// do 648 (subject term - Chronological Term)
			if (fieldType.equals("648")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "648", subFieldsToInclude, " ");
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
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
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
				}

			}
			
			// do 651 (subject term - Geographic Name)
			if (fieldType.equals("651")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "651", subFieldsToInclude, " ");
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
				}

			}
			
			/* TODO
			 * 
			 * Field 752 same as 651 but hierarchical.  Break up into individual entries.
			 * 

			XML ENTITY: same as above

			* 
			*/	
	
			
			
			
			// do 653 (subject term - Uncontrolled)
			if (fieldType.equals("653")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "653", subFieldsToInclude, " ");
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
				}

			}
			
			// do 654 (subject term - Faceted topical term)
			if (fieldType.equals("654")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				subFieldsToInclude.add("b");
				String thisSubjectString = getSubject(recordID, "654", subFieldsToInclude, " ");
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
				}

			}
			
			// do 656 (subject term - Occupation)
			if (fieldType.equals("656")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "656", subFieldsToInclude, " ");
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
				}

			}
			
			// do 657 (subject term - Function)
			if (fieldType.equals("657")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				String thisSubjectString = getSubject(recordID, "657", subFieldsToInclude, " ");
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
				}

			}
			
			// do 658 (subject term - Curriculum Objective)
			if (fieldType.equals("658")) {
				ArrayList<String> subFieldsToInclude = new ArrayList<String>();
				subFieldsToInclude.add("a");
				subFieldsToInclude.add("b");
				String thisSubjectString = getSubject(recordID, "658", subFieldsToInclude, " ");
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
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
				if (thisSubjectString != null && thisSubjectString.length() > 0) {
					subjectTerms.add(fixAmper(thisSubjectString));
				}

			}
			
			// digital surrogates
			if (fieldType.equals("856")) {
				surrogateSub = sqlObj.selectSubFieldValuesByID(fieldID, "u");
			}
			
			// collex:genre
			if (fieldType.equals("655")) {
				String workingValue = "";
				// get raw value
				String rawValue = sqlObj.getFieldByID(fieldID);;
				// get subfields
				String subA = "";
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int iag=0;iag < subFieldA.size();iag++) {
					subA = subFieldA.get(iag);
				}
				if (subA != null && subA.length() > 0) {
					workingValue = subA;
				} else if (rawValue != null && rawValue.length() > 0) {
					workingValue = rawValue;
				}
				
				// remove trailing period
				genre = fixAmper(workingValue);
			}
			
			// dc:coverage
			if (fieldType.equals("751")) {
				String workingValue = "";
				// get raw value
				String rawValue = sqlObj.getFieldByID(fieldID);;
				// get subfields
				String subA = "";
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int iag=0;iag < subFieldA.size();iag++) {
					subA = subFieldA.get(iag);
				}
				if (subA != null && subA.length() > 0) {
					workingValue = subA;
				} else if (rawValue != null && rawValue.length() > 0) {
					workingValue = rawValue;
				}
				coverage.add(fixAmper(workingValue));
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
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int iag=0;iag < subFieldA.size();iag++) {
					subA = fixAmper(subFieldA.get(iag));
				}
				ArrayList<String> subFieldB = sqlObj.selectSubFieldValuesByID(fieldID, "b");
				for (int iag=0;iag < subFieldB.size();iag++) {
					subB = fixAmper(subFieldB.get(iag));
				}
				ArrayList<String> subFieldC = sqlObj.selectSubFieldValuesByID(fieldID, "c");
				for (int iag=0;iag < subFieldC.size();iag++) {
					subC = fixAmper(subFieldC.get(iag));
				}	
				ArrayList<String> subFieldD = sqlObj.selectSubFieldValuesByID(fieldID, "d");
				for (int iag=0;iag < subFieldD.size();iag++) {
					subD = fixAmper(subFieldD.get(iag));
				}
				ArrayList<String> subFieldF = sqlObj.selectSubFieldValuesByID(fieldID, "f");
				for (int iag=0;iag < subFieldF.size();iag++) {
					subF = fixAmper(subFieldF.get(iag));
				}
				ArrayList<String> subFieldG = sqlObj.selectSubFieldValuesByID(fieldID, "g");
				for (int iag=0;iag < subFieldG.size();iag++) {
					subG = fixAmper(subFieldG.get(iag));
				}
				ArrayList<String> subFieldH = sqlObj.selectSubFieldValuesByID(fieldID, "h");
				for (int iag=0;iag < subFieldH.size();iag++) {
					subH = fixAmper(subFieldH.get(iag));
				}
				
				String seperator = "--";
				boolean notFirst = false;
				if (subA != null && subA.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subA;
					notFirst = true;
				}
				if (subB != null && subB.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subB;
					notFirst = true;
				}
				if (subC != null && subC.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subC;
					notFirst = true;
				}
				if (subD != null && subD.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subD;
					notFirst = true;
				}
				if (subF != null && subF.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subF;
					notFirst = true;
				}
				if (subG != null && subG.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subG;
					notFirst = true;
				}
				if (subH != null && subH.length() > 0) {
					if (notFirst) {
						workingValue = workingValue + seperator;
					}
					workingValue = workingValue + subH;
					notFirst = true;
				}
					
					
				if ((workingValue != null) && (workingValue.length() == 0) && (rawValue != null) &&  (rawValue.length() > 0)) {
					workingValue = rawValue;
				}
				
				if (workingValue != null && workingValue.length() > 0) {
					String trimmedValue = removeFinalComma(workingValue);
					coverage.add(fixAmper(trimmedValue));
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
				}
				
				if (note != null && note.length() > 0) {
					fiveHundNotes.add(fixAmper(note));
				} else if (rawValue != null && rawValue.length() > 0) {
					fiveHundNotes.add(fixAmper(rawValue));
				}
			}
			
/* TODO
			501 - same as 500
			504 - same as 500
			505 - same as 500
			509 - same as 500
*/
			
			// 510 notes -- Indexing service that indexes the item
			if (fieldType.equals("510")) {
				// get raw value
				String rawValue = sqlObj.getFieldByID(fieldID);
				// get subfields
				String subA = "";
				String subC = "";
				ArrayList<String> subFieldA = sqlObj.selectSubFieldValuesByID(fieldID, "a");
				for (int in=0;in < subFieldA.size();in++) {
					subA = subFieldA.get(in);
				}
				ArrayList<String> subFieldC = sqlObj.selectSubFieldValuesByID(fieldID, "c");
				for (int in=0;in < subFieldC.size();in++) {
					subC = subFieldC.get(in);
				}
				
				String retString = "";
				if (subA != null && subA.length() > 0) {
					if (subC != null && subC.length() > 0) {
						retString = subA + " " + subC;
					} else {
						String trimmedSubA = removeFinalComma(subA);
						retString = trimmedSubA;
					}
				} else {
					retString = rawValue;
				}
	
				fiveHundTenNotes.add(fixAmper(retString));
			}
			
/* TODO
			515 -- all as 550's
			520
			521
			522
			525
			530
			533
			534
			535
			539
			546
			550
			555
			561
*/

		}
		
		// build the content part of the RDF
		rdfString = rdfString + "        <dct:title>" + finalTitle + "</dct:title>\n";
		
		for (int iat=0;iat < authorArray.size();iat++) {
			ArrayList<String> thisCont = authorArray.get(iat);
			rdfString = rdfString + formatContributor(thisCont.get(0), thisCont.get(1));
		}
		
		if (finalDate != null && finalDate.length() > 0) {
			rdfString = rdfString + "        <dc:date>\n             <collex:date>\n";
			rdfString = rdfString + "                  <rdfs:label>" + finalDateString + "</rdfs:label>\n";
			rdfString = rdfString + "                  <rdfs:value>" + finalDateString + "</rdfs:value>\n";
			rdfString = rdfString + "             </collex:date>\n        </dc:date>\n";
			
		}
		if (coverage != null && coverage.size() > 0) {
			for (int ic=0;ic < coverage.size();ic++) {
				rdfString = rdfString + "        <dc:coverage>" + coverage.get(ic) + "</dc:coverage>\n";
			}
		}
		if (genre != null && genre.length() > 0) {
			rdfString = rdfString + "        <collex:genre>" + genre + "</collex:genre>\n";
		}
		if (subjectTerms != null && subjectTerms.size() > 0) {
			for (int ist=0;ist < subjectTerms.size();ist++) {
				rdfString = rdfString + subjectTerms.get(ist);
			}
		}
		if (fiveHundTenNotes != null && fiveHundTenNotes.size() > 0) {
			for (int isn=0;isn < fiveHundTenNotes.size();isn++) {
				rdfString = rdfString + "        <dct:isReferencedBy>" + fiveHundTenNotes.get(isn) + "</dct:isReferencedBy>\n";
			}
		}
		if (fiveHundNotes != null && fiveHundNotes.size() > 0) {
			for (int isn=0;isn < fiveHundNotes.size();isn++) {
				rdfString = rdfString + "        <dct:description>" + fiveHundNotes.get(isn) + "</dct:description>\n";
			}
		}
		
		// put loop to build holding records here
		ArrayList<String> children = new ArrayList<String>();
		ArrayList<HashMap<String,String>> holdingRecords = sqlObj.selectHoldingRecordIDs(itemID);
		int ihr = 0;
		while (ihr < holdingRecords.size()) {
			String uniqueHoldingID = "";
			HashMap<String,String> holdingRecordResults = holdingRecords.get(ihr);
			int holdingRecordID = Integer.parseInt(holdingRecordResults.get("id"));
			// now get all the 852 fields for the record
			ArrayList<Integer> eightFiftyTwos = sqlObj.selectEightFiftyTwoFields(holdingRecordID);
			int ihf = 0;
			while (ihf < eightFiftyTwos.size()) {
				int eightFiftyTwofieldID = eightFiftyTwos.get(ihf);
				ArrayList<HashMap<String,String>> holdingSubs  = sqlObj.selectAllSubfields(eightFiftyTwofieldID);
				// get the a subfield value (Location - in form of library code)
				String aVal = fixAmper(getSubfieldValue(holdingSubs, "a"));
				// get the b subfield value (Sublocation or collection)
				String bVal = fixAmper(getSubfieldValue(holdingSubs, "b"));
				// get the e subfield value (Address)
				String eVal = fixAmper(getSubfieldValue(holdingSubs, "e"));
				// get the j subfield value (Shelving Control Number)
				String jVal = fixAmper(getSubfieldValue(holdingSubs, "j"));
				// get the x subfield value (non public note)
				String xVal = fixAmper(getSubfieldValue(holdingSubs, "x"));
				// get the r subfield value (unique id)
				String rVal = fixAmper(getSubfieldValue(holdingSubs, "r"));
				uniqueHoldingID = rVal;
				// get list of q values (physical location)
				ArrayList<String> qVals = getSubfieldValueList(holdingSubs, "q");
				
				
				// get the unique holding info
				String uniqueHRI = "http://" + domainURI + "/" + rVal;
				String rdfAboutHolding = "    <estc:estc rdf:about=\"" + uniqueHRI + "\">\n";
				children.add(uniqueHRI);
				String rdfStringAdditions = "";
				int ihq = 0;
				while (ihq < qVals.size()) {
					String subLocationValue = qVals.get(ihq);
					if (subLocationValue != null && subLocationValue.length() > 0) {
						rdfStringAdditions = rdfStringAdditions + "        <dct:description>" + subLocationValue + "</dct:description>\n";
					}
					ihq++;
				}
				rdfStringAdditions = "        <role:OWN>" + aVal + "</role:OWN>\n";
				rdfStringAdditions = rdfStringAdditions + "        <role:RPS>" + bVal + "</role:RPS>\n";
				rdfStringAdditions = rdfStringAdditions + "        <bf:shelfMark>" + jVal + "</bf:shelfMark>\n";
								
				// now construct an rdf output for this holding:
				String holdingRDF = rdfHeader + rdfAboutHolding + rdfString + rdfStringAdditions + parentAssoc + rdfFooter;
				
				// TODO: Write out Holding RDF
				try {
					// write out the rdf
					PrintWriter holdingWriter = new PrintWriter( configObj.writeDir + "/hold_" + uniqueHoldingID + ".rdf", "UTF-8");
					holdingWriter.print(holdingRDF);
					holdingWriter.close();
					
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					System.out.println("Error exporting record " + holdingRecordID + " holding item " + uniqueHoldingID);
					e.printStackTrace();
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					System.out.println("Error exporting record " + holdingRecordID + " holding item " + uniqueHoldingID);
					e.printStackTrace();
				}
				
				//System.out.println(holdingRDF);
				System.out.println("Processed holding record " + holdingRecordID + " item " + uniqueHoldingID);

				// need to keep this so that the loop works right
				ihf++;
			}
			
			// mark the holding record as exported
			sqlObj.updateExported(holdingRecordID);
			ihr++;
		
		}
		
		// add child associations if any
		int ihch = 0;
		while (ihch < children.size()) {
			rdfString = rdfString + "        <bf:hasInstance>" + children.get(ihch) + "</bf:hasInstance>\n";
			ihch++;
		}
		
		
		/* TODO
		 * digSur never gets set.  Need to feed field 856 into here
		 */

		// add digital surrogates
		for (int ids=0;ids < surrogateSub.size();ids++) {
			String digSur = surrogateSub.get(ids);
			if (digSur != null && digSur.length() > 0) {
				rdfString = rdfString + "        <scm:url>" + digSur + "</scm:url>\n";
			}
		}
		
		String bibRDF = rdfHeader + rdfAbout + rdfString + rdfFooter;
		
		// TODO: write out bib rdf
		try {
			PrintWriter bibWriter = new PrintWriter( configObj.writeDir + "/bib_" + itemID + ".rdf", "UTF-8");
			bibWriter.print(bibRDF);
			bibWriter.close();
			// if i'm here than we didn't throw an error so 
			// so mark the record as exported
			sqlObj.updateExported(recordID);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("Error exporting record " + recordID + " holding item " + itemID);
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			System.out.println("Error exporting record " + recordID + " holding item " + itemID);
			e.printStackTrace();
		}
		
		// System.out.println(bibRDF);
		System.out.println("Processed bib record " + recordID + " item " + itemID);
		
		return ret;
	}
	
	// create an RDF string for a resource
	public String getSubject(int recordID, String field, ArrayList<String> subFields, String separator) {
		String ret = "";
		String workingString = "";
		
		// get raw value and field id
		String rawValue = sqlObj.getFieldByNumber(recordID, field);
		ArrayList<HashMap<String,String>> RetInfo = sqlObj.getFieldInfo(recordID, field);
		String FieldID = "";
		for (int sfa=0;sfa < RetInfo.size();sfa++) {
			HashMap<String,String> thisHash = RetInfo.get(sfa); 
			FieldID = thisHash.get("id");
		}
		
		if (FieldID != null && FieldID.length() > 0) {
			
			int FieldIdAsInt = Integer.parseInt(FieldID);
			
			for (int ia=0;ia < subFields.size();ia++) {
				String localWorking = "";
				ArrayList<String> subFieldIt = sqlObj.selectSubFieldValuesByID(FieldIdAsInt, subFields.get(ia));
				for (int iab=0;iab < subFieldIt.size();iab++) {
					localWorking = subFieldIt.get(iab);
				}
				if (localWorking != null && localWorking.length() > 0) {
					if (workingString != null && workingString.length() > 0) {
						workingString = workingString + separator;
					}
					workingString = workingString + localWorking;
				}
			}			
			
		}
		
		if ((workingString != null) && (workingString.length() == 0) && (rawValue != null) && (rawValue.length() > 0)) {
			workingString = rawValue;
		}
		
		if (workingString != null && workingString.length() > 0) {
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
	
	public String getSubfieldValue(ArrayList<HashMap<String,String>> thisRow, String subfield) {
		String retVal = null;
		int isfr = 0;
		while (isfr < thisRow.size()) {
			HashMap<String,String> holdingRecordSubFieldsRes = thisRow.get(isfr);
			String subfieldvalue = holdingRecordSubFieldsRes.get("subfield");
			if (subfieldvalue.equals(subfield)) {
				retVal = holdingRecordSubFieldsRes.get("value");
			}
			isfr++;
		}
		
		return retVal;
		
	}
	public ArrayList<String> getSubfieldValueList(ArrayList<HashMap<String,String>> thisRow, String subfield) {
		ArrayList<String> retVals = new ArrayList<String>();
		int isfr = 0;
		while (isfr < thisRow.size()) {
			HashMap<String,String> holdingRecordSubFieldsRes = thisRow.get(isfr);
			String subfieldvalue = holdingRecordSubFieldsRes.get("subfield");
			if (subfieldvalue.equals(subfield)) {
				retVals.add(fixAmper(holdingRecordSubFieldsRes.get("value")));
			}
			isfr++;
		}
		
		return retVals;
		
	}
	
	public static String fixAmper(String str) {
		String retStr = "";
		if ( str != null && str.length() > 0) {
			retStr = str.replace("&", "&amp;");
		}
		return retStr;
	}

}
