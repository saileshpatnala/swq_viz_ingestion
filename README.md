# swq_viz_ingestion

## Apache Jena Fuseki Database setup (local server)
**Installing on MacOS**
1. Install Java and JDK version 8
1. Download .zip file from https://jena.apache.org/download/ for Apache Jena Fuseki
1. Unzip the zipped folder ```unzip apache-jena-fuseki-3.6.0.zip```
1. ```cd apache-jena-fuseki-3.6.0```
1. Run server standalone :
```./fuseki-server --update --mem /dataset_name```
1. Returns port number that it’s running on (e.g. 3030)
1. Go on web browser and type: ```localhost: <port number>```

## MySQL Database setup (local server)
**Installing on MacOS**
1. ```brew install mysql```
1. Set up MySQL root access account: ```mysqladmin -u root password "<yourpassword>"```
1. Stopping MySQL server: ```mysql.server stop```
1. Running MySQL: ```mysql -u root -p```
1. It will ask you to enter the password you set for the server
1. Create the database ```create database estcimport_dev;```
1. cd into folder with sql files
1. Importing single .sql file into MySQL database: ```mysql -u root -p estcimport_dev < <name>.sql```
1. Importing multiple .sql files into MySQL database (from unix command line): ```cat *.sql | mysql -u root -p estcimport_dev```

## Running Java App to export RDF files -> Port into Jena Fuseki
**Using Eclipse for Java App**
1. clear your ```testrdf``` write directory in your workspace folder of java app, create one if not already done so
1. start up sql server: ```mysql.server start```
1. run java app via eclipse IDE
1. quit partway ```ctrl+C``` (for testing purposes, don't want to export all RDF files)
1. start up Apache Jena Fuseki
1. upload RDF files into Apache Jena Fuseki 
1. add all prefixes into SPARQL query (example below)
1. Run SPARQL query **(fixes still need to be made)**
```SPARQL
prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
prefix gl: <http://bl.uk.org/schema#>
prefix bf: <http://bibframe.org/vocab/>
prefix collex: <http://www.collex.org/schema#>
prefix dc: <http://purl.org/dc/elements/1.1/#>
prefix dct: <http://purl.org/dc/terms/#>
prefix estc: <http://estc21.ucr.edu/schema#>
prefix foaf: <http://xmlns.com/foaf/0.1/#>
prefix geo: <http://www.w3.org/2003/01/geo/wgs84_pos/#>
prefix isbdu: <http://iflastandards.info/ns/isbd/unc/elements/>
prefix rdau: <http://rdaregistry.info/Elements/u/#>
prefix reg: <http://metadataregistry.org/uri/profile/RegAp/#>
prefix relators: <http://id.loc.gov/vocabulary/relators/>
prefix role: <http://www.loc.gov/loc.terms/relators/>
prefix scm: <http://schema.org/>
prefix skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?subject ?predicate ?object
WHERE {
 ?subject ?predicate ?object
}

```

## Setting up write directory in Java App
1. update ```config.yml``` and set ```writedir: <file_path>/swq_viz_ingestion/testrdf```

## Java App Ingestion

Before you run, you'll need to properly configure the
db information and also a write directory in config.yml.

There's an estc_dev.zip file in the Google Drive.  This
is the database it talks to, with data.  Once you get the 
db up and running, you might want to clean out some records
so that you don't have wait through queries of the entire db
every time. Up do you.  Rembmeber that the basic structure
of the db is records -> feilds -> subfields.  (But you 
shouldn't have to do anything with the db other than make
sure it is on a server that is accessible to the java app.)

When you run, run with these arguments passed in:

```-debug -exportJena```

The overall process works like:

Listner.class -> ProcessManager.class -> ExportJenaRdf.class

I've commented ExportJenaRD extensively, so you 
should have no problem figuring out where to dive in.
