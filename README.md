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
1. ```create database estcimport_dev;``` to create the database
1. Importing single .sql file into MySQL database: ```mysql -u root -p estcimport_dev < estcimport_dev_estc_agents.sql ```
1. Importing multiple .sql files into MySQL database: ```cat *.sql | mysql -u root -p estcimport_dev```

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

-debug -exportJena

The overall process works like:

Listner.class -> ProcessManager.class -> ExportJenaRdf.class

I've commented ExportJenaRD extensively, so you 
should have no problem figuring out where to dive in.
