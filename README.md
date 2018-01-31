# swq_viz_ingestion

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
