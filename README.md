**DESCRIPTION:**

_Coming soon.._

**SYNTAX:**

<code>Rscript <file_script.R> (-d <DB_PATH> | -f <DBs_FOLDER>)</code>

**OPTIONS:**

  <code>-d</code> for SINGLE DB run followed by path to the database
  
  <code>-f</code> for ALL DBs run on folder containing the dbs followed by path to the folder


**Example of a single database run:**
  
<code>Rscript data_extractor.R -d ./output </code>

The line above will run data_extractor.R on a single database inside output folder located in current directory

**Example of a multiple databases run:** 
  
<code>Rscript data_extractor.R -f ./set_of_dbs</code>

The line above will run data_extractor.R on a all databases inside set_of_dbs folder located in current directory

**Note:**
database name must start with "database_" in order to by recognized. For example: _database_5p4_nze01_
