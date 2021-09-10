### DESCRIPTION

_Coming soon.._

### Part 1: Script

**SYNTAX:**

<code>Rscript <file_script.R> (-d <DB_PATH> | -f <DBs_FOLDER>)</code>

**OPTIONS:**

  <code>-d</code> for SINGLE DB run followed by the path to the database
  
  <code>-f</code> for FOLDER that contains multiple dbs followed by the path to the folder


**Example of a single database run:**
  
<code>Rscript data_extractor.R -d ./output </code>

The line above will run data_extractor.R on a single database inside output folder located in current directory of the script

**Example of a multiple databases run:** 
  
<code>Rscript data_extractor.R -f ./set_of_dbs</code>

The line above will run data_extractor.R on a all databases inside set_of_dbs folder located in current directory of the script

**Note:**
database name must start with "database_" in order to by recognized. For example: _database_5p4_nze01_
  
### Part 2: Configuration File
  
***QUERIES***: 
  
  Queries title and XML are listed in the list queries_xml with the following structure: 
  
  <code> queries_xml <- list( "<numberStr>" = list (<query.title> = <query.xml>), ...) </code>
  
Query title is used when are querying the Main.Query using the title to fetch the XML query in file. Or we can use the xml query directly in the list. The former method is prefered as it is cleaner. The latter more prone to human error. To choose between the title or XML to construct the query, we configure Query.BY.
    
<code> Query.BY <- {"title", "xml"} </code>

  
- "title": (default) use the title provided in queries_xml list to extract the query's xml from Main_queries.xml. 
- "xml": use the the actual xml query provided in queries_xml list to run the query 
 

MAIN.QUERY a path to the main queries xml file to read the query from. Uses forward-slash path seperator i.e. "/". Note: this variable is mandatory.


<code> MAIN.QUERY <- "/path/to/Main_queries.xml"</code>


To select the queries to run from the list queries_xml, configure the vector SELECTED.QUERIES by including the queries numbers. Assigning c() or NULL means include all queries in the list (default)
                  
Example: <code>SELECTED.QUERIES <- c(7, 9)</code> to run queries 7 and 9
  
Example: <code>SELECTED.QUERIES <- c(1:18)</code> for a range of numbers
    
To select subset of the databases in the folder, list the names of the databases or leave empty to run all folders:

Example: <code>SELECTED.DBs <- c("database_02", ..)</code> runs query on db name database_02

Example: <code>SELECTED.DBs <- c() or NULL </code> for all dbs in the folder (default)

To specify the regions to query, lists the regions (including Global) or leave empty (c() or NULL) to run all regions:

Example: <code> REGIONS <- c('USA', 'Canada') </code> for USA and Canada regions.
    
Example: <code> REGIONS <- c('Global') </code> for Global region (default)


### Part 3: Logging

    Coming soon..
    
### Part 4: Output
    
    Coming soon..
