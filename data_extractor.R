#!/usr/bin/env Rscript
# Author  : Fahad Alswaina
# Github  : github.com/alswaina
# Email   : alswaina.fahad@gmail.com

#TODO:



#LIBRARIES ----------------------------------------------------------------------------------
library(rgcam)

#FUNCTIONS -----------------------------------------------------------------------
main <- function(db.path, execution.type, queries_xml, output.path, MAIN.QUERY, QUERY.BY){
  #default MAIN QUERY
  #MAIN.QUERY <- normalizePath(MAIN.QUERY)
  print_headerExecution(top = T)
  
  if(execution.type =="db"){ #run on a single db
    #header
    header <- basename(db.path)
    print_headerDB(header=basename(db.path), top = T)
    #conn
    myconn <- get_db_conn(db.path=db.path)

    if(is.null(myconn)){
      break
    }

    scenario.name <- get_recent_scenario(myconn=myconn)
    #env
    print_env(db.path = db.path, MAIN.QUERY = MAIN.QUERY, scenario.name = scenario.name)
    #processing
    process_queries(myconn, db.path, MAIN.QUERY, queries_xml, scenario=scenario.name, QUERY.BY=QUERY.BY, output.path=output.path)
    #end
    print_headerDB(header=NULL, top = F)
  }
  else if(execution.type == "recursive"){ #run on a set of dbs
    list_dbs <- list.dirs(path=db.path, recursive = F, full.names = T)
    list_dbs <- grep(pattern = ".*(database_)", x = list_dbs, value = TRUE)
    dbs_count <- length(list_dbs)
    counter <- 0
    for(db.path in list_dbs){
      counter <- counter + 1
      db.path <- normalizePath(db.path)
      #header
      print_headerDB(header=paste0(basename(db.path), " - ", counter, "/", dbs_count), top = T)
      #conn
      myconn <- get_db_conn(db.path=db.path)

      if(is.null(myconn)){
        break
      }

      scenario.name <- get_recent_scenario(myconn=myconn)
      #env
      print_env(db.path = db.path, MAIN.QUERY = MAIN.QUERY, scenario.name = scenario.name)
      #processing
      process_queries(myconn, db.path, MAIN.QUERY, queries_xml, scenario=scenario.name, QUERY.BY=QUERY.BY, output.path=output.path)
      #end
      print_headerDB(header=NULL, top = F)
    }
  }
  print_headerExecution(top = F)
}

get_table <- function(myconn, query, scenario){
  if(is.null(REGIONS)){REGIONS <- NULL}
  noquote(" ")
  success <- F
  
  table <- tryCatch(
    {
      table <- runQuery(dbConn=myconn, query=query, scenario=scenario, regions=REGIONS)
      cat("+ Query proccessed Successfully!\n")
      success <- T
      table
    }, 
    error=function(cond){
      cat(paste("- Error on query\n"))
      cat(paste(cond, "\n"))
      table <- NULL
    }, 
    warning=function(cond){
      cat("- Warning on query\n")
      cat(paste(cond, "\n"))
      table <- NULL
    }
  )
  
  if(!success){
    return(table)
  }
  
  table <- tryCatch(
    {
      table <- reshape2::dcast(table, ... ~ year)
      cols <- names(table)
      colx_reorder <- c(cols[-1], cols[1])
      table <- table[colx_reorder]
      cat("+ Data proccessed Successfully!\n")
      table
    }, 
    error=function(cond){
      cat("- Error when processing Data\n")
      cat(paste(cond, "\n"))
      return(NULL)
    }, 
    warning=function(cond){
      cat("- Warning when processing Data\n")
      cat(paste(cond, "\n"))
      return(NULL)
    }
  )
  return(table)
}

#queries
process_queries <- function(myconn, db.path, MAIN.QUERY, queries_xml, scenario, QUERY.BY, output.path){
  for(query.counter in names(queries_xml)){
    if(!as.integer(query.counter) %in% ONLY_RUN){
      next
    }
    #output.filename: <Q#>_<db_foldername>
    output.filename <- paste0("Q", query.counter, "_", basename(db.path), ".csv")

    query.title <- names(queries_xml[[query.counter]])
    query.xml <- queries_xml[[query.counter]][[query.title]]
    
    query.query <- buid_query(query.title=query.title, MAIN.QUERY=MAIN.QUERY)
    
    process_query(myconn, scenario = scenario,
                  output.filename=output.filename, output.path=output.path,
                  QUERY.BY=QUERY.BY, query.xml=query.xml, query.query= query.query, query.title=query.title,
                  query.counter=query.counter)
    cat("---\n")
  }
}

process_query <- function(myconn, scenario, output.filename, output.path, QUERY.BY, query.xml, query.query, query.title, query.counter){
  print_headerQuery(query.counter, query.title)
  query <- NULL
  if(QUERY.BY == "title"){
    query <- query.query
  }else if(QUERY.BY == "xml"){
    query = query.xml
  }
  
  if(!is.null(query)){
    result <- get_table(myconn, query, scenario=scenario)
    write_output(result = result, output.filename = output.filename, output.path = output.path)
  }else{
    print("ERROR IN DETECTING THE QUERY!")
  }
}

buid_query <- function(query.title, MAIN.QUERY){
  query <- paste0("doc('", MAIN.QUERY, "')//*[@title='", query.title, "']")
  query
}

#output
write_output <- function(result, output.filename, output.path){
  #<db_path>/<output.filename>_<db_name>
  
  if(!is.null(result)){
    output.filepath <- paste0(output.path,"/", output.filename)
    write.csv(result, file = output.filepath, row.names = FALSE)
    print(paste0("OUTPUT: ", output.filepath))
  }else{
    print("OUTPUT: NOT GENERATED")
  }
  noquote(" ")
}

#connections
get_db_conn <- function(db.path){
  myconn <- NULL
  try(
    {
      myconn <- localDBConn(dbPath = dirname(db.path), dbFile = basename(db.path))
    }, silent = T)
  
  if(is.null(myconn)){
    cat(paste0("ERROR - CANNOT CONNTECT TO DB: ", basename(db.path), "\n"))
  }
  myconn
}

get_recent_scenario <- function(myconn){
  lastScenarioRunInfo <- tail(listScenariosInDB(myconn), n=1)
  lastScenarioLongname <- lastScenarioRunInfo["fqName"]$fqName
  lastScenarioLongname
}

#prints
print_headerDB <- function(header, top){
  if(top){
    cat(paste0("\n*-*-* [ DB: ",header," ] *-*-*", "\n\n"))
  }
  else{
    cat(paste0("\n*-*-*", "\n"))
  }
}

print_headerExecution <- function(top){
  if(top){
    exec.decoration <- paste0("\n--------------------------------[", "EXECUTION STARTED", "]--------------------------------", "\n")
    cat(exec.decoration)
  }
  else{
    exec.decoration <- paste0("\n--------------------------------[", "EXECUTION FINISHED", "]--------------------------------", "\n")
    cat(exec.decoration)
  }
}

print_headerQuery <- function(query.counter, query.title){
  query.decoration <- paste0("\nQUERY_", query.counter, ": ", query.title, "\n")
  cat(query.decoration)
}

print_env <- function(db.path, MAIN.QUERY, scenario.name){
  #DB_PATH
  print(paste0("DB_PATH: ", db.path))
  
  #MAIN_QUERY
  print(paste0("MAIN_QUERY: ", MAIN.QUERY))
  
  #SCENARIO
  print(paste0("ON_SCENARIO: ", scenario.name))
  noquote(" ")
}



#EXECUTION ------------------------------------------------------------------------
tryCatch(
  {

    #validate configuration file
    validation.check <- function(){
      validation_config <- function(val) {
        tryCatch({
          val
        },
        error = function(c) {stop("Config.R is not valide", call. = F)},
        warning = function(c) {stop("Config.R doesn't exist", call. = F)})
      }
      validation_path <- function(val, path_type) {
        tryCatch({
          val <- normalizePath(val)
          if(path_type=='file'){
            if(!file.exists(val) | dir.exists(val)){
              stop(paste("File:", val, "- doesn't exsist or not a file!"), call. = F)
            }
            
          }else if(path_type=='folder'){
            
            if(!file.exists(val) | !dir.exists(val)){
              stop(paste("File:", val, "- doesn't exsist or not a folder"), call. = F)
            }
          }
          val
        },
        error=function(cond){
          stop(cond$message, call. = F)
        },
        warning=function(cond){
          stop(cond$message, call. = F)
        })
        return(val)
      }
      validation_variables_stop<- function(val, name){
        if(!length(val)){
          stop(paste("Please proide values in", name), call. = F)
        }
      }
      validation_variables_warning<- function(val, name){
        if(!length(val)){
          warning(paste("Empty value:", name), call. = F)
        }
      }
      validation_args <- function(args){
        # test if there is at least one argument: if not, return an error
        if (length(args)==0) {
          stop("Please provide the DB_PATH or DBs_FOLDER as following: Rscript <file_script.R> [-d <DB_PATH> | -f <DBs_FOLDER>]", call.=FALSE)
        } else if (length(args)==2) {
          # default output file
          if (args[1] == "-d"){
            execution.type <- "db"
            db.path <- args[2]
          }else if(args[1] == "-f"){
            execution.type <- "recursive"
            db.path <- args[2]
          }
        } else{
          stop("Please provide only one arguament to Rscript as following: Rscript <Rscript.R> [-d <DB_PATH> | -f <DBs_FOLDER>]", call.=FALSE)
        }
        return(
          list(
            "execution.type" = execution.type, 
            "db.path" = db.path
          )
        )
      }

      #terminal input
      input.full <- commandArgs(trailingOnly = FALSE)
      args <- commandArgs(trailingOnly=TRUE)

      #args validation
      result.input <- validation_args(args)

      execution.type <-  result.input[["execution.type"]]

      db.path <-  result.input[["db.path"]]

      db.path <- validation_path(db.path, path_type = 'folder')

      #extract script path
      file.arg.name <- "--file="
      script.path <- sub(file.arg.name, "", input.full[grep(file.arg.name, input.full)])
      script.path <- normalizePath(script.path)
      
      #Validate & load configuration file
      validation_config(source("./config.R"))
      source("./config.R")
      
      MAIN.QUERY <- validation_path(MAIN.QUERY, path_type = "file")
      validation_variables_stop(ONLY_RUN, name="config/ONLY_RUN")
      validation_variables_warning(REGIONS, name="config/REGIONS")
      validation_variables_stop(queries_xml, name="config/queries_xml")
      
      return(
        list(
          "script.path" = script.path,
          "db.path" = db.path, 
          "execution.type" = execution.type
        )
      )
    }
    validated.values <- validation.check()

    #script info
    script.path <- validated.values[["script.path"]]
    script.name <- basename(script.path)
    script.dir <- dirname(script.path)

    execution.type <- validated.values[["execution.type"]]
    
    #folder/db info
    db.path <- validated.values[["db.path"]]
    db.name <- basename(db.path)
    
    #timing
    Saudi.time <- as.POSIXlt(Sys.time(), tz="Asia/Riyadh")
    curr_time <- paste("TIMESTAMP:", Saudi.time)
    
    #output dir
    time_formated <- format(Saudi.time, "%b_%d__%H_%M")
    output.name <- paste0("output_", db.name,"__", time_formated)
    output.path <- paste0(script.dir, "/", output.name)
    
    dir.create(path = output.path)
    
    #logging
    log.path <- paste0(output.path, "/", "logFile.txt")
    log.file <- file(log.path) # File name of output log
    sink(log.file, append = TRUE, type = "output", split = T) # Writing console output to log file
    print(paste("LOG.PATH:", log.path))

    print(curr_time)
    print(paste("OUTPUT.DIR:", output.path))
    
    if(is.null(QUERY.BY)){
      QUERY.BY <- "title" 
      }
    
    main(db.path=db.path, execution.type=execution.type, queries_xml=queries_xml, output.path=output.path, MAIN.QUERY=MAIN.QUERY, QUERY.BY=QUERY.BY)
  }, 
  error=function(cond){
    err = paste("-",cond, "\n")
    cat(err)
  }, 
  warning=function(cond){
    warr = paste("-",cond, "\n")
    cat(warr)
  },
  finally={
    closeAllConnections() # Close connection to log file
  }
)