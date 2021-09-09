#!/usr/bin/env Rscript
# Author  : Fahad Alswaina
# Github  : github.com/alswaina
# Email   : alswaina.fahad@gmail.com

#TODO:


#LIBRARIES ----------------------------------------------------------------------------------
library(rgcam)

#FUNCTIONS -----------------------------------------------------------------------
main <- function(db.path, execution.type, queries_xml, output.path, MAIN.QUERY, QUERY.BY){
  print_headerExecution(top = TRUE)
  
  if(execution.type == "db"){ #run on a single db
    #header
    print_headerDB(header = basename(db.path), top = TRUE)
    #conn
    myconn <- get_db_conn(db.path = db.path)

    if(is.null(myconn)){
      break
    }

    scenario.name <- get_recent_scenario(myconn=myconn)
    #env
    print_env(db.path = db.path, MAIN.QUERY = MAIN.QUERY, scenario.name = scenario.name)
    #processing
    process_queries(myconn, db.path, MAIN.QUERY, queries_xml, scenario = scenario.name, QUERY.BY = QUERY.BY, output.path = output.path)
    #end
    print_headerDB(header = NULL, top = FALSE)
  }
  else if(execution.type == "recursive"){ #run on a set of dbs
    list_dbs <- list.dirs(path = db.path, recursive = FALSE, full.names = TRUE)
    
    #TODO:
    # restricting the name init. to database_ can be eliminated 
    list_dbs <- grep(pattern = ".*(database_)", x = list_dbs, value = TRUE)
    selected.dbs.available <- list()
    dbs_count <- 0
    
    if(length(SELECTED.DBs)){
      for(db in list_dbs){
        if(basename(db) %in% SELECTED.DBs){
          dbs_count <- dbs_count + 1
          selected.dbs.available[basename(db)] <- normalizePath(db)
        }
      }
      
      db_name.available <- names(selected.dbs.available)
      db_name.notfound <- setdiff(SELECTED.DBs, db_name.available)
      
      if(length(db_name.notfound) == length(SELECTED.DBs)){
        msg <- paste('None of SELECTED.DBs are available in the folder:', db.path)
        stop(msg, call. = FALSE)
      }
      else if(length(db_name.notfound)){
        msg <- paste('\nNOTE: The following dbs in SELECTED.DBs are not found:','\n')
        cat(msg)
        msg <- paste0(db_name.notfound, '\n')
        cat(msg)
      }
      
      list_dbs <- unname(unlist(selected.dbs.available))
      dbs_count <- length(list_dbs)
    }
    else{
      dbs_count <- length(list_dbs)
    }
    
    counter <- 0
    for(db.path in list_dbs){
      db.path <- normalizePath(db.path)
      db.name <- basename(db.path)
      
      counter <- counter + 1
      #header
      print_headerDB(header = paste0(db.name, " - ", counter, "/", dbs_count), top = TRUE)
      #conn
      myconn <- get_db_conn(db.path = db.path)

      if(is.null(myconn)){
        break
      }

      scenario.name <- get_recent_scenario(myconn = myconn)
      #env
      print_env(db.path = db.path, MAIN.QUERY = MAIN.QUERY, scenario.name = scenario.name)
      #processing
      process_queries(myconn, db.path, MAIN.QUERY, queries_xml, scenario = scenario.name, QUERY.BY = QUERY.BY, output.path = output.path)
      #end
      print_headerDB(header = NULL, top = FALSE)
    }
  }
  print_headerExecution(top = FALSE)
}

get_table <- function(myconn, query, scenario){
  if(is.null(REGIONS)){REGIONS <- NULL}
  noquote(" ")
  success <- FALSE
  
  table <- tryCatch(
    {
      table <- runQuery(dbConn = myconn, query = query, scenario = scenario, regions = REGIONS)
      cat("+ Query proccessed Successfully!\n")
      success <- TRUE
      table
    }, 
    error = function(cond){
      cat(paste("- Error on query\n"))
      cat(paste(cond, "\n"))
      table <- NULL
    }, 
    warning = function(cond){
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
      cols <- names(table)
      table <- data.frame(table)
      if("year" %in% cols){
        table <- reshape2::dcast(table, ... ~ year, value.var = "value")
        cols <- names(table)
      }
      colx_reorder <- c(cols[-1], cols[1])
      table <- table[, colx_reorder]
      cat("+ Data proccessed Successfully!\n")
      table
    }, 
    error = function(cond){
      cat("- Error when processing Data\n")
      cat(paste(cond, "\n"))
      return(NULL)
    }, 
    warning = function(cond){
      cat("- Warning when processing Data\n")
      cat(paste(cond, "\n"))
      return(NULL)
    }
  )
  return(table)
}

#queries
process_queries <- function(myconn, db.path, MAIN.QUERY, queries_xml, scenario, QUERY.BY, output.path){
  #TODO:
  # loop through SELECTED.QUERIES instead - improve speed
  for(query.counter in names(queries_xml)){
    if(length(SELECTED.QUERIES) > 0 & !as.integer(query.counter) %in% SELECTED.QUERIES){
      next
    }
    #output.filename: <Q#>_<db_foldername>
    output.filename <- paste0("Q", query.counter, "_", basename(db.path), ".csv")

    query.title <- names(queries_xml[[query.counter]])
    #TODO:
    # NO need when QUERY.BY title
    query.xml <- queries_xml[[query.counter]][[query.title]]
    
    query.query <- buid_query(query.title = query.title, MAIN.QUERY = MAIN.QUERY)
    
    process_query(myconn, scenario = scenario,
                  output.filename = output.filename, output.path = output.path,
                  QUERY.BY = QUERY.BY, query.xml = query.xml, query.query = query.query, query.title = query.title,
                  query.counter = query.counter)
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
    result <- get_table(myconn, query, scenario = scenario)
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
    }, silent = TRUE)
  
  if(is.null(myconn)){
    cat(paste0("ERROR - CANNOT CONNTECT TO DB: ", basename(db.path), "\n"))
  }
  myconn
}

get_recent_scenario <- function(myconn){
  lastScenarioRunInfo <- tail(listScenariosInDB(myconn), n = 1)
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
      validation_config <- function(val){
        tryCatch({
          val
        },
        error = function(c){stop("config.R file is not valide", call. = FALSE)},
        warning = function(c){stop("config.R file doesn't exist", call. = FALSE)})
      }
      validation_path <- function(val, path_type){
        tryCatch({
          val <- normalizePath(val)
          if(path_type == 'file'){
            if(!file.exists(val) | dir.exists(val)){
              stop(paste("File:", val, "- doesn't exsist or not a file!"), call. = FALSE)
            }
            
          }else if(path_type == 'folder'){
            
            if(!file.exists(val) | !dir.exists(val)){
              stop(paste("File:", val, "- doesn't exsist or not a folder"), call. = FALSE)
            }
          }
          val
        },
        error = function(cond){
          stop(cond$message, call. = FALSE)
        },
        warning = function(cond){
          stop(cond$message, call. = FALSE)
        })
        return(val)
      }
      validation_variables_stop <- function(val, name){
        if(!length(val)){
          stop(paste("Please proide values in", name), call. = FALSE)
        }
      }
      validation_variables_warning <- function(val, name){
        if(!length(val)){
          warning(paste("Empty value:", name), call. = FALSE)
        }
      }
      validation_args <- function(args){
        # test if there is at least one argument: if not, return an error
        if(length(args) == 0){
          stop("Please provide the DB_PATH or DBs_FOLDER as following: Rscript <file_script.R> [-d <DB_PATH> | -f <DBs_FOLDER>]", call. = FALSE)
        } else if(length(args) == 2){
          # default output file
          if(args[1] == "-d"){
            execution.type <- "db"
            db.path <- args[2]
          }else if(args[1] == "-f"){
            execution.type <- "recursive"
            db.path <- args[2]
          }
        } else{
          stop("Please provide only one arguament to Rscript as following: Rscript <Rscript.R> [-d <DB_PATH> | -f <DBs_FOLDER>]", call. = FALSE)
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
      args <- commandArgs(trailingOnly = TRUE)

      #args validation
      result.input <- validation_args(args)

      execution.type <-  result.input[["execution.type"]]

      db.path <-  result.input[["db.path"]]

      db.path <- validation_path(db.path, path_type = 'folder')

      #extract script path
      file.arg.name <- "--file="
      script.path <- sub(file.arg.name, "", input.full[grep(file.arg.name, input.full)])
      script.path <- normalizePath(script.path)
      script.name <- basename(script.path)
      script.dir <- dirname(script.path)
      
      #set working directory to the script's directory
      setwd(script.dir)
      this.dir <- getwd()
      
      #Validate & load configuration file
      config.path <- paste0(this.dir, "/", "config.R")
      
      validation_config(source(config.path))
      source(config.path)
      
      MAIN.QUERY <- validation_path(MAIN.QUERY, path_type = "file")
      #validation_variables_stop(SELECTED.QUERIES, name = "config/SELECTED.QUERIES")
      validation_variables_warning(REGIONS, name = "config/REGIONS")
      validation_variables_stop(queries_xml, name = "config/queries_xml")
      
      return(
        list(
          "script.path" = script.path,
          "script.name" = script.name,
          "script.dir" = script.dir,
          "MAIN.QUERY" = MAIN.QUERY,
          "execution.type" = execution.type, 
          "db.path" = db.path
        )
      )
    }
    validated.values <- validation.check()

    #script info
    script.path <- validated.values[["script.path"]]
    script.name <- validated.values[["script.name"]]
    script.dir <- validated.values[["script.dir"]]
    MAIN.QUERY <- validated.values[["MAIN.QUERY"]]
    execution.type <- validated.values[["execution.type"]]
    
    #folder/db info
    db.path <- validated.values[["db.path"]]
    db.name <- basename(db.path)
    
    #timing
    Saudi.time <- as.POSIXlt(Sys.time(), tz = "Asia/Riyadh")
    curr_time <- paste("TIMESTAMP:", Saudi.time)
    
    #output dir
    time_formated <- format(Saudi.time, "%b_%d__%H_%M")
    output.name <- paste0("output_", db.name,"__", time_formated)
    output.path <- paste0(script.dir, "/", output.name)
    
    dir.create(path = output.path)
    
    #logging
    log.path <- paste0(output.path, "/", "logFile.txt")
    log.file <- file(log.path) # File name of output log
    sink(log.file, append = TRUE, type = "output", split = TRUE) # Writing console output to log file
    print(paste("LOG.PATH:", log.path))

    print(curr_time)
    print(paste("OUTPUT.DIR:", output.path))
    
    if(is.null(QUERY.BY)){
      QUERY.BY <- "title" 
      }
    
    main(db.path = db.path, execution.type = execution.type, queries_xml = queries_xml, output.path = output.path, MAIN.QUERY = MAIN.QUERY, QUERY.BY = QUERY.BY)
  }, 
  error = function(cond){
    err = paste("-",cond, "\n")
    cat(err)
  }, 
  warning = function(cond){
    warr = paste("-",cond, "\n")
    cat(warr)
  },
  finally = {
    closeAllConnections() # Close connection to log file
  }
)