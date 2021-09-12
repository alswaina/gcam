#!/usr/bin/env Rscript
# Author  : Fahad Alswaina
# Github  : github.com/alswaina
# Email   : alswaina.fahad@gmail.com

#TODO:
# improve prints functions' names: i.e. print_headerQuery => print_exc_header

#LIBRARIES ----------------------------------------------------------------------------------
library(rgcam)

#FUNCTIONS -----------------------------------------------------------------------
main <- function(db.path, execution.type, queries_xml, output.path, MAIN.QUERY, QUERY.BY) {
    log.executionHeaderPrint(top = TRUE)
    
    if (execution.type == "db") {
      #run on a single db
      #header
      log.rootHeaderPrint(type="DB", header = basename(db.path), top = TRUE)
      #conn
      myconn <- get_db_conn(db.path = db.path)
      
      if (is.null(myconn)) {
        break
      }
      
      scenario.name <- get_recent_scenario(myconn = myconn)
      #env
      log.envPrint(
        db.path = db.path,
        scenario.name = scenario.name
      )
      #processing
      process_queries(
        myconn,
        db.path,
        MAIN.QUERY,
        queries_xml,
        scenario = scenario.name,
        QUERY.BY = QUERY.BY,
        output.path = output.path
      )
      #end
      log.rootHeaderPrint(type="DB", header = NULL, top = FALSE)
    }
    else if (execution.type == "recursive") {
      if (RUN.TYPE == 'seperate') {
        counter <- 0
        for (db.path in SELECTED.DBs) {
          db.name <- basename(db.path)
          counter <- counter + 1
          #header
          log.rootHeaderPrint(type="DB", header = paste0(db.name, " - ", counter, "/", length(SELECTED.DBs)), top = TRUE)
          #conn
          myconn <- get_db_conn(db.path = db.path)
          if (is.null(myconn)) {
            msg <- paste('CONNECTION TO DB:', db.path, 'COULD NOT BE STABLISHED! (skipped)')
            warning(msg)
            next
          }
          scenario.name <- get_recent_scenario(myconn = myconn)
          #env
          log.envPrint(db.path = db.path, scenario.name = scenario.name)
          #processing
          process_queries(myconn, db.path, MAIN.QUERY, queries=SELECTED.QUERIES, scenario = scenario.name, QUERY.BY = QUERY.BY, output.path = output.path)
          #end
          log.rootHeaderPrint(type="DB", header = NULL, top = FALSE)
          }
          }else if(RUN.TYPE == 'aggregate'){
          #set connections for all SELECTED.DBs
          for (query in names(SELECTED.QUERIES)) {
            query.name <- names(SELECTED.QUERIES[[query]])
            #header
            log.rootHeaderPrint(type="QUERY", header = paste0(query.name, " - ", query, "/", length(SELECTED.QUERIES)), top = TRUE)
            
            #build query
            query.query <- buid_query(query.title = query.name, MAIN.QUERY = MAIN.QUERY)
            #processing
            process_dbs(SELECTED.QUERIES[query], SELECTED.DBs, query.query, output.path)
            #end
            log.rootHeaderPrint(type="QUERY", header = NULL, top = FALSE)
          }        
      }
      log.executionHeaderPrint(top = FALSE)
      log.writeCSV(file.name="logTable.csv",  file.path=output.path)
    }
}

process_dbs <- function(query, SELECTED.DBs, query.query, output.path){
  selected.dbs.connections <- list() #list(<db_name>:<connection instance>, )
  query.number <- names(query)
  query.title <- names(query[[1]])
  query.xml <- unlist(unname(query[[1]]))
  
  for(db.path in SELECTED.DBs){
    conn <- get_db_conn(db.path = db.path)
    if (is.null(conn)) {
      msg <- paste('CONNECTION TO DB:', db.path, 'COULD NOT BE STABLISHED! (skipped)\n')
      warning(msg)
      next
    }
    selected.dbs.connections[[basename(db.path)]] <- conn
  }
  
  if(!length(selected.dbs.connections)){
    msg <- paste('NO CONNECTION WAS STABLISHED! (abort)\n')
    stop(msg)
  }
  output.table <- list()
  counter <- 0
  for(conn in names(selected.dbs.connections)){
    db.name <- conn
    db.conn <- selected.dbs.connections[[conn]]
    counter <- counter + 1
    scenario.name <- get_recent_scenario(myconn = db.conn)
    
    #processing
    analytics <- process_db(myconn=db.conn, scenario = scenario.name,
                  query = query.query, db.title = db.name, db.counter = counter)
    if(!length(output.table)){
      output.table <- analytics[["result"]]
    }else{
      output.table <- data.table::rbindlist(list(output.table, analytics[["result"]]))
    }
    cat("---\n")

    log.table <- log.addRecord(
      Query.number = names(query), 
      Query.name = query.title, 
      DB.name = db.name,
      Scenario.name = scenario.name,
      Succeed = ifelse(analytics[["Success"]], TRUE, FALSE),
      Output.filename = paste0('Q_', names(query), ".csv"),
      ExecEnd = analytics[["ExecEnd"]], 
      ExecStart = analytics[["ExecStart"]])
  }
  output.filename <- paste0('Q_', names(query), ".csv")
  write_output(result = output.table, output.filename = output.filename, output.path = output.path)
}

process_db <- function(myconn, scenario, query, db.title, db.counter){
  log.nodeHeaderPrint(type="DB", counter=db.counter, title=db.title, scenario.name = scenario)

  if(!is.null(query)){
    ExecStart <- Sys.time()
    result <- get_table(myconn, query, scenario = scenario)
    ExecEnd <- Sys.time()
    Success <- length(result)>0
    #return(result)
  }else{
    Success <- FALSE
    print("ERROR IN DETECTING THE QUERY!", quote=FALSE)
  }
  
  return(list(
    "ExecStart" = ExecStart, 
    "ExecEnd" = ExecEnd, 
    "Success" = Success, 
    "result" = result
  ))
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
process_queries <- function(myconn, db.path, MAIN.QUERY, queries, scenario, QUERY.BY, output.path){
  #TODO:
  # loop through SELECTED.QUERIES instead - improve speed
  for(query.counter in names(queries)){
    #output.filename: <Q#>_<db_foldername>
    output.filename <- paste0("Q", query.counter, "_", basename(db.path), ".csv")
    
    query.title <- names(queries[[query.counter]])
    #TODO:
    # NO need when QUERY.BY title
    query.xml <- queries[[query.counter]][[query.title]]
    
    query.query <- buid_query(query.title = query.title, MAIN.QUERY = MAIN.QUERY)
    
    analytics <- process_query(myconn, scenario = scenario,
                  output.filename = output.filename, output.path = output.path,
                  QUERY.BY = QUERY.BY, query.xml = query.xml, query= query.query, query.title = query.title,
                  query.counter = query.counter)
    log.table <- log.addRecord(
      Query.number = query.counter, 
      Query.name = query.title, 
      DB.name = basename(db.path),
      Scenario.name = scenario,
      Succeed = ifelse(analytics[["Success"]], TRUE, FALSE),
      Output.filename = output.filename,
      ExecEnd = analytics[["ExecEnd"]], 
      ExecStart = analytics[["ExecStart"]])
    cat("---\n")
  }
}

process_query <- function(myconn, scenario, output.filename, output.path, QUERY.BY, query.xml, query, query.title, query.counter){
  log.nodeHeaderPrint(type="QUERY", counter=query.counter, title=query.title)
  
  if(!is.null(query)){
    ExecStart <- Sys.time()
    result <- get_table(myconn, query, scenario = scenario)
    ExecEnd <- Sys.time()
    Success <- length(result)>0
    write_output(result = result, output.filename = output.filename, output.path = output.path)
  }else{
    print("ERROR IN DETECTING THE QUERY!", quote=FALSE)
    Success <- FALSE
  }
  return(list(
    "ExecStart" = ExecStart, 
    "ExecEnd" = ExecEnd, 
    "Success" = Success
  ))
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
    print(paste0("OUTPUT: ", output.filepath), quote=FALSE)
  }else{
    print("OUTPUT: NOT GENERATED", quote=FALSE)
  }
  noquote(" ")
}

#connections
get_db_conn <- function(db.path){
  myconn <- NULL
  try(
    {
      log <- capture.output({
        myconn <- localDBConn(dbPath = dirname(db.path), dbFile = basename(db.path))
      },type = c('message'))
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


#EXECUTION ------------------------------------------------------------------------
tryCatch(
  {

    #validate configuration file
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
      validation_args_config <- function(args){
        # test if there is at least one argument: if not, return an error
        
        if(!length(args) %in% c(2,4)){
          #added
          stop("Please use the syntax: Rscript <file_script.R> [-c <CONFIG_PATH>] (-d <DB_PATH> | -f <DBs_FOLDER>)", call. = FALSE)
        } else if(length(args) == 2 && args[1] %in% c('-d', '-f')){
          # default config file at the current location
          config.input = "./config.R"
        }else if(length(args) == 4 & args[1] == "-c"){
          # named config file
          config.input = args[2]
        }
        else{
          stop("Please use the syntax: Rscript <file_script.R> [-c <CONFIG_PATH>] (-d <DB_PATH> | -f <DBs_FOLDER>)", call. = FALSE)
        }
        
        return(
          list(
            "config.input" = config.input
          )
        )
      }
      validation_args_db <- function(args){
        # test if there is at least one argument: if not, return an error
        if(!length(args) %in% c(2,4)){
          #added
          stop("Please provide the DB_PATH or DBs_FOLDER as following: Rscript <file_script.R> [-c <CONFIG_PATH>] [-d <DB_PATH> | -f <DBs_FOLDER>]", call. = FALSE)
        } else if(length(args) == 2){
          if(args[1] == "-d"){
            # default output file
            execution.type <- "db"
            db.path <- args[2]
          }else if(args[1] == "-f"){
            execution.type <- "recursive"
            db.path <- args[2]
          }else{
            stop("Expected (-d <DB_PATH> | -f <DBs_FOLDER>) following: Rscript <Rscript.R> [-c <CONFIG_PATH>] [-d <DB_PATH> | -f <DBs_FOLDER>]", call. = FALSE)
          }
        }else if(length(args) == 4){
          if(args[3] == "-d"){
            # default output file
            execution.type <- "db"
            db.path <- args[4]
          }else if(args[3] == "-f"){
            execution.type <- "recursive"
            db.path <- args[4]
          }else{
            stop("Expected (-d <DB_PATH> | -f <DBs_FOLDER>) following: Rscript <Rscript.R> [-c <CONFIG_PATH>] [-d <DB_PATH> | -f <DBs_FOLDER>]", call. = FALSE)
          }
        }
        return(
          list(
            "execution.type" = execution.type, 
            "db.path" = db.path
          )
        )
      }
      
      #under testing
      validation_selected_dbs <- function(SELECTED.DBs, db.path) {
        #return a list of available paths for selected dbs
        list_dbs <-
          list.dirs(path = db.path,
                    recursive = FALSE,
                    full.names = TRUE)
        #TODO:
        # restricting the name init. to database_ can be eliminated
        list_dbs <-
          grep(pattern = ".*(database_)",
               x = list_dbs,
               value = TRUE)
        
        if (!length(list_dbs)) {
          msg <-
            paste('No dbs are available in the folder:',
                  db.path)
          stop(msg, call. = FALSE)
        }
        
        selected.dbs.available <- list()
        if (length(SELECTED.DBs)) {
          for (db in list_dbs) {
            if (basename(db) %in% SELECTED.DBs) {
              selected.dbs.available[basename(db)] <- normalizePath(db)
            }
          }
          notfound <- setdiff(SELECTED.DBs, names(selected.dbs.available))
          
          if (length(notfound) == length(SELECTED.DBs)) {
            msg <-
              paste('None of SELECTED.DBs are available in the folder:',
                    db.path)
            stop(msg, call. = FALSE)
          }
          else if (length(notfound)) {
            msg <-
              paste('\nNOTE: The following dbs in SELECTED.DBs are not found:',
                    '\n')
            cat(msg)
            msg <- paste0(notfound, '\n')
            cat(msg)
          }
          list_dbs <- unname(unlist(selected.dbs.available))
        }
        return(list_dbs)
      }
      validation_selected_queries <- function(SELECTED.QUERIES, queries_xml) {
        if (!length(queries_xml)) {
          msg <-
            paste('Queries xml list is empty!', '\n')
          stop(msg, call. = FALSE)
        }
        queries <- list()
        
        # if(class(SELECTED.QUERIES) != 'numeric' & class(SELECTED.QUERIES) != 'NULL' ){
        #   msg <-
        #     paste('SELECTED.QUERIES is not of numeric type!', '\n')
        #   stop(msg, call. = FALSE)
        # }
        
        if (length(SELECTED.QUERIES)) {
          notfound <-
            setdiff(SELECTED.QUERIES, as.integer(names(queries_xml)))
          found <-
            intersect(SELECTED.QUERIES, as.integer(names(queries_xml)))
          
          if (!length(found)) {
            msg <-
              paste('None of SELECTED.QUERIES are available in the queries xml list:','\n')
            stop(msg, call. = FALSE)
          }
          else if (length(notfound)) {
            msg <-
              paste('\nNOTE: The following queries numbers in SELECTED.QUERIES are not found:', '\n')
            cat(msg)
            msg <- paste0(notfound, '\n')
            cat(msg)
          }
          for (q in as.character(found)) {
            queries[q] <- queries_xml[q]
          }
        }
        else{
          queries <- queries_xml
        }
        return(queries)
      }
      
      #terminal input
      input.full <- commandArgs(trailingOnly = FALSE)
      args <- commandArgs(trailingOnly = TRUE)
      
      #args validation for db
      db.input <- validation_args_db(args)
      
      #args validation for config file
      config.input <- validation_args_config(args)
      
      execution.type <-  db.input[["execution.type"]]
      db.path <-  db.input[["db.path"]]
      config.input = config.input[["config.input"]]
      
      db.path <- validation_path(db.path, path_type = 'folder')
      config.path <- validation_path(config.input, path_type = 'file')

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
      #config.path <- paste0(this.dir, "/", "config.R")
      
      validation_config(source(config.path))
      source(config.path)
      
      logging.path <- paste0(this.dir, "/", "logging.R")
      source(logging.path)
      
      
      MAIN.QUERY <- validation_path(MAIN.QUERY, path_type = "file")
      #validation_variables_stop(SELECTED.QUERIES, name = "config/SELECTED.QUERIES")
      #validation_variables_warning(REGIONS, name = "config/REGIONS")
      validation_variables_stop(queries_xml, name = "config/queries_xml")
      
      #under testing
      SELECTED.DBs <-
        validation_selected_dbs(SELECTED.DBs, db.path)
      SELECTED.QUERIES <-
        validation_selected_queries(SELECTED.QUERIES, queries_xml)
      
      if (is.null(QUERY.BY) | !length(QUERY.BY)) {
        QUERY.BY <- "title"
      }
      if (is.null(QUERY.BY) | !length(REGIONS)) {
        REGIONS <- c('Global')
      }
      if (is.null(QUERY.BY) | !length(RUN.TYPE)) {
        RUN.TYPE <- "seperate"
      }
      return(
        list(
          "script.path" = script.path,
          "script.name" = script.name,
          "script.dir" = script.dir,
          "MAIN.QUERY" = MAIN.QUERY,
          "execution.type" = execution.type,
          "db.path" = db.path,
          "SELECTED.DBs" = SELECTED.DBs,
          "SELECTED.QUERIES" = SELECTED.QUERIES, 
          "RUN.TYPE" = RUN.TYPE, 
          "REGIONS" = REGIONS, 
          "QUERY.BY" = QUERY.BY, 
          "config.path" = config.path
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
    SELECTED.DBs <- validated.values[["SELECTED.DBs"]]
    SELECTED.QUERIES <- validated.values[["SELECTED.QUERIES"]]
    config.path <- validated.values[["config.path"]]
    
    RUN.TYPE <- validated.values[["RUN.TYPE"]]
    REGIONS <- validated.values[["REGIONS"]]
    QUERY.BY <- validated.values[["QUERY.BY"]]
    

    #folder/db info
    db.path <- validated.values[["db.path"]]
    db.name <- basename(db.path)
    
    #timing
    Saudi.time <- as.POSIXlt(Sys.time(), tz = "Asia/Riyadh")
    
    #output dir
    time_formated <- format(Saudi.time, "%b_%d__%H_%M")
    output.name <- paste0("output_", db.name,"__", time_formated)
    output.path <- paste0(script.dir, "/", output.name)
    
    dir.create(path = output.path, showWarnings = FALSE)
    
    #logging
    log.path <- paste0(output.path, "/", "logFile.txt")
    log.file <- file(log.path) # File name of output log
    sink(log.file, append = TRUE, type = "output", split = TRUE) # Writing console output to log file
 
    print(paste("TIMESTAMP:", Saudi.time), quote=FALSE)
    print(paste("MAIN_QUERY:", MAIN.QUERY), quote=FALSE)
    print(paste("OUTPUT.DIR:", output.path), quote=FALSE)
    print(paste("LOG.PATH:", log.path), quote=FALSE)
    print(paste("CONFIG.PATH:", config.path), quote=FALSE)
    
    selected <- NULL
    if (length(SELECTED.QUERIES)){
      selected <- as.integer(names(SELECTED.QUERIES))
    }else{
      selected <- 'ALL'
    }
    
  print("SELECTED.QUERIES:", quote = FALSE)
  print(selected, quote = FALSE)
  
  if (length(SELECTED.DBs)){
    selected <- NULL
    for(db in SELECTED.DBs){
        selected <- c(selected, basename(db))
    }
  }
  else{
    selected <- 'ALL'
  }
  print("SELECTED.DBs:", quote = FALSE)
  print(selected, quote = FALSE)
  
  selected <- REGIONS
  print("REGIONS:", quote = FALSE)
  print(selected, quote = FALSE)
  
  selected <- RUN.TYPE
  print("RUN.TYPE:", quote = FALSE)
  print(selected, quote = FALSE)
    
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