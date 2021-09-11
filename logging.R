log.table <- data.frame(
  Query.number = double(),
  Query.name = character(),
  DB.name = character(),
  Scenario.name = character(),
  Succeed = logical(),
  Output.filename = character(),
  ExecTime = character()
)

log.initTable <- function(){
  return(
    data.frame(
      Query.number = double(),
      Query.name = character(),
      DB.name = character(),
      Scenario.name = character(),
      Succeed = logical(),
      Output.filename = character(),
      ExecTime = character()
    ))
}
log.addRecord <- function(
  Query.number,
  Query.name,
  DB.name,
  Scenario.name,
  Succeed,
  Output.filename,
  ExecEnd, 
  ExecStart){
  data <- data.frame(
    Query.number = Query.number, 
    Query.name = Query.name, 
    DB.name = DB.name,
    Scenario.name = Scenario.name,
    Succeed = Succeed,
    Output.filename = Output.filename,
    ExecTime = round(difftime(ExecEnd, ExecStart, units = "min"), 2)
  )
  
  log.table <- rbind(log.table, data)
  #print(log.table)
  #return(log.table)
  assign("log.table", log.table, envir = .GlobalEnv)
}

log.getTable <- function(){
  return(log.table)
}
