setwd("/root/cron")
 # setwd("C:/Mishon/CRON/3.1___CRON_DEPENDS_ON_INV/")


source("./Seq1__companywideInventory.R"             ,local=TRUE)$value
source("./Seq13_Slow_Moving_Products.R"   ,local=TRUE)$value
source("./Seq14_Inventory_OOS.R"   ,local=TRUE)$value



library(RCurl)
library(DBI)
library(dplyr)
library(intrval)
library(stringr)
library(data.table)
library(RMySQL)
library(glue)
library(gmailr)
library(tableHTML)
library(tidyr)
library(stringi)

readRenviron("./.Renviron")
Sys.setenv(tz="EST")

InitialDF = data.frame(
  Job_Name = character(0),
  Logged_Time =as.Date(character(0)),
  Job_Status = character(0),
  Message = character(0)
)


###**** Sequence 1 ******
###Updates the Company-wide Inventory data into productInventory Table
print(glue("Updating Company-wide Inventory data on {Sys.time()}"))

job1_Res <- tryCatch(
  {
    job1_Res <- updateCompanyWideInventory()
  },
  error=function(e)
  {
    return(InitialDF)
  })




###**** Sequence 13 ******
###Generates Slow Moving Products
print(glue("Generating Slow Moving Products on {Sys.time()}"))


job13_Res <- tryCatch(
  {
    job13_Res <- update_Slow_Moving_Products()
  },
  error=function(e)
  {
    return(InitialDF)
  })
gc()
###**** Sequence 14 ******
###Generates OOS Inventory Report
print(glue("Generating OOS Inventory Report on {Sys.time()}"))

job14_Res <- tryCatch(
  {
    job14_Res <- update_inventory_oos()
  },
  error=function(e)
  {
    return(InitialDF)
  })
gc()



finalDF <- rbind(job1_Res,job13_Res,job14_Res)

rds_mishondb <- tryCatch(
  {
    rds_mishondb <- DBI::dbConnect(
      drv      = RMySQL::MySQL(),
      dbname   = Sys.getenv("rds_mishondb_dbname"),
      host     = Sys.getenv("rds_mishondb_host"),
      port     = as.numeric(Sys.getenv("rds_mishondb_port")),
      user     = Sys.getenv("rds_mishondb_user"),
      password = Sys.getenv("rds_mishondb_password"))
  },
  error=function(e)
  {
    return(NULL)
  })

if(!is.null(rds_mishondb))
{
  dbWriteTable(rds_mishondb,"cron_status_log",finalDF,row.names=FALSE,append=TRUE)
  
  
  dbDisconnect(conn=rds_mishondb)
  
  print(glue("Successfully updated data on {Sys.time()}"))
}
