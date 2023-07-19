update_Slow_Moving_Products <- function()
{
  res <- data.frame(
    Job_Name = character(0),
    Logged_Time =as.Date(character(0)),
    Job_Status = character(0),
    Message = character(0)
  )
  #####Database Connection###################
  rds_mishondb_con <- tryCatch(
    {
      rds_mishondb_con <- DBI::dbConnect(
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
  
  rds_odoodb <- tryCatch(
    {
      rds_odoodb <- DBI::dbConnect(
        drv      = RPostgres::Postgres(),
        dbname   = Sys.getenv("rds_odoodb_dbname"),
        host     = Sys.getenv("rds_odoodb_host"),
        port     = as.numeric(Sys.getenv("rds_odoodb_port")),
        user     = Sys.getenv("rds_odoodb_user"),
        password = Sys.getenv("rds_odoodb_password"))
      
    },
    error=function(e)
    {
      return(NULL)
    })
  if(!is.null(rds_mishondb_con) & !is.null(rds_odoodb))
  {
    prdct_template <- tryCatch(
      {
        prdct_template <- dbGetQuery(rds_odoodb,"select template.default_code as SKU, supplierinfo.min_qty, supplierinfo.sequence  
                             from product_template as template inner join product_supplierinfo as supplierinfo on 
                             supplierinfo.product_tmpl_id=template.id
                             where (template.default_code like 'C-%' or template.default_code like 'K-%') and 
                             template.active = TRUE and template.product_status='active' and template.available_in_new_dundee = TRUE or 
                             template.x_available_in_chesterfield = TRUE")
        
      },
      error=function(e)
      {
        return(NULL)
      })
    
    productInventory <- tryCatch(
      {
        productInventory <- dbGetQuery(rds_mishondb_con, glue("select Product,WH_Quantity as CHES_on_hand,
                                                              RTL_Quantity as ND_on_hand,Total_Quantity as Total_on_hand,
                                                              Bundle_Components,Component_Type,Amazon_Launch_Date,
                                                              Ebay_Launch_Date from todays_inventory where date(Date) = '{Sys.Date()-1}'"))
        
        
      },
      error=function(e)
      {
        return(NULL)
      })
    
    salesData <- tryCatch(
      {
        salesData <- dbGetQuery(rds_mishondb_con,glue("SELECT SKU, SUM(CASE WHEN DATE(DATE) BETWEEN '{Sys.Date()-365}' AND '{Sys.Date()}' THEN QtySold END) AS QtySold_12Months,
        SUM(CASE WHEN DATE(DATE) BETWEEN '{Sys.Date()-730}' AND '{Sys.Date()}' THEN QtySold END) AS QtySold_24Months,
        SUM(CASE WHEN DATE(DATE) < '{Sys.Date()}' THEN QtySold END) AS QtySold_Total 
FROM dailysales WHERE GMV > 0 GROUP BY SKU"))
        
      },
      error=function(e)
      {
        return(NULL)
      })
    minSalesData <- tryCatch(
      {
        minSalesData = dbGetQuery(rds_mishondb_con,"SELECT SKU, MIN(DATE(DATE)) AS Min_Sales_Date FROM dailysales GROUP BY SKU")
        
        
      },
      error=function(e)
      {
        return(NULL)
      })
    forecastData <- tryCatch(
      {
        forecastData <- dbGetQuery(rds_mishondb_con,"select * from demand_FC_Pur_SKU_odc")
        
        
      },
      error=function(e)
      {
        return(NULL)
      })
    
    dbDisconnect(conn=rds_mishondb_con)
    
    dbDisconnect(conn=rds_odoodb)
    
    #  
    
    if(!is.null(prdct_template)& !is.null(productInventory) & !is.null(salesData) &
       !is.null(minSalesData) & !is.null(forecastData))
    {
      
      salesData =  salesData[!grepl("KOOZIE|8TEN",toupper(salesData$SKU)),]
      productInventory =  productInventory[!grepl("KOOZIE|8TEN",toupper(productInventory$Product)),]
      
      prdct_template1 <- prdct_template %>% group_by(sku)%>%slice(which.min(sequence))
      
      validSalesData <- left_join(productInventory%>%filter(Bundle_Components!=""),salesData,by=c("Product"="SKU"))
      validSalesData[is.na(validSalesData)] <- 0
      # #
      
      #
      compDF3 <- data.frame()
      for(i in 1:dim(validSalesData)[1])
      {
         # print(i)
        req_product <- validSalesData[i,]
        reqBundleComponents <- productInventory$Bundle_Components[productInventory$Product %in% req_product$Product]
        budleComponents <- unlist(strsplit(reqBundleComponents,","))
        
        component <- budleComponents%>%str_extract_all(pattern = "15MIS\\_\\w+\\-\\w{3}\\-\\d+")%>%unlist()
        req_quant <- budleComponents%>%str_extract_all(pattern ="\\=\\d+")%>%unlist()%>%str_replace_all("=","")%>%unlist()%>%as.numeric()
        compDF0 <- data.table(Product=component,req_quant=req_quant)
        
        compDF1 = compDF0%>%mutate(SKU= str_extract_all(Product,pattern="\\w{1}\\-\\w{3}\\-\\d{4}")%>%unlist())
        
        compDF1$Qty_Sold_12Months <- compDF1$req_quant * req_product$QtySold_12Months
        compDF1$Qty_Sold_24Months <- compDF1$req_quant * req_product$QtySold_24Months
        compDF1$QtySold_Total <- compDF1$req_quant * req_product$QtySold_Total
        
        compDF3<- rbind(compDF1 %>% select(-c("Product","req_quant")),compDF3)
        
       
        
      }
      
      # #
      summ_SalesDF <- compDF3 %>% group_by(SKU) %>%summarise(Qty_Sold_12Months = sum(Qty_Sold_12Months),
                                                             Qty_Sold_24Months = sum(Qty_Sold_24Months),
                                                             QtySold_Total = sum(QtySold_Total))
      
      
      forecastData_Summ <- forecastData %>% filter(product_status !="discontinued") %>% group_by(SKU) %>% summarise(FC_Qty = sum(FC_QTY))
      # #
      productData1 <- productInventory[productInventory$Component_Type=="MIS" & !grepl("KOOZIE|8TEN",productInventory$Product),]
      # productData1 <- productInventory[productInventory$Component_Type=="MIS" & productInventory$Product!="15MIS_KOOZIE1",]
      productData1$refSKU = str_extract_all(productData1$Product,pattern="\\w{1}\\-\\w{3}\\-\\d{4}")%>%unlist()
      
      productData1$Amazon_Launch_Date <- as.Date(productData1$Amazon_Launch_Date,"%m/%d/%Y")
      productData1$Ebay_Launch_Date <- as.Date(productData1$Ebay_Launch_Date,"%m/%d/%Y")
      
      productData1$Min_Launch_Date <- pmin(productData1$Amazon_Launch_Date,productData1$Ebay_Launch_Date,na.rm=TRUE)
      
      final_DF0 <- left_join(productData1,minSalesData,by=c("Product"="SKU"))
      
      final_DF0$Launch_Date <- pmin(final_DF0$Min_Launch_Date,final_DF0$Min_Sales_Date,na.rm=TRUE)
      
      join1 <- left_join(prdct_template1%>%select(sku,min_qty),summ_SalesDF,by=c("sku"="SKU"))
      join2 <- left_join(join1,forecastData_Summ,by=c("sku"="SKU"))
      join3 <- left_join(join2,final_DF0%>%select(CHES_on_hand,ND_on_hand,Total_on_hand,Launch_Date,refSKU),by=c("sku"="refSKU"))
      
      
      join3$Last_Updated_Date <- Sys.Date()
      
      rds_mishondb_con <- tryCatch(
        {
          rds_mishondb_con <- DBI::dbConnect(
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
      
      
      if(!is.null(rds_mishondb_con) & dim(join3)[1]>0)
      {
        DBI::dbGetQuery(rds_mishondb_con, "TRUNCATE TABLE slow_moving_products")
        DBI::dbWriteTable(rds_mishondb_con,"slow_moving_products",join3,row.names=FALSE,append=TRUE)
        
        dbDisconnect(conn=rds_mishondb_con)
        
        
        res <- res%>%add_row(
          Job_Name = "Slow Moving Products",
          Logged_Time = Sys.Date(),
          Job_Status = "SUCCESS",
          Message = "Successfully saved data"
        )
        print(glue("successfully saved data in Slow Moving Products on {Sys.time()}"))
      } else{
        res <- res%>%add_row(
          Job_Name ="Slow Moving Products",
          Logged_Time = Sys.Date(),
          Job_Status = "ERROR",
          Message = "No Data to save"
        )
        print(glue("No Data to save in Slow Moving Products on {Sys.time()}"))
      }
      

      
    } else{
      res <- res%>%add_row(
        Job_Name ="Slow Moving Products",
        Logged_Time = Sys.Date(),
        Job_Status = "ERROR",
        Message = "Error while reading data in pushpull"
      )
      print(glue("Error while reading data in slow Moving Products on {Sys.time()}"))
    }
    
    
    
    
    
  } else{
    res <- res%>%add_row(
      Job_Name ="Slow Moving Products",
      Logged_Time = Sys.Date(),
      Job_Status = "ERROR",
      Message = "Database connection failed"
    )
    print(glue("Database connection failed on {Sys.time()}"))
  }
  
  return(res)
  
  
}
