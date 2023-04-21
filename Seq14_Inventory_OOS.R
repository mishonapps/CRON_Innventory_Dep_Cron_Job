update_inventory_oos <- function()
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
        
        ###Update the condition
        prdct_template <- dbGetQuery(rds_odoodb,"select template.default_code as SKU, supplierinfo.min_qty, supplierinfo.sequence, 
                            supplierinfo.delay,template.x_available_in_chesterfield as cf_avail,template.available_in_new_dundee as nd_avail,
                            available_in_ga as ga_avail 
                             from product_template as template inner join product_supplierinfo as supplierinfo on 
                             supplierinfo.product_tmpl_id=template.id
                             where (template.default_code like 'C-%' or template.default_code like 'K-%') and 
                             template.active = TRUE and template.product_status='active'")
        
      },
      error=function(e)
      {
        return(NULL)
      })
    
    stock_Move <- tryCatch(
      {
        stock_Move <- dbGetQuery(rds_odoodb,glue("select product.default_code as SKU,dest_location.complete_name as destination,date(stock_move.date_expected),stock_move.product_qty,stock_move.booking_reference, 
        date(stock_move.confirmed_shipping_date) as conf_del_date,
        date(forecasted_delivery_date) as forecasted_delivery_date,
        date(order_line.date_shipping) as org_del_date from stock_move
                                    inner join product_product as product on product.id=stock_move.product_id
                                    inner join stock_location as dest_location on dest_location.id=stock_move.location_dest_id 
                                    inner join purchase_order_line as order_line on order_line.id = stock_move.purchase_line_id 
                                    where stock_move.state='assigned' and
                                                 date(stock_move.date_expected)>'{Sys.Date()}' and (stock_move.booking_reference is null or stock_move.booking_reference !='Cancel')"))
        
        # (stock_move.booking_reference is null or
        
        },
      error=function(e)
      {
        return(NULL)
      })
    
    prdct_Packaging <- tryCatch(
      {
        prdct_Packaging <- dbGetQuery(rds_odoodb,"select packaging.id,product_template.default_code,
packaging.sequence, packaging.height, packaging.width, packaging.length,packaging.weight,packaging.max_weight,
packaging.is_individual, 
packaging.qty  from product_packaging as packaging inner join product_template on 
                           packaging.product_tmpl_id=product_template.id")
        
      },
      error=function(e)
      {
        return(NULL)
      })
    
    
    
    productInventory <- tryCatch(
      {
        productInventory <- dbGetQuery(rds_mishondb_con, glue("select Product,WH_Quantity as CHES_on_hand,RTL_Quantity as
                                                              ND_on_hand,GA_Quantity as GA_on_hand,Total_Quantity as Total_on_hand,Bundle_Components,
                                                              Component_Type,Amazon_Launch_Date,Ebay_Launch_Date from
                                                              todays_inventory where date(Date) = '{Sys.Date()-1}'"))
      },
      error=function(e)
      {
        return(NULL)
      })
    
    salesData <- tryCatch(
      {
        thisYear <- as.numeric(format(Sys.Date(),"%Y"))
        lastYear <-  thisYear-1
        salesData <- dbGetQuery(rds_mishondb_con,glue("SELECT SKU, SUM(CASE WHEN DATE(DATE) BETWEEN '{lastYear}-01-01' AND '{lastYear}-12-31' THEN QtySold END) AS QtySold_lastYear,
        SUM(CASE WHEN DATE(DATE) BETWEEN '{thisYear}-01-01' AND '{Sys.Date()}' THEN QtySold END) AS QtySold_YTD,
        SUM(CASE WHEN DATE(DATE) BETWEEN '2020-01-01' AND '{Sys.Date()}' THEN QtySold END) AS QtySold_LifeTime 
FROM dailysales WHERE GMV > 0 GROUP BY SKU"))
        
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
    
    forecast_StatusData <- tryCatch(
      {
        forecast_StatusData <- dbGetQuery(rds_mishondb_con,"select * from demand_Forecast_status_odc")
      },
      error=function(e)
      {
        return(NULL)
      })
    
    firstReceived <- tryCatch(
      {
        firstReceived <- dbGetQuery(rds_mishondb_con,"select sku,DATE as first_received_date from firstReceived")
      },
      error=function(e)
      {
        return(NULL)
      })
    
    
    dbDisconnect(conn=rds_mishondb_con)
    
    dbDisconnect(conn=rds_odoodb)
    
    #  
    
    if(!is.null(prdct_template)& !is.null(productInventory) & !is.null(salesData) &
       !is.null(stock_Move) & !is.null(forecastData))
    {
      
      salesData =  salesData[!grepl("KOOZIE|8TEN",toupper(salesData$SKU)),]
      productInventory =  productInventory[!grepl("KOOZIE|8TEN",toupper(productInventory$Product)),]
      
      stock_Move2 <- stock_Move%>%filter(!grepl("cancel",tolower(booking_reference))) %>%group_by(sku,date)%>%summarise(order_qty = sum(product_qty),booking_reference=booking_reference[1])%>%slice(which.min(as.Date(date)))
      
      prdct_template1 <- prdct_template %>% group_by(sku)%>%slice(which.min(sequence))
      
      validSalesData <- left_join(productInventory%>%filter(Bundle_Components!=""),salesData,by=c("Product"="SKU"))
      validSalesData[is.na(validSalesData)] <- 0
      
      
      #Qty Sold Calculation
      compDF3 <- data.frame()
      for(i in 1:dim(validSalesData)[1])
      {
        req_product <- validSalesData[i,]
        reqBundleComponents <- productInventory$Bundle_Components[productInventory$Product %in% req_product$Product]
        budleComponents <- unlist(strsplit(reqBundleComponents,","))
        
        component <- budleComponents%>%str_extract_all(pattern = "15MIS\\_\\w+\\-\\w{3}\\-\\d+")%>%unlist()
        req_quant <- budleComponents%>%str_extract_all(pattern ="\\=\\d+")%>%unlist()%>%str_replace_all("=","")%>%unlist()%>%as.numeric()
        compDF0 <- data.table(Product=component,req_quant=req_quant)
        
        compDF1 = compDF0%>%mutate(SKU= str_extract_all(Product,pattern="\\w{1}\\-\\w{3}\\-\\d{4}")%>%unlist())
        
        compDF1$QtySold_lastYear <- compDF1$req_quant * req_product$QtySold_lastYear
        compDF1$QtySold_YTD <- compDF1$req_quant * req_product$QtySold_YTD
        compDF1$QtySold_LifeTime <- compDF1$req_quant * req_product$QtySold_LifeTime
        
        compDF3<- rbind(compDF1 %>% select(-c("Product","req_quant")),compDF3)
        
        # print(i)
        
      }
     
      summ_SalesDF <- compDF3 %>% group_by(SKU) %>%summarise(QtySold_YTD = sum(QtySold_YTD),
                                                             QtySold_lastYear  = sum(QtySold_lastYear),
                                                             QtySold_LifeTime = sum(QtySold_LifeTime))
      
      
      forecastData_Summ <- forecastData %>% filter(product_status !="discontinued") %>% group_by(SKU) %>% summarise(Forecasted_Annual_Qty = sum(FC_QTY))
      
      #
      productData1 <- productInventory[productInventory$Component_Type=="MIS" & !grepl("KOOZIE|8TEN",productInventory$Product),]
      productData1$refSKU = str_extract_all(productData1$Product,pattern="\\w{1}\\-\\w{3}\\-\\d{4}")%>%unlist()
      
      # #
      #
      join1 <- left_join(prdct_template1%>%select(sku,min_qty,delivery_lead_time=delay,cf_avail,nd_avail,ga_avail),summ_SalesDF,by=c("sku"="SKU"))
      join2 <- left_join(join1,forecastData_Summ,by=c("sku"="SKU"))
      join3 <- left_join(join2,productData1%>%select(CHES_on_hand,ND_on_hand,GA_on_hand,refSKU),by=c("sku"="refSKU"))
      join3_1 <- join3[!is.na(join3$sku),]
      join3_1[is.na(join3_1)] <- 0
      join3_1$Total_on_hand <- join3_1$CHES_on_hand + join3_1$ND_on_hand + join3_1$GA_on_hand
      
      join4_1 <- left_join(join3_1,stock_Move2%>%select(sku,Next_receipt_Date=date,booking_reference,On_Order_Qty=order_qty),by="sku")
      
      req_product <- join4_1 %>% filter(Forecasted_Annual_Qty > Total_on_hand & Total_on_hand > 0 )
      
      
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
      
      reqSKUS <- paste0("'",req_product$sku,"'",collapse=",")
      
      seasonality <- tryCatch(
        {
          seasonality <- dbGetQuery(rds_mishondb_con, glue("select SKU,Date,sIndex from seasonality where SKU in ({reqSKUS})"))
        },
        error=function(e)
        {
          return(NULL)
        })
      
      dbDisconnect(rds_mishondb_con)
      
      forecastData_m = data.frame(
        SKU = character(0),
        Days_Untill_OOS = numeric(0),
        qty_req_after_oos_before_recv = numeric(0)
        
      )
     
      for(s in 1:dim(req_product)[1])
      {
        # print(s)
        prdctInfo =  req_product[s,]
        
        dateSeq_forecast <- seq(Sys.Date(),Sys.Date()+364,1)
        
        dateSeq_forecast <- str_replace_all(dateSeq_forecast,"02-29","03-01")
        
        dateFormatted = data.frame(Date = sapply(str_split(dateSeq_forecast,"-"),function(x) {return(paste(x[2],x[3],sep="-"))}),
                                   id=seq(1,365))
        
        sec_seasonality <- seasonality[seasonality$SKU==prdctInfo$sku &
                                         seasonality$Date %in% dateFormatted$Date,]
        
        sec_seasonality_2 = left_join(sec_seasonality,dateFormatted,by="Date")
        
        # #
        
        for(s2 in 1:365)
        {
         
          seasonality_sum = sum(sec_seasonality_2$sIndex[sec_seasonality_2$id<=s2])
          
          forecastInfo = (seasonality_sum * prdctInfo$Forecasted_Annual_Qty)/365
          
          if(round(prdctInfo$Total_on_hand-forecastInfo)  ==0)
          {
            # #
            remaining_Days = ifelse( is.na(as.Date(prdctInfo$Next_receipt_Date)),0,
                                     ifelse(as.Date(prdctInfo$Next_receipt_Date)>Sys.Date(),
                                            as.numeric(as.Date(prdctInfo$Next_receipt_Date)-(Sys.Date()+s2)),0))
            
            forecastInfo2 = 0
            
            if(remaining_Days>0)
            {
              seasonality_sum2 = sum(sec_seasonality_2$sIndex[sec_seasonality_2$id>s2 & sec_seasonality_2$id <= s2+remaining_Days +30])
              
              forecastInfo2 = (seasonality_sum2 * prdctInfo$Forecasted_Annual_Qty)/365
            } 
            
            
            forecastData_m = forecastData_m %>% add_row(
              SKU = prdctInfo$sku,
              Days_Untill_OOS = s2,
              qty_req_after_oos_before_recv = forecastInfo2
              
            )
            
            
            
            
            break
          } 
          
          
        }
        
        
      }
      # #
      #
      #
      join4 <- left_join(join4_1,forecastData_m,by=c("sku"="SKU"))
      
      join4$qty_req_after_oos_before_recv[is.na(join4$qty_req_after_oos_before_recv)] <- 0
      
      join4$booking_reference[is.na(join4$Next_receipt_Date)] <- "Not on order"
      join4$booking_reference[!is.na(join4$Next_receipt_Date) & is.na(join4$booking_reference)] <- "Not Assigned"
      
      
      
      join4$On_Order_Qty[join4$booking_reference=="Not on order"] <- 0
      
      join4$Days_Untill_OOS[is.na(join4$Days_Untill_OOS) & (join4$Total_on_hand > join4$Forecasted_Annual_Qty | join4$sku %in% req_product$sku)] <- 999
      oos_Skus = join4$sku[ join4$Total_on_hand ==0 & (join4$Forecasted_Annual_Qty + join4$QtySold_lastYear + join4$QtySold_YTD>0 )]
      
      
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
      
      oosDates <- tryCatch(
        {
          oosDates <- dbGetQuery(rds_mishondb_con, glue("SELECT distinct Product,MAX(DATE(DATE)) AS DATE FROM productInventory WHERE Component_Type='MIS' AND Total_Quantity>0 GROUP BY Product"))
        },
        error=function(e)
        {
          return(NULL)
        })
      
      dbDisconnect(rds_mishondb_con)
      #
      oosDates2 =  oosDates[!grepl("KOOZIE|8TEN",toupper(oosDates$Product)),]
      
      oosDates2$refSKUs <- str_extract_all(oosDates2$Product,pattern="\\w{1}\\-\\w{3}\\-\\d{4}")%>%unlist()
      
      oosDates3 <- oosDates2[oosDates2$refSKUs %in% oos_Skus, ]
      
      oosDates3$Days_Untill_OOS2 <- as.numeric(as.Date(oosDates3$DATE) - Sys.Date())
      
      #
      
      join4 <- left_join(join4,oosDates3%>%select(refSKUs ,Days_Untill_OOS2),by=c("sku"="refSKUs"))
      
      join4$Days_Untill_OOS <- coalesce(join4$Days_Untill_OOS,join4$Days_Untill_OOS2)
      
      #Added to make sure 
      join4$Days_Untill_OOS[join4$QtySold_LifeTime>0 & join4$Forecasted_Annual_Qty==0] = 0
   
      join4$Days_Untill_OOS[is.na(join4$Days_Untill_OOS)] <- 999
      
      join4$OOS_Date <- as.character(Sys.Date() + as.numeric(join4$Days_Untill_OOS))
      
      
      join4$OOS_before_Receiving <- ifelse(join4$booking_reference=="Not on order","Not on order",
                                           ifelse(as.Date(join4$Next_receipt_Date)>= as.Date(join4$OOS_Date) |
                                                    is.na(join4$OOS_Date) ,
                                                  "TRUE","FALSE"))
      
      
      join4$Days_between_oos_receiving <-  ifelse(join4$OOS_before_Receiving=="TRUE" & !is.na(join4$OOS_Date),as.Date(join4$Next_receipt_Date)-as.Date(join4$OOS_Date),
                                                  ifelse(join4$OOS_before_Receiving=="TRUE" & is.na(join4$OOS_Date),as.Date(join4$Next_receipt_Date)-Sys.Date(),0))
      
      
      
      join4$Risk_Status <- ifelse(join4$booking_reference=="Not on order",
                                  ifelse((join4$Days_Untill_OOS > 0 & join4$delivery_lead_time > join4$Days_Untill_OOS) | join4$Days_Untill_OOS < 0 ,"At risk but not on order","Not at risk"),
                                  ifelse((join4$Days_Untill_OOS > 0 & join4$OOS_before_Receiving =="TRUE"  & join4$delivery_lead_time > join4$Days_Untill_OOS) | join4$Days_Untill_OOS < 0  ,"At risk on order","Not at risk"))
      
      
      
      join4$Risk_Status[join4$QtySold_LifeTime>0 & join4$Forecasted_Annual_Qty==0] = "Unable to identify because of Zero forecast"
      
      
      join4$Product_Status <- ifelse(join4$Total_on_hand ==0 & (join4$Forecasted_Annual_Qty + join4$QtySold_lastYear + join4$QtySold_YTD>0 ),"Out of stock",
                                     ifelse(join4$Total_on_hand ==0& (join4$Forecasted_Annual_Qty + join4$QtySold_lastYear + join4$QtySold_YTD==0), "New Product","In Stock"))
      
      # #
      
      fill_qty_req <- join4[join4$Days_between_oos_receiving>0 & join4$qty_req_after_oos_before_recv==0 & join4$Forecasted_Annual_Qty>0,]
      #
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
      
      reqSKUS2 <- paste0("'",fill_qty_req$sku,"'",collapse=",")
      
      seasonality2 <- tryCatch(
        {
          seasonality2 <- dbGetQuery(rds_mishondb_con, glue("select SKU,Date,sIndex from seasonality where SKU in ({reqSKUS2})"))
        },
        error=function(e)
        {
          return(NULL)
        })
      
      dbDisconnect(rds_mishondb_con)
      
     
      
      filledDF <- data.frame(
        sku = character(0),
        forecast= numeric(0)
      )
      
      for(req in 1:dim(fill_qty_req)[1])
      {
        prd_info = fill_qty_req[req,]
        
        dateSeq_forecast <- seq(Sys.Date(),as.Date(prd_info$Next_receipt_Date),1)
        
        dateFormatted = sapply(str_split(dateSeq_forecast,"-"),function(x) {return(paste(x[2],x[3],sep="-"))})
        
        
        seasonality_sum <- sum(seasonality2$sIndex[seasonality2$SKU==prd_info$sku & seasonality2$Date %in% dateFormatted])
        
        forecastInfo = (seasonality_sum * prd_info$Forecasted_Annual_Qty)/365
        
        filledDF <- filledDF%>%add_row(
          sku = prd_info$sku,
          forecast= round(forecastInfo,2)
        )
      }
      #
      # #
      
      join4$qty_req_after_oos_before_recv[join4$sku %in% filledDF$sku]<- filledDF$forecast
      
      
      prdct_Packaging3 = prdct_Packaging%>%
        filter(!is_individual)%>%
        group_by(default_code)%>%
        slice(which.min(sequence))%>%select(sku=default_code,height,width,length,weight=max_weight,Master_Carton_Qty=qty)

      
      join4_0_1 <- left_join(join4,prdct_Packaging3,by="sku")
      
      
      join4_0_2 <- left_join(join4_0_1,
                             stock_Move%>%group_by(sku)%>%
                               slice(which.min(as.Date(forecasted_delivery_date)))%>%
                               select(sku,forecasted_delivery_date),by="sku")%>%
        left_join(.,forecast_StatusData%>%select(-c(FC_QTY,LastUpdatedDate)),by=c("sku"="SKU"))%>%
        left_join(.,firstReceived,by="sku")
      
      
      join4_0_2$Last_Updated_Date <- Sys.Date()
      join4_0_2$booking_reference = stringi::stri_enc_toascii(as.character(join4_0_2$booking_reference))
      
      join4_0_2$cf_avail = as.character(join4_0_2$cf_avail)
      join4_0_2$nd_avail = as.character(join4_0_2$nd_avail)
      join4_0_2$ga_avail = as.character(join4_0_2$ga_avail)
      
      finalResultDF <- join4_0_2%>%select(
        sku,
        product_status,
        purchase_ok,
        cf_avail,
        nd_avail,
        ga_avail,
        CHES_on_hand,
        ND_on_hand,
        GA_on_hand,
        Total_on_hand,
        first_received_date,
        QtySold_YTD,
        QtySold_lastYear,
        QtySold_LifeTime,
        Forecasted_Annual_Qty,
        forecast_status,
        delivery_lead_time,
        min_order_qty = min_qty,
        Next_receipt_Date,
        forecasted_delivery_date,
        booking_reference,
        On_Order_Qty,
        OOS_Date,
        Days_Untill_OOS,
        OOS_before_Receiving,
        Days_between_oos_receiving,
        qty_req_after_oos_before_recv,
        Risk_Status,
        width_Inch = width,
        height_Inch = height,
        length_Inch = length,
        weight = weight,
        Master_Carton_Qty,
        Last_Updated_Date
        
        
        
        
        
      )
      
      
      
      
      #
      
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
      
      # #
      #
      #
      if(!is.null(rds_mishondb_con) & dim(finalResultDF)[1]>0)
      {
        if(DBI::dbExistsTable(rds_mishondb_con,"oos_inventory_report"))
        {
          DBI::dbSendQuery(rds_mishondb_con, "TRUNCATE TABLE oos_inventory_report")
        }
        DBI::dbWriteTable(rds_mishondb_con,"oos_inventory_report",finalResultDF,row.names=FALSE,append=TRUE)
        
        dbDisconnect(conn=rds_mishondb_con)
        
        
        res <- res%>%add_row(
          Job_Name = "OOS Inventory Report",
          Logged_Time = Sys.Date(),
          Job_Status = "SUCCESS",
          Message = "Successfully saved data"
        )
        print(glue("successfully saved data in OOS Inventory Report on {Sys.time()}"))
      } else{
        res <- res%>%add_row(
          Job_Name ="OOS Inventory Report",
          Logged_Time = Sys.Date(),
          Job_Status = "ERROR",
          Message = "No Data to save"
        )
        print(glue("No Data to save in OOS Inventory Report on {Sys.time()}"))
      }
      
      
      
    } else{
      res <- res%>%add_row(
        Job_Name ="OOS Inventory Report",
        Logged_Time = Sys.Date(),
        Job_Status = "ERROR",
        Message = "Error while reading data in OOS Inventory Report"
      )
      print(glue("Error while reading data in OOS Inventory Report on {Sys.time()}"))
    }
    
    
    
    
    
  } else{
    res <- res%>%add_row(
      Job_Name ="OOS Inventory Report",
      Logged_Time = Sys.Date(),
      Job_Status = "ERROR",
      Message = "Database connection failed"
    )
    print(glue("Database connection failed on {Sys.time()}"))
  }
  
  return(res)
  
  
}
