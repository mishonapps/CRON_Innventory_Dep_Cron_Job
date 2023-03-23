update_Replenishment_Report <- function(){
  res <- data.frame(
    Job_Name = character(0),
    Logged_Time =as.Date(character(0)),
    Job_Status = character(0),
    Message = character(0)
  )
  #####Database Connection###################
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
  if(!is.null(rds_mishondb) & !is.null(rds_odoodb))
  {
    productQuantityData <- tryCatch(
      {
        productQuantityData = dbGetQuery(rds_mishondb,"SELECT sku, CF_QTY + ND_QTY + GA_QTY as Total_QTY FROM total_received_quantity")
        
      },
      error=function(e)
      {
        return(NULL)
      })
    
    inv_consp_Pct <- tryCatch(
      {
        inv_consp_Pct <- dbGetQuery(rds_mishondb,"select consp.SKU,Sales_Last30,instock_Pct_150,sm.Qty_Sold_12Months,
                            annualForecast_CF,annualForecast_ND,annualForecast_CF+annualForecast_ND as forecast from inv_consp_purchasing as consp 
                                    left join slow_moving_products as sm on sm.sku = consp.SKU where purchase_ok='true' and product_status='active'")
        
      },
      error=function(e)
      {
        return(NULL)
      })
    
    
    min_max <- tryCatch(
      {
        min_max <- dbGetQuery(rds_mishondb,"select * from min_max_report")
      },
      error=function(e)
      {
        return(NULL)
      })
    
    instockDF <- tryCatch(
      {
        instockDF <- dbGetQuery(rds_mishondb,"select * from product_instock")
      },
      error=function(e)
      {
        return(NULL)
      })
    
    firstReceived <- tryCatch(
      {
        firstReceived <- dbGetQuery(rds_mishondb,"select sku,DATE as first_received_date from firstReceived")
      },
      error=function(e)
      {
        return(NULL)
      })
    
    
    reqSKUs <- paste0("'",inv_consp_Pct$SKU,"'",collapse=",")
    
    
    supplierInfo <- tryCatch(
      {
        supplierInfo <- dbGetQuery(rds_odoodb,glue("select product_template.default_code, partner.name,supplierinfo.sequence,supplierinfo.min_qty,supplierinfo.price  from 
                           product_supplierinfo as supplierinfo inner join product_template on 
                           supplierinfo.product_tmpl_id=product_template.id inner join res_partner as partner on partner.id=supplierinfo.name 
                           where product_template.default_code in ({reqSKUs})"))
        
      },
      error=function(e)
      {
        return(NULL)
      })
    
    packagingInfo <- tryCatch(
      {
        packagingInfo <- dbGetQuery(rds_odoodb,glue("select product_template.default_code,packaging.qty,packaging.sequence as seq_pack  from 
                           product_supplierinfo as supplierinfo inner join product_template on 
                           supplierinfo.product_tmpl_id=product_template.id  inner join 
                           product_packaging as packaging on packaging.product_tmpl_id = supplierinfo.product_tmpl_id 
                           where product_template.default_code in ({reqSKUs}) and  packaging.is_individual = false"))
        # a
      },
      error=function(e)
      {
        return(NULL)
      })
    
    stock_Move <- tryCatch(
      {
        stock_Move <- dbGetQuery(rds_odoodb,glue("select product.default_code as SKU,stock_move.id,source_location.complete_name as source,dest_location.complete_name as destination,date(stock_move.date_expected) as date,stock_move.product_qty,stock_move.booking_reference from stock_move
                                    inner join product_product as product on product.id=stock_move.product_id
                                    inner join stock_location as dest_location on dest_location.id=stock_move.location_dest_id
                                    inner join stock_location as source_location on source_location.id=stock_move.location_id
                                    where stock_move.state='assigned'"))
      },
      error=function(e)
      {
        return(NULL)
      })
    
    purchaseOrder <- tryCatch(
      {
        # #
        purchaseOrder <- dbGetQuery(rds_odoodb,glue("select po.name,product.default_code,partner.name,price_unit,pol.product_qty as qty_received,date(po.date_order) AS date_order,pol.state,pol.sequence from purchase_order_line as pol 
                                            inner JOIN  purchase_order AS po ON po.id=pol.order_id INNER join product_product as product on product.id = pol.product_id 
                                            inner join res_partner as partner on partner.id=pol.partner_id where product.default_code in ({reqSKUs}) and pol.state!='draft'"))
        
        
        
      },
      error=function(e)
      {
        return(NULL)
      })
    
    todays_inventory <- tryCatch(
      {
        ###Updated as requested by Utsav
        todays_inventory <- dbGetQuery(rds_mishondb,"SELECT Product,WH_Quantity,RTL_Quantity,GA_Quantity,WH_Quantity + RTL_Quantity+ GA_Quantity   as Total_Quantity FROM todays_inventory WHERE Component_Type = 'MIS'")
        
        
      },
      error=function(e)
      {
        return(NULL)
      })
    
    forecastStatusData <- tryCatch(
      {
        forecastStatusData = dbGetQuery(rds_mishondb,"select * from demand_Forecast_status_odc")
      },
      error=function(e)
      {
        return(NULL)
      })
    
    DBI::dbDisconnect(rds_odoodb)
    DBI::dbDisconnect(rds_mishondb)
    
    #  
    # #
    if(!is.null(productQuantityData)& !is.null(inv_consp_Pct) & !is.null(min_max)&
       !is.null(supplierInfo)& !is.null(packagingInfo) & !is.null(stock_Move)&
       !is.null(purchaseOrder) & !is.null(todays_inventory) & !is.null(instockDF) )
    {
      # #
      
      # productQuantityData1 <- productQuantityData%>%filter(!grepl("8TEN|KOOZ",Product))
      todays_inventory1 <- todays_inventory%>%filter(!grepl("8TEN|KOOZ",Product))
      
      # productQuantityData1$SKU = str_extract_all(productQuantityData1$Product,pattern="\\w{1}\\-\\w{3}\\-\\d{4}")%>%unlist()
      todays_inventory1$refSKU = str_extract_all(todays_inventory1$Product,pattern="\\w{1}\\-\\w{3}\\-\\d{4}")%>%unlist()
      
      productQuantityData$is_received = "FALSE"
      productQuantityData$is_received[productQuantityData$Total_QTY>0] = "TRUE"
      
      join1 <- left_join(inv_consp_Pct,productQuantityData%>%select(-c("Total_QTY")),by=c("SKU"="sku"))
      
      supplierInfo2 <- supplierInfo%>%group_by(default_code)%>%slice(which.min(sequence))
      packagingInfo2 <- packagingInfo %>%group_by(default_code)%>%slice(which.min(seq_pack))
      
      
      supplierInfo3 <- left_join(supplierInfo2,packagingInfo2,by="default_code")
      
      join2 <- left_join(join1,supplierInfo3%>%select(-c("sequence","seq_pack")),by=c("SKU"="default_code"))
      
      # purchaseOrder2 <- purchaseOrder%>%group_by(default_code)%>%sort()%>%slice(which.max(as.Date(date_order)))
      purchaseOrder2 <- purchaseOrder%>%group_by(default_code)%>%slice(which.max(as.Date(date_order)))
      
      
      
      join3 <- left_join(join2,purchaseOrder2%>%select(SKU=default_code,Last_Purchased_Order=name,price_unit,qty_received),by="SKU")
      
      join4 <- left_join(join3,min_max%>%select(SKU,Min_Value,Max_Value),by=c("SKU"))
      
      join5 <- left_join(join4,todays_inventory1%>%select(-Product),by=c("SKU"="refSKU"))
      join5$Total_Quantity[is.na(join5$Total_Quantity)] = 0
      
      ##############Check other Conditions to exclude interco 
      stock_Move2 <- stock_Move%>%filter(as.Date(date)>=Sys.Date()-30 &
                                           ( !grepl("cancel|Cancel",booking_reference) | is.na(booking_reference)) &
                                                (source == 'Partner Locations/Vendors'))%>%group_by(sku)%>%summarise(On_Order_Qty=sum(product_qty))
      
      # 
      # 
      
      join6 <- left_join(join5,stock_Move2,by=c("SKU"="sku"))
      join6$On_Order_Qty[is.na(join6$On_Order_Qty)] = 0
      
      join6$IS_Order_Required = join6$On_Order_Qty+join6$Total_Quantity<=join6$Min_Value
      # #
      join6$OrderQty = ifelse(join6$IS_Order_Required,join6$Max_Value-(join6$On_Order_Qty+join6$Total_Quantity),0)
      
      join6$is_received[is.na(join6$is_received)] = "FALSE"
      
      join7 <- join6%>%mutate(Min_Value_Final = ifelse(is.na(Min_Value),1,Min_Value),
                              Max_Value_Final = ifelse(is.na(Max_Value),ifelse(is.na(min_qty),Min_Value_Final,Min_Value_Final +min_qty ),Max_Value))
      
      
      
      
      
      #
      
      build_instock_distribution <- function(SKU)
      {
        reqSKUInfo = instockDF[grepl(SKU,instockDF$Bundle_Components),]%>%filter(!grepl("GHO",Product))
        reqSKUInfo$pctInstock <- (reqSKUInfo$instock/150)*100

        return(paste0(reqSKUInfo$Product,"(",reqSKUInfo$pctInstock,"%)",collapse=","))

      }

      join8<- join7%>%rowwise%>%mutate(last_150_pct_instock = build_instock_distribution(SKU))

   
      
      join9 <- left_join(join8,forecastStatusData%>%select(SKU,forecast_status),by="SKU")%>%left_join(.,firstReceived,by=c("SKU"="sku"))
   
      # #
      finalDF <- join9%>%select(SKU,
                                Vendor=name,
                                first_receipt_date=first_received_date,
                                MOQ=min_qty,
                                Sales_Last30_Days=Sales_Last30,
                                Instock_Pct_150Days=last_150_pct_instock,
                                R12_ND_Forecast=annualForecast_ND,
                                R12_CF_Forecast=annualForecast_CF,
                                R12_Forecast=forecast,
                                forecast_status = forecast_status,
                                Last_Purchased_Order,
                                Last_Purchased_Price = price_unit,
                                Last_Purchased_Qty = qty_received,
                                Master_Carton_Qty=qty,
                                IS_Order_Required,
                                OrderQty,
                                Re_Order_Min = Min_Value_Final,
                                Re_Order_Max = Max_Value_Final,
                                On_Order_Qty,
                                ND_On_Hand_Qty=RTL_Quantity,
                                CF_On_Hand_Qty=WH_Quantity,
                                GA_On_Hand_Qty=GA_Quantity,
                                Total_On_Hand_Qty=Total_Quantity,
                                Qty_Sold_Last_12Months = Qty_Sold_12Months)
      
      
      
      finalDF$Last_Updated_Date <- Sys.Date()
      
      
      # #
      #
      
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
      
      
      if(!is.null(rds_mishondb) & dim(finalDF)[1]>0)
      {
        if(DBI::dbExistsTable(rds_mishondb,"replenishment_report"))
        {
          DBI::dbSendQuery(rds_mishondb, "TRUNCATE TABLE replenishment_report")
        }
       
        DBI::dbWriteTable(rds_mishondb,"replenishment_report",finalDF,row.names=FALSE,append=TRUE)
        
        
        dbDisconnect(conn=rds_mishondb)
        
        
        res <- res%>%add_row(
          Job_Name = "Replenishment_report",
          Logged_Time = Sys.Date(),
          Job_Status = "SUCCESS",
          Message = "Successfully saved data"
        )
        print(glue("successfully saved data in Replenishment_report on {Sys.time()}"))
      } else{
        res <- res%>%add_row(
          Job_Name ="Replenishment_report",
          Logged_Time = Sys.Date(),
          Job_Status = "ERROR",
          Message = "No Data to save"
        )
        print(glue("No Data to save in Replenishment_report on {Sys.time()}"))
      }
      
      
      
      
    } else{
      res <- res%>%add_row(
        Job_Name ="Replenishment_report",
        Logged_Time = Sys.Date(),
        Job_Status = "ERROR",
        Message = "Error while reading data in Replenishment_report"
      )
      print(glue("Error while reading data in Replenishment_report on {Sys.time()}"))
    }
    
  }else{
    res <- res%>%add_row(
      Job_Name ="Replenishment_report",
      Logged_Time = Sys.Date(),
      Job_Status = "ERROR",
      Message = "Database connection failed"
    )
    print(glue("Database connection failed on {Sys.time()}"))
  }
  
  return(res)
}