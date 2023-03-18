updateCompanyWideInventory <- function()
{
  res <- data.frame(
    Job_Name = character(0),
    Logged_Time =as.Date(character(0)),
    Job_Status = character(0),
    Message = character(0)
  )
 # #
  #####FTP Connections###################
  FTP_Mishon_Host_URL <- Sys.getenv("FTP_Mishon_Host_URL")
  FTP_Mishon_Inv_url <- Sys.getenv("FTP_Mishon_Inv_url")
  FTP_Mishon_Inv_credentials <- Sys.getenv("FTP_Mishon_Inv_credentials")
  # #
  #### Get All File Names in FTP###############
  filenames <- tryCatch(
    {
      filenames <-  RCurl::getURL(FTP_Mishon_Host_URL, userpwd=FTP_Mishon_Inv_credentials,
                          ftp.use.epsv = TRUE,dirlistonly = TRUE)
    },
    error=function(e)
    {
      return(NULL)
    })
  
  ##### Database Connaction################
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
  if(!is.null(rds_mishondb_con) & !is.null(filenames))
  {
    ######## Get Existing Dates for which we have inventory data###########
    existingDates <- dbGetQuery(rds_mishondb_con,glue("select distinct Date from productInventory WHERE date(Date) BETWEEN '{Sys.Date()-30}' and '{Sys.Date()-1}'"))$Date
    
    # existingDates <- dbGetQuery(rds_mishondb_con,glue("select distinct Date from todays_inventory WHERE date(Date) >'{Sys.Date()-1}'"))$Date
    
    indvFilnames <- strsplit(filenames,'\r\n|\n')[[1]]
    reportFiles <- indvFilnames[grep(indvFilnames,pattern="\\_report")]
    
    ###### Actual Dates Last 15 Days###########
    actDates <- seq(Sys.Date()-30,Sys.Date()-1,1)
    # actDates <- seq(Sys.Date()-1,Sys.Date()-1,1)
    
    ##### Missing Dates in our Database from last 15 Days ##############
    reqDates <- actDates[as.character(actDates) %ni% existingDates]
    
   #
    #### Loop through all required Dates and save the Data for those dates if the corresponding inventory file exists#############
    if(length(reqDates)>0)
    {
      lapply(reqDates,function(x)
      {
        
        #####Adding +1 for the date because the inventory file we received today contains previous days Data
        ##### For Example on 2010-12-22 we have the data for 2010-12-21###############
        reqDate <- format(as.Date(x)+1,"%m%d%Y")
        reqFiles <- reportFiles[grep(reportFiles,pattern=glue("{reqDate}"))][1]
        
        # #
       
        #### Dowload data from the FTP
        downloadFile <- tryCatch(
          {
            download.file(glue("{FTP_Mishon_Inv_url}{reqFiles}"), destfile = "./data/ftpDownload_Product.zip",mode = "wb")
          },
          error=function(e)
          {
            return(NULL)
          })
        
        ##### If we are able to successfully download######
        if(!is.null(downloadFile))
        {
          # #
          print(glue("Updating Data of {x}"))
          
          unlink(glue("./data/extractedData"),recursive = TRUE)
          
          unzip("./data/ftpDownload_Product.zip",exdir="./data/extractedData")
          reqFile <- list.files("./data/extractedData")
          
          #  
          
          productData0 <- tryCatch(
            {
              productData0 <- fread(glue("./data/extractedData/{reqFile}"),sep="\t")
            },
            error=function(e)
            {
              return(NULL)
            })
          # #
          if(!is.null(productData0))
          {
          
            
            # #
            
            productData1 <- productData0 %>% select(-c("Attribute1Name","Attribute2Name","Attribute3Name",
                                                      "Attribute4Name","Attribute5Name","Attribute6Name",
                                                      "Attribute7Name","Attribute8Name","Attribute9Name",
                                                      "Attribute10Name","Attribute11Name","Attribute12Name",glue("V{length(productData0)}")))
            
            
           # #
            
            
             productData <- productData1%>%select(
              Inventory_Number=`Inventory Number`,
              Warehouse_Quantity=`Attribute5Value`,
              Retail_Store_Quantity=`Attribute7Value`,
              GA_Quantity=`Attribute6Value`,
              Total_Quantity=`Total Quantity`,
              Bundle_Components=`Bundle Components`,
              Buy_It_Now_Price=`Buy It Now Price`,
              Classification,
              Price_Live=`Attribute1Value`,
              Auto_DOHT=`Attribute2Value`,
              CF_Addon_Cost=`Attribute3Value`,
              Avg_Cost_CF=`Attribute4Value`,
              Product_Class=`Attribute8Value`,
              Amazon_Launch_Date=`Attribute9Value`,
              EBay_Launch_Date=`Attribute10Value`,
              Core_Component=`Attribute11Value`,
              Optimal_DC=`Attribute12Value`
            )
            
            
            
            
            
            productData$Price_Live <- as.numeric(productData$Price_Live)
            
            productData$Auto_DOHT[productData$Auto_DOHT=="NaN"] <- 0
            nums <- productData$Auto_DOHT[grep(productData$Auto_DOHT,pattern="\\(")] %>% 
              str_extract_all(pattern="\\d+\\.\\d+") %>% unlist() %>% as.numeric()
            
            
            productData$Auto_DOHT[grep(productData$Auto_DOHT,pattern="\\(")] <- nums * -1
            
            productData$Auto_DOHT <- as.numeric( productData$Auto_DOHT)
            
            data15 <- productData[grep(pattern="15\\w{3}",productData$Inventory_Number),]%>%
              filter(Classification!="PARENTSKUS")
            
            data15_NPar <-  data15[!grep(pattern="PARENT",data15$Inventory_Number),]
            
            data_kits_prn_BC <- data15_NPar[grep(pattern="15KIT|15PRN",data15_NPar$Inventory_Number),]%>%filter(Bundle_Components !="")
            
            
            data15_MIS <- data15_NPar[grep(pattern="15MIS",data15_NPar$Inventory_Number),]
            
            data_Mis_NGHO <- data15_MIS[!grep(pattern="GHO",data15_MIS$Inventory_Number),]
            
            #Data of Mis Ghost products with Bundle Components
            data_Mis_GHO_B <- data15_MIS[grep(pattern="GHO",data15_MIS$Inventory_Number),]%>%filter(Bundle_Components!="")
            
            #Data of Mis Ghost products with No Bundle Components
            data_Mis_GHO_NB <- data15_MIS[grep(pattern="GHO",data15_MIS$Inventory_Number),]%>%filter(Bundle_Components=="")
           
            #Build Bundle Components for Ghost products
            for(p in 1:length(data_Mis_GHO_NB$Inventory_Number))
            {
              # p=1
              # print(p)
              product = data_Mis_GHO_NB$Inventory_Number[p]
              
              reqProduct <- str_extract(product,pattern = "15MIS\\_\\w+\\-\\w{3}\\-\\d+")
              data_Mis_GHO_NB$Bundle_Components[p] <- glue("{reqProduct}=1")
              data_Mis_GHO_NB$Core_Component[p] <- reqProduct
              
            }
            
            #Build Bundle Components for Non Ghost products
            data_Mis_NGHO$Bundle_Components[data_Mis_NGHO$Bundle_Components==""] <- glue("{data_Mis_NGHO$Inventory_Number[data_Mis_NGHO$Bundle_Components=='']}=1")
            
            #Combine Mis(Non parent (Ghost + Non Ghost), )
            Final_MIS0 <- rbind(data_Mis_NGHO,data_Mis_GHO_B,data_Mis_GHO_NB)
            
            #
            
            Final_MIS_NC <- Final_MIS0%>%filter(Core_Component=="")
            # %>%filter(Classification!="")
            Final_MIS_C <- Final_MIS0%>%filter(Core_Component!="")
            
            #
            # Final_MIS_NC$Core_Component <-  str_extract_all(Final_MIS_NC$Bundle_Components,pattern = "15MIS\\_\\w+\\-\\w{3}\\-\\d+|15MIS_Koozie1") %>% unlist()
            
            Final_MIS_NC$Core_Component <- lapply(str_split(Final_MIS_NC$Bundle_Components,"=1"), `[[`, 1)%>%unlist()
            # lapply(x,
            Final_MIS<- rbind(Final_MIS_C,Final_MIS_NC)
            
            data_FBA <-  data15_NPar[grep(pattern="15FBA",data15_NPar$Inventory_Number),]
            #
            #Since FBA Products does not have any bundle component information, get them from corresponding KIT/MIS
            for(j in 1:length(data_FBA$Inventory_Number))
            {
              # j=460
              # print(j)
              product <- data_FBA$Inventory_Number[j]
              equivalentProduct = unlist(str_split(ifelse(length(grep("\\MK\\d+",product))==0,gsub("15FBA","15MIS",product),gsub("15FBA","15KIT",product)),"_G1"))[1]
              kit = data15_NPar[data15_NPar$Inventory_Number %in% equivalentProduct,]
              
              isMIS <- any(grep(pattern="15MIS",kit$Inventory_Number))
              

              if(isMIS)
              {
                kit2 = Final_MIS[Final_MIS$Inventory_Number %in% equivalentProduct,]
                data_FBA[j,"Inventory_Number"]<- product
                data_FBA[j,"Bundle_Components"]<- ifelse(length(kit2$Bundle_Components)>0,kit2$Bundle_Components,"")
                data_FBA[j,"Core_Component"]<- ifelse(length(kit2$Core_Component)>0,kit2$Core_Component,"")
                data_FBA[j,"Classification"]<-ifelse(length(kit2$Classification)>0,kit2$Classification,"")
              }
              else
              {
                data_FBA[j,"Inventory_Number"]<- product
                data_FBA[j,"Bundle_Components"]<- ifelse(length(kit$Bundle_Components)>0,kit$Bundle_Components,"")
                data_FBA[j,"Core_Component"]<- ifelse(length(kit$Core_Component)>0,kit$Core_Component,"")
                data_FBA[j,"Classification"]<- ifelse(length(kit$Classification)>0,kit$Classification,"")
              }
            }
            #
            #
            productData_initial <- rbind(Final_MIS,data_kits_prn_BC,data_FBA)
            
            # #
            #Remove if there were any 12WPS Products
            productData_initial <- productData_initial[!grep("12WPS",productData_initial$Bundle_Components),]
            #
            
            data15_MIS <- productData_initial[grep(pattern="15MIS",productData_initial$Inventory_Number),]
            
            # product_MIS0 = left_join(data15_MIS%>%select(-c("Bundle_Components","Classification")),ccData,by="Inventory_Number")
            product_MIS_NA <- data15_MIS[is.na(Warehouse_Quantity),]
            product_MIS_NotNA <- data15_MIS[!is.na(Warehouse_Quantity),]
            
            #
            
            productData_MIS1 <- data.frame(
              Product =character(0),
              WH_Quantity = integer(0),
              RTL_Quantity = integer(0),
              GA_Quantity = integer(0),
              Total_Quantity= integer(0),
              Classification = character(0),
              Bundle_Components = character(0),
              Core_Component = character(0),
              Auto_DOHT = integer(0),
              Price_Live = integer(0),
              CF_Addon_Cost = integer(0),
              Avg_Cost_CF = integer(0),
              Product_Class = character(0),
              Buy_It_Now_Price =integer(0),
              Amazon_Launch_Date = character(0),
              EBay_Launch_Date = character(0),
              Optimal_DC = character(0),
              Date = character(0)
            )
            #
            #
            for(i in 1:length(product_MIS_NA$Inventory_Number))
            {
              
              # print(i)
              # i=3308
              req_product = product_MIS_NA$Inventory_Number[i]
              # req_product = "15KIT_MK1002564"
              req_productInfo <- product_MIS_NA %>% filter(Inventory_Number %in% req_product)
              
              # coreCompData <- ccData%>%filter(Inventory_Number==req_product)
              
              budleComponents <- unlist(strsplit(req_productInfo$Bundle_Components,","))
              
              if(length(budleComponents)>0)
              {
                # print(i)
                component <- budleComponents%>%str_extract_all(pattern = "15MIS\\_\\w+\\-\\w{3}\\-\\d+|15MIS_Koozie1|15MIS_8TEN")%>%unlist()
                req_quant <- budleComponents%>%str_extract_all(pattern ="\\=\\d+")%>%unlist()%>%str_replace_all("=","")%>%unlist()%>%as.numeric()
                
                compDF <- data.frame(Product=component,req_quant=req_quant)
                
                componentInfo0 <- data15_MIS %>% filter(Inventory_Number %in% component)
                
                componentInfo <- left_join(componentInfo0,compDF,by=c("Inventory_Number"='Product'))
                #
                #Check for MIS Products
                if(dim(componentInfo)[1]==length(budleComponents))
                {
                  componentInfo$org_whQuant = componentInfo$Warehouse_Quantity/componentInfo$req_quant
                  componentInfo$org_rtlQuant = componentInfo$Retail_Store_Quantity/componentInfo$req_quant
                  componentInfo$org_GAQuant = componentInfo$GA_Quantity/componentInfo$req_quant
                  
                  whQuant <- min(componentInfo$org_whQuant)
                  rtlQuant <- min(componentInfo$org_rtlQuant)
                  gaQuant <- min(componentInfo$org_GAQuant)
                  #
                  productData_MIS1 <- productData_MIS1 %>% add_row(
                    Product =req_product,
                    WH_Quantity = floor(whQuant),
                    RTL_Quantity = floor(rtlQuant),
                    GA_Quantity = floor(gaQuant),
                    # Total_Quantity= ifelse(req_productInfo$Total_Quantity==0,floor(whQuant)+floor(rtlQuant)+floor(gaQuant),req_productInfo$Total_Quantity),
                    Total_Quantity= req_productInfo$Total_Quantity,#Because Total Quantity is from CA and Individual Quantities are from SX
                    Classification = ifelse(req_productInfo$Classification=="",req_productInfo$Classification,req_productInfo$Classification),
                    Bundle_Components = req_productInfo$Bundle_Components,
                    Core_Component = req_productInfo$Core_Component,
                    Auto_DOHT = req_productInfo$Auto_DOHT,
                    Price_Live = req_productInfo$Price_Live,
                    CF_Addon_Cost = req_productInfo$CF_Addon_Cost,
                    Avg_Cost_CF = req_productInfo$Avg_Cost_CF,
                    Buy_It_Now_Price = req_productInfo$Buy_It_Now_Price,
                    Product_Class=req_productInfo$Product_Class,
                    Amazon_Launch_Date = req_productInfo$Amazon_Launch_Date,
                    EBay_Launch_Date = req_productInfo$EBay_Launch_Date,
                    Optimal_DC = req_productInfo$Optimal_DC,
                    Date = as.character(x)
                  )
                  
                }
                
                else
                {
                  next
                }
              }
              
            }
            #
            productData_MIS2 <- data.frame(
              Product =product_MIS_NotNA$Inventory_Number,
              WH_Quantity = product_MIS_NotNA$Warehouse_Quantity,
              RTL_Quantity = product_MIS_NotNA$Retail_Store_Quantity,
              GA_Quantity = product_MIS_NotNA$GA_Quantity,
              Total_Quantity= product_MIS_NotNA$Total_Quantity,
              Classification = product_MIS_NotNA$Classification,
              Bundle_Components = product_MIS_NotNA$Bundle_Components,
              Core_Component = product_MIS_NotNA$Core_Component,
              Auto_DOHT = product_MIS_NotNA$Auto_DOHT,
              Price_Live = product_MIS_NotNA$Price_Live,
              CF_Addon_Cost = product_MIS_NotNA$CF_Addon_Cost,
              Avg_Cost_CF = product_MIS_NotNA$Avg_Cost_CF,
              Buy_It_Now_Price = product_MIS_NotNA$Buy_It_Now_Price,
              Product_Class= product_MIS_NotNA$Product_Class,
              Amazon_Launch_Date = product_MIS_NotNA$Amazon_Launch_Date,
              EBay_Launch_Date = product_MIS_NotNA$EBay_Launch_Date,
              Optimal_DC = product_MIS_NotNA$Optimal_DC,
              Date = as.character(x)
            )
            
            productData_MIS <- rbind(productData_MIS1,productData_MIS2)
            
            data15_KIT_PRN <- productData_initial[grep(pattern="15KIT|15PRN",productData_initial$Inventory_Number),]%>%filter(Bundle_Components !="")
            
            #
            #
            productData_Kits <- data.frame(
              Product =character(0),
              WH_Quantity = integer(0),
              RTL_Quantity = integer(0),
              GA_Quantity =  integer(0),
              Total_Quantity= integer(0),
              Classification = character(0),
              Bundle_Components = character(0),
              Core_Component = character(0),
              Auto_DOHT = integer(0),
              Price_Live = integer(0),
              CF_Addon_Cost = integer(0),
              Avg_Cost_CF = integer(0),
              Buy_It_Now_Price =integer(0),
              Product_Class=character(0),
              Amazon_Launch_Date = character(0),
              EBay_Launch_Date = character(0),
              Optimal_DC = character(0),
              Date = character(0)
            )
            #
            for(i in 1:length(data15_KIT_PRN$Inventory_Number))
            {
              # i=432
              # print(i)
              req_product = data15_KIT_PRN$Inventory_Number[i]
              # req_product = "15KIT_MK1002564"
              req_productInfo <- data15_KIT_PRN %>% filter(Inventory_Number %in% req_product)
              
              # coreCompData <- ccData%>%filter(Inventory_Number==req_product)
              
              budleComponents <- unlist(strsplit(req_productInfo$Bundle_Components,","))
              
              if(length(budleComponents)>0)
              {
                
                component <- budleComponents%>%str_extract_all(pattern = "15MIS\\_\\w+\\-\\w{3}\\-\\d+")%>%unlist()
                req_quant <- budleComponents%>%str_extract_all(pattern ="\\=\\d+")%>%unlist()%>%str_replace_all("=","")%>%unlist()%>%as.numeric()
                
                if(length(component)==length(req_quant))
                {
                  compDF <- data.frame(Product=component,req_quant=req_quant)
                  
                  componentInfo0 <- productData_MIS %>% filter(Product %in% component)
                  
                  componentInfo <- left_join(componentInfo0,compDF,by=c('Product'))
                  
                  
                  if(dim(componentInfo)[1]==length(budleComponents))
                  {
                    componentInfo$org_whQuant = componentInfo$WH_Quantity/componentInfo$req_quant
                    componentInfo$org_rtlQuant = componentInfo$RTL_Quantity/componentInfo$req_quant
                    componentInfo$org_gaQuant = componentInfo$GA_Quantity/componentInfo$req_quant
                    
                    whQuant <- min(componentInfo$org_whQuant)
                    rtlQuant <- min(componentInfo$org_rtlQuant)
                    gaQuant <- min(componentInfo$org_gaQuant)
                    
                    productData_Kits <- productData_Kits %>% add_row(
                      Product =req_product,
                      WH_Quantity = floor(whQuant),
                      RTL_Quantity = floor(rtlQuant),
                      GA_Quantity = floor(gaQuant),
                      # Total_Quantity= ifelse(req_productInfo$Total_Quantity==0,floor(whQuant)+floor(rtlQuant)+floor(gaQuant),req_productInfo$Total_Quantity),
                      Total_Quantity= req_productInfo$Total_Quantity,
                      Classification = ifelse(req_productInfo$Classification=="",req_productInfo$Classification,req_productInfo$Classification),
                      Bundle_Components = req_productInfo$Bundle_Components,
                      Core_Component = req_productInfo$Core_Component,
                      Auto_DOHT = req_productInfo$Auto_DOHT,
                      Price_Live =req_productInfo$Price_Live,
                      CF_Addon_Cost = req_productInfo$CF_Addon_Cost,
                      Avg_Cost_CF = req_productInfo$Avg_Cost_CF,
                      Buy_It_Now_Price = req_productInfo$Buy_It_Now_Price,
                      Product_Class=req_productInfo$Product_Class,
                      Amazon_Launch_Date = req_productInfo$Amazon_Launch_Date,
                      EBay_Launch_Date = req_productInfo$EBay_Launch_Date,
                      Optimal_DC = req_productInfo$Optimal_DC,
                      Date = as.character(x)
                    )
                    
                  }
                  
                  else
                  {
                    next
                  }
                }
                
              }
              
            }
            
            #
            
            #
           
            productData__1 <- rbind(productData_MIS,productData_Kits)
            # #
            # productData__1$Total_Quantity <- productData__1$WH_Quantity + productData__1$GA_Quantity + productData__1$RTL_Quantity
            
            data15_FBA <- productData_initial[grep(pattern="15FBA",productData_initial$Inventory_Number),]
            # product_FBA0 = left_join(data15_FBA%>%select(-c("Bundle_Components","Classification")),ccData,by="Inventory_Number")
            
           
     # ********* Add this code just in case there were no FBA components in the Export**************       
            
             # if(dim(data15_FBA)[1]>0)
            # {
            #   productData_FBA <- data.frame(
            #     Product =data15_FBA$Inventory_Number,
            #     WH_Quantity = data15_FBA$Warehouse_Quantity,
            #     RTL_Quantity = data15_FBA$Retail_Store_Quantity,
            #     Total_Quantity= data15_FBA$Total_Quantity,
            #     Classification = data15_FBA$Classification,
            #     Bundle_Components = data15_FBA$Bundle_Components,
            #     Core_Component = data15_FBA$Core_Component,
            #     Auto_DOHT = data15_FBA$Auto_DOHT,
            #     Buy_It_Now_Price = data15_FBA$Buy_It_Now_Price,
            #     Product_Class=data15_FBA$Product_Class,
            #     Amazon_Launch_Date = data15_FBA$Amazon_Launch_Date,
            #     EBay_Launch_Date = data15_FBA$EBay_Launch_Date,
            #     Optimal_DC = data15_FBA$Optimal_DC,
            #     Date = as.character(x)
            #   )
            #   
            #   productData_Final <- rbind(productData,productData_FBA)
            # } else{
            #   productData_Final <- productData
            # }
            
              productData_FBA <- data.frame(
                Product =data15_FBA$Inventory_Number,
                WH_Quantity = data15_FBA$Warehouse_Quantity,
                RTL_Quantity = data15_FBA$Retail_Store_Quantity,
                GA_Quantity = data15_FBA$GA_Quantity,
                Total_Quantity= data15_FBA$Total_Quantity,
                Classification = data15_FBA$Classification,
                Bundle_Components = data15_FBA$Bundle_Components,
                Core_Component = data15_FBA$Core_Component,
                Auto_DOHT = data15_FBA$Auto_DOHT,
                Price_Live =data15_FBA$Price_Live,
                CF_Addon_Cost = data15_FBA$CF_Addon_Cost,
                Avg_Cost_CF = data15_FBA$Avg_Cost_CF,
                Buy_It_Now_Price = data15_FBA$Buy_It_Now_Price,
                Product_Class=data15_FBA$Product_Class,
                Amazon_Launch_Date = data15_FBA$Amazon_Launch_Date,
                EBay_Launch_Date = data15_FBA$EBay_Launch_Date,
                Optimal_DC = data15_FBA$Optimal_DC,
                Date = as.character(x)
              )
              
              productData_Final <- rbind(productData__1,productData_FBA)
           
              #
            
            
            
           
            #Adding Component Type Variable
            productData_Final$Component_Type=""
            productData_Final$Component_Type[grep("MIS",productData_Final$Product)] <- "MIS"
            productData_Final$Component_Type[grep("MIS.*MPK",productData_Final$Product)] <- "MIS_MPK"
            productData_Final$Component_Type[grep("MIS.*GHO|MIS.*G\\d{1,2}",productData_Final$Product)] <- "MIS_GHOST"
            productData_Final$Component_Type[grep("KIT",productData_Final$Product)] <- "KIT"
            productData_Final$Component_Type[grep("KIT.*G\\d{1,2}",productData_Final$Product)] <- "KIT_GHOST"
            productData_Final$Component_Type[grep("FBA",productData_Final$Product)] <- "FBA"
            productData_Final$Component_Type[grep("FBA.*MPK",productData_Final$Product)] <- "FBA_MPK"
            productData_Final$Component_Type[grep("FBA.*G\\d{1,2}",productData_Final$Product)] <- "FBA_GHOST"
            productData_Final$Component_Type[grep("PRN",productData_Final$Product)] <- "PRN"
            productData_Final$Component_Type[grep("PRN*G\\d{1,2}",productData_Final$Product)] <- "PRN_GHOST"
            
            #
            finalDF <- productData_Final[!is.na(productData_Final$Product), ]
            finalDF$Product <- toupper(finalDF$Product)
            
            finalDF =  finalDF[!grepl("KOOZIE|8TEN",finalDF$Product),]
            
            #
            
            ####Validate Number of Products
           # #
            
            PreviousDays_Total <- dbGetQuery(rds_mishondb_con,glue("select count(Product) as count from todays_inventory WHERE date(Date) = '{as.Date(x)-1}'"))
            
            
            res2 <- data.frame(
              Report = "Companywide Inventory",
              Column_Name = "Number of Products",
              Status = ifelse(dim(finalDF)[1]>=PreviousDays_Total$count,"SUCCESS","ERROR"),
              Message = ifelse(dim(finalDF)[1] >= PreviousDays_Total$count ,"No Missing Products",paste0(PreviousDays_Total$count-dim(finalDF)[1]," Products were missing")),
              Validated_On = Sys.Date()
            )
            
            #
            DBI:: dbWriteTable(rds_mishondb_con,"Pre_Processing_Data_Validation",res2,row.names=FALSE,append=TRUE)
            #
             #
            # #
            
            #
            if(!is.null(finalDF) & dim(finalDF)[1]>0)
            {
              # #
              writeDB1 <- tryCatch(
                {
                  DBI:: dbWriteTable(rds_mishondb_con,"productInventory",finalDF%>%select(-c("Auto_DOHT","Buy_It_Now_Price")),row.names=FALSE,append=TRUE)
                  DBI::dbWriteTable(rds_mishondb_con,"todays_inventory",finalDF%>%select(-c("Auto_DOHT","Buy_It_Now_Price")),overwrite=TRUE,row.names=FALSE)
                  # DBI::dbWriteTable(rds_mishondb_con,"avg_cost_sellable_sku",finalDF%>%select(c("Product","Date","Avg_Cost_CF")),overwrite=TRUE,row.names=FALSE)
                },
                error=function(e)
                {
                  return(NULL)
                })
              
              
              if(!is.null(writeDB1))
              {
                res <- res%>%add_row(
                  Job_Name =as.character(glue("Company_Wide_Inventory")),
                  Logged_Time = Sys.Date(),
                  Job_Status = "SUCCESS",
                  Message = "Successfully saved data"
                )
                print(glue("successfully saved data on {Sys.time()}"))
                dbWriteTable(rds_mishondb_con,"cron_status_log",res,row.names=FALSE,append=TRUE)
              } else
              {
                res <- res%>%add_row(
                  Job_Name = as.character(glue("Company_Wide_Inventory")),
                  Logged_Time = Sys.Date(),
                  Job_Status = "ERROR",
                  Message = "Error while Saving data"
                )
                # res <- append("ERROR",res)
                print(glue("Error while Saving data on {Sys.time()}"))
                dbWriteTable(rds_mishondb_con,"cron_status_log",res,row.names=FALSE,append=TRUE)
              }
            }else{
              res <- res%>%add_row(
                Job_Name = as.character(glue("Company_Wide_Inventory")),
                Logged_Time = Sys.Date(),
                Job_Status = "ERROR",
                Message = "No Data saved"
              )
              # res <- append("ERROR",res)
              print(glue("No Data saved on {Sys.time()}"))
              dbWriteTable(rds_mishondb_con,"cron_status_log",res,row.names=FALSE,append=TRUE)
            }
            
            # file.remove(glue("./data/extractedData/{reqFile}"))
            
            
          }
          else
          {
            res <- res%>%add_row(
              Job_Name = as.character(glue("Company_Wide_Inventory")),
              Logged_Time = Sys.Date(),
              Job_Status = "ERROR",
              Message = "File has no data or error while reading"
            )
            # res <- append("ERROR",res)
            print(glue("File has no data or error while reading for {x} on {Sys.time()}"))
            dbWriteTable(rds_mishondb_con,"cron_status_log",res,row.names=FALSE,append=TRUE)
          }
          
        }
        else{
          res <- res%>%add_row(
            Job_Name = as.character(glue("Company_Wide_Inventory")),
            Logged_Time = Sys.Date(),
            Job_Status = "ERROR",
            Message = "Error While Reading Inventory Data from FTP"
          )
          # res <- append("ERROR",res)
          print(glue("Error While Reading Inventory Data from FTP on {Sys.time()}"))
          dbWriteTable(rds_mishondb_con,"cron_status_log",res,row.names=FALSE,append=TRUE)
        }
        
        
        
      })
    }else{
      res <- res%>%add_row(
        Job_Name = "Company_Wide_Inventory",
        Logged_Time = Sys.Date(),
        Job_Status = "WARNING",
        Message = "No Data to save"
      )
      print(glue("No Data to save on {Sys.time()}"))
    }
    
    
    dbDisconnect(rds_mishondb_con)
  }else
  {
    res <- res%>%add_row(
      Job_Name = "Company_Wide_Inventory",
      Logged_Time = Sys.Date(),
      Job_Status = "ERROR",
      Message = "Database or FTP Connection failed"
    )
    
   
 
    print(glue("Database or FTP Connection failed on {Sys.time()}"))
    
   
  }
  
  return(res)
  
}
