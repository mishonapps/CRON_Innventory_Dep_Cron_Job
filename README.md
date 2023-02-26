# Overview
This cronjob contains all the reports that are dependent on Latest inventory values, because new inventory values are pushed from SX to CA at 4 am 
Hence this cronjob also runs after 4 am. The following are the Data/Reports generated with this cron.

# Company-wide inventory Data
Here we take daily inventory from FTP server (companywideinventory). 

**Points to remember**
1) ND,CF and GA inventory Quantites are from SX, However Total Quantity(is not the sum of all the three quantites) instead it is from CA
          
          What is the difference between SX and CA Quantites?
          SX Quantity = This the total on hand quantity (Doesn't matter if they are available to sell or not)
          CA Quantity = This is the total available quantity
          
The formatted data will be saved into the following table of mishondb.

**Table Schema 1:** *productInventory*

COLUMN_NAME         |  DATA_TYPE  |  Comment
--------------------|-------------|-------------------------
Product             |  text       |  Sellable SKU
WH_Quantity         |  bigint     |  Chesterfield on hand Quantity from SX
RTL_Quantity        |  bigint     |  New Dundee on hand Quantity from SX
GA_Quantity         |  bigint     |  Georgia on hand Quantity from SX
Total_Quantity      |  bigint     |  Total available Quantity from CA
Date                |  text       |  Date
Bundle_Components   |  text       |  Bundle_Components
Core_Component      |  text       |  Core_Component
Price_Live          |  double     |  Price_Live
CF_Addon_Cost       |  double     |  CF_Addon_Cost
Avg_Cost_CF         |  double     |  Avg_Cost_CF
Classification      |  text       |  Classification
Component_Type      |  text       |  Component_Type (MIS/KIT/FBA)
Product_Class       |  text       |  Product_Class
Amazon_Launch_Date  |  text       |  Amazon_Launch_Date
EBay_Launch_Date    |  text       |  EBay_Launch_Date
Optimal_DC          |  text       |  Optimal_DC

# Slow Moving Products
This function helps us to determine the slow moving products based on the forecasted quantity and Quantity sold in last 12 and 24 months. This Reports data will be saved in the following table of mishondb

**Table Schema 1:** *slow_moving_products*

COLUMN_NAME         |  DATA_TYPE  | Comment
--------------------|-------------|-------------------
ID                  |  int        | Unique ID
sku                 |  text       | sku
min_qty             |  double     | minimun order quantity
Qty_Sold_12Months   |  double     | Quantity Sold in 12Months
Qty_Sold_24Months   |  double     | Quantity Sold in 24Months
QtySold_Total       |  double     | QtySold_Total
FC_Qty              |  double     | Forecasted Quantity
CHES_on_hand        |  double     | CHES_on_hand
ND_on_hand          |  double     | New Dundee_on_hand
Total_on_hand       |  double     | Total_on_hand
Launch_Date         |  text       | When the product was first launched
Last_Updated_Date   |  text       | Last_Updated_Date

# Out of stock Inventory Report
This function helps us to determine the products that are at risk of out of stock based on forecasted quantity, on hand quantity, Incoming dates, MOQ and lead times.
This Reports data will be saved in the following table of mishondb

**Table Schema 1:** *oos_inventory_report*
COLUMN_NAME                    | DATA_TYPE |  Comment
-------------------------------|-----------|----------------------------------
ID                             | int       |  Unique ID
sku                            | text      |  Purcahse SKU
min_qty                        | double    |  min_qty
delivery_lead_time             | bigint    |  delivery_lead_time
QtySold_YTD                    | double    |  QtySold_YTD
QtySold_lastYear               | double    |  QtySold_lastYear
Forecasted_Annual_Qty          | double    |  Forecasted_Annual_Qty
CHES_on_hand                   | double    |  CHES_on_hand
ND_on_hand                     | double    |  ND_on_hand
GA_on_hand                     | double    |  GA_on_hand
Total_on_hand                  | double    |  Total_on_hand
Next_receipt_Date              | text      |  next receiving date
booking_reference              | text      |  from stock moves
On_Order_Qty                   | double    |  On_Order_Qty
Days_Untill_OOS                | double    |  Days untill out of stock
qty_req_after_oos_before_recv  | double    |  qty_req_after_oos_before_recv
OOS_Date                       | text      |  Out of stock Date
OOS_before_Receiving           | text      |  will the sku be OOS before Receiving
Days_between_oos_receiving     | double    |  Days between Out of stock and next receiving date
Risk_Status                    | text      |  Whether the product is at risk or not
Product_Status                 | text      |  Product_Status
height                         | double    |  height
width                          | double    |  width
length                         | double    |  length
Master_Carton_Qty              | double    |  Master_Carton_Qty
forecasted_delivery_date       | text      |  forecasted_delivery_date
Last_Updated_Date              | text      |  Last_Updated_Date


