


#=================== Exporting ggplot from databricks to Azure blob storage


library(stringr)

# Filter for 0-500 days
winback_plot <- winback_plot %>% dplyr::filter(days_to_winback >= 0 & days_to_winback <= 1000)

# Plot
 P=ggplot(winback_plot, aes(x = days_to_winback)) +
  geom_histogram(
    bins = 50,
    fill = "#4C72B0",   # nicer steelblue
    color = "white",    # bar outlines
    alpha = 0.5
  ) +
  geom_vline(
    aes(xintercept = median(days_to_winback, na.rm = TRUE)),
    color = "#D55E00",  # red-orange
    linetype = "dashed",
    size = 1
  ) +
  annotate(
    "text",
    x = median(winback_plot$days_to_winback, na.rm = TRUE),
    y = max(table(cut(winback_plot$days_to_winback, 50))) * 0.20,
    label = paste0("Median: ", round(median(winback_plot$days_to_winback, na.rm = TRUE))),
    color = "#D55E00",
    angle = 90,
    vjust = -0.5
  ) +
  scale_x_continuous(limits = c(0, 600)) +
  scale_y_continuous(limits = c(0, 10000)) +
  labs(
    title = "Days Between Churn and Winback",
    subtitle = str_wrap(paste0("Histogram of customer winback timing in days.
              Returning customers takes ", round(median(winback_plot$days_to_winback, na.rm = TRUE))," median days to come back,while the other take longer days "),width=80),
    x = "Days After Churn",
    y = "Number of Customers"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "bold"),
    plot.subtitle = element_text(color = "gray40"),
    axis.title = element_text(face = "bold")
  )+
  theme_bw()+
  theme_classic()+
  theme(
    plot.title = element_text(face = "bold"),       # Bold title
    plot.subtitle = element_text(color = "gray40"), # Optional styling for subtitle
    axis.title = element_text(face = "bold")
  )
    # theme_dark()
  # theme_light()


# Save as PNG in Databricks file system
ggsave("/dbfs/tmp/winback_plot.png", plot = P, width = 8, height = 5, dpi = 300)


library(AzureStor)


bl_endp_key2 <- storage_endpoint("https://mlblobman01.blob.core.windows.net/", key= dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01"))

list_storage_containers(bl_endp_key2)

cont <- storage_container(bl_endp_key2, "directcampaing")

# Upload local file
upload_blob(cont, src = "/dbfs/tmp/winback_plot.png", dest = "ggplot/winback_plot.png")




#==================== Reading 

DataDF = sparklyr::spark_read_csv(sc,"/FileStore/tables/UCL_Barc_Winback.csv")

display(DataDF)







# Databricks notebook source
#storage_account_name = "STORAGE_ACCOUNT_NAME"
#storage_account_access_key = "YOUR_ACCESS_KEY"

library(AzureStor)



# COMMAND ----------



# COMMAND ----------


# bl_endp_key <- storage_endpoint("https://stordatabricksprodtod.blob.core.windows.net", key= dbutils.secrets.get(scope = "SportsPkey", key = "SportsBlobPS"))

# COMMAND ----------

#mlblobman

#https://mlblobman-secondary.blob.core.windows.net/

# bl_endp_key2 <- storage_endpoint("https://mlblobman.blob.core.windows.net", key= dbutils.secrets.get(scope = "mlstorage7", key = "mlman7"))

bl_endp_key2 <- storage_endpoint("https://mlblobman01.blob.core.windows.net/", key= dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01"))

#bl_endp_key2 <- storage_endpoint("https://mlblobman.blob.core.windows.net", key= bb)

# COMMAND ----------

list_storage_containers(bl_endp_key2)

# COMMAND ----------

list_storage_containers(bl_endp_key2)

# COMMAND ----------

cont <- storage_container(bl_endp_key2, "test1")

cont <- storage_container(bl_endp_key2, "overlappingsportent")

# COMMAND ----------

list_storage_files(cont)

# COMMAND ----------

library(dplyr)

# COMMAND ----------

mtcars_test = mtcars %>% as.data.frame()

# COMMAND ----------

storage_write_csv(mtcars_test, cont, "mtcars_test.csv")

# COMMAND ----------



#============= Saving the file ==========

library(AzureStor)

bl_endp_key2 <- storage_endpoint("https://mlblobman01.blob.core.windows.net/", key= dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01"))

list_storage_containers(bl_endp_key2)

cont <- storage_container(bl_endp_key2, "directcampaing")

# # ======== Active & Churned Subs for Last three months =======

Final_FAN_Churned_NonViewingMinutes_EmailId_DF = Final_FAN_Churned_NonViewingMinutes_EmailId_SDF %>% as.data.frame()

File_Active_Name = paste0('CampaignAutomation/Final_FAN_Churned_NonViewingMinutes_EmailId_DF',Sys.Date(),'.csv')

storage_write_csv(Final_FAN_Churned_NonViewingMinutes_EmailId_DF,cont,File_Active_Name)

return(Final_FAN_Churned_NonViewingMinutes_EmailId_DF)

}




mtnew <- storage_read_csv(cont, "assetidv3")

# COMMAND ----------

display(mtnew)

# COMMAND ----------

names(mtnew) = c('Match','Asset1','Asset2','Asset3')

# COMMAND ----------

mtnew = 

# COMMAND ----------

library(dplyr)

mtnew%>%data.frame()

# COMMAND ----------

vieweridblobv3 <- storage_read_csv(cont, "deviceidblobwcv2")



# COMMAND ----------

display(vieweridblobv3)

# COMMAND ----------

k = vieweridblobv3%>%select(DevDetails)%>% mutate(.,DevDetails = DevDetails)

# COMMAND ----------

d=data.frame(jsoncol=k$DevDetails,stringsAsFactors = FALSE)
d

# COMMAND ----------

d %>% mutate(.,FFF = jsonlite::fromJSON(jsoncol))

# COMMAND ----------

do.call(rbind.data.frame, lapply(d$jsoncol, jsonlite::fromJSON(., flatten=TRUE)))

# COMMAND ----------

#library(jsonlite)
#install.packages(tidyjson)
library(rjson) 



# COMMAND ----------

#vieweridblobv3 %>% select(.,DevDetails)%>%
# dplyr::mutate(DevDetails = map(DevDetails, ~ fromJSON(.) %>% as.data.frame())) %>% unnest(DevDetails)

# do.call( rbind, 
#          lapply(vieweridblobv3$DevDetails, 
#                  function(j) as.list(unlist(fromJSON(j, flatten=TRUE)))
 #       )       )



#json_as_df <- raw_df$json %>% spread_all

# retain columns
#json_as_df <- raw_df %>% as.tbl_json(json.column = "json") %>% spread_all

#vieweridblobv3%>%select(.,DevDetails)%>%
#lapply(RcppSimdJson::fparse) %>% 
#  data.table::rbindlist(fill = TRUE)

vieweridblobv3_v1 = vieweridblobv3%>%select(.,DevDetails)%>%as.character()

#do.call(rbind.data.frame, lapply(vieweridblobv3_v1$DevDetails, jsonlite::fromJSON(.)))

vieweridblobv3_v1



# COMMAND ----------

df <- data.frame(json = c('{"client":"ABC Company","totalUSD":7110.0000,"durationDays":731,"familySize":4,"assignmentType":"Long Term","homeLocation":"Australia","hostLocation":"United States","serviceName":"Service ABC","homeLocationGeoLat":-25.274398,"homeLocationGeoLng":133.775136,"hostLocationGeoLat":37.09024,"hostLocationGeoLng":-95.712891}', '{"client":"ABC Company","totalUSD":7110.0000,"durationDays":731,"familySize":4,"assignmentType":"Long Term","homeLocation":"Australia","hostLocation":"United States","serviceName":"Service XYZ","homeLocationGeoLat":-25.274398,"homeLocationGeoLng":133.775136,"hostLocationGeoLat":37.09024,"hostLocationGeoLng":-95.712891}'))

df

# COMMAND ----------

do.call(rbind.data.frame, lapply(df$json, jsonlite::fromJSON))

# COMMAND ----------

library(tidyr)
library(tidyverse)
library(reshape2)


# COMMAND ----------

dataset_DeviceHardware= vieweridblobv3%>%dplyr::select(.,DevDetails)%>%separate(.,DevDetails,c('K1','K2'),sep=",")
display(dataset_DeviceHardware)

# COMMAND ----------

df <- tibble(
  id = c(1, 2), 
  json_col = c('{"a": [1,2,3], "b": [4, 5, 6]}', '{"f": [100,2,8]}')
)
df


vieweridblobv3 %>%dplyr::select(.,)
 mutate(json_col = map(json_col, ~ fromJSON(.) %>% as.data.frame())) %>%
 unnest(json_col)

# COMMAND ----------

library(data.table)
library(dplyr)
library(lubridate)
library(sparklyr)
library(SparkR)
library(AzureStor)

# COMMAND ----------

#======================== Creating table in delta lake databricks =====================================

mtcars_test = mtcars %>% data.frame()

# COMMAND ----------

sc <- spark_connect(method = "databricks")

# COMMAND ----------

library(SparkR)
# Get the existing SparkSession that is already initiated on the cluster.
sparkR.session()

# COMMAND ----------

#mtcars_test_tbl = dplyr::copy_to(sc, mtcars_test ,'mtcars_test_V1')

#sdf_register(mtcars_test_tbl,'mtcars_test_tbl')

data.df = as.DataFrame(mtcars)


# COMMAND ----------

table_name = 'testtable'

SparkR::saveAsTable(df= data.df , tableName=table_name)



# =================== Writing to Databricks Delta Table ===========================

library(SparkR)
# Get the existing SparkSession that is already initiated on the cluster.
sparkR.session()

# COMMAND ----------

# =================== Writing to Databricks Delta Table ===========================

Final_DF = Final_Correct_AssetID %>% sdf_collect()

Final_DF2 = as.DataFrame(Final_DF)
#------ second method 

SparkR::saveAsTable(df= Final_DF2 , tableName= "Final_DF4", mode="overwrite" )


#================================ Writing to DB =============================

FinalData_V11 = FinalData_V1 %>% dplyr::select(.,asset_id,Type)%>% dplyr::rename(.,AssetID=asset_id)

FinalData_V12 = FinalData_V1 %>% dplyr::select(.,SessTg_VideoID,Type)%>% dplyr::rename(.,AssetID=SessTg_VideoID)

FinalData_F1 = dplyr::bind_rows(FinalData_V11,FinalData_V12)%>% dplyr::distinct() %>% dplyr::bind_rows(.,AssetIDMappingTable_DF)

FinalData_F1_DF  = as.DataFrame(FinalData_F1)

# FinalData_F1_DF = dplyr::copy_to(sc,FinalData_F1,'FinalData_F1')

# FinalData_F1_SDF = sdf_register(FinalData_F1_DF,'FinalData_F1_DF')

display(FinalData_F1_DF)

# COMMAND ----------


# ================== Writing the Raw data information related to asset,asset1,asset_id,competitionName,SessTg_VideoID


#----------- First Version -------------

# ================== Writing the Raw data information related to asset,asset1,asset_id,competitionName,SessTg_VideoID

AllCompetition_SD_dff = as.DataFrame(AllCompetition_SD_df)

path = paste("/user/hive/warehouse/AllCompetition_SD_df",lubridate::date(Sys.time()),sep='_')

SparkR::write.df(AllCompetition_SD_dff,path, source = "delta",mode="overwrite")

#-------------- Second Version -----------------

path = paste("/user/hive/warehouse/FinalData_F1_DF",lubridate::date(Sys.time()),sep='_')

 SparkR::write.df(FinalData_F1_DF,path, source = "delta",mode="overwrite")



# -------------- Reading the data from delta table  --------------------
FinalData_F1_DF_sdf = SparkR::read.df("/user/hive/warehouse/Final_DF4", source = "delta")

# ================= Reading 

library(SparkR)
# Get the existing SparkSession that is already initiated on the cluster.
sparkR.session()

# ---------------- Second processing as on 20th May 2024 , to read from delta table

AssetIDMappingTableV1 = SparkR::read.df("/user/hive/warehouse/assetidmappingtable_current", source = "delta")

AssetIDMappingTable = SparkR::distinct(AssetIDMappingTableV1 )

createOrReplaceTempView(AssetIDMappingTable, "AssetIDMappingTable")

# hive_metastore.default.assetidmappingtable_current
display(AssetIDMappingTable)

AssetIDMappingTable_df = sdf_sql(sc,"select * from AssetIDMappingTable")

display(AssetIDMappingTable_df)


# # *************************** Archiving the previous data reading from the Contentor ************************

library(AzureStor)

bl_endp_key2 <- storage_endpoint("https://mlblobman01.blob.core.windows.net/", key= dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01"))

list_storage_containers(bl_endp_key2)



cont <- storage_container(bl_endp_key2, "beinstaffvouchertod")

list_storage_files(cont)

beinstaffvouchertod <- storage_read_csv(cont, "beinstaffvouchercoupons.csv") %>% data.frame() 

beinstaffvouchertod_sdf = dplyr::copy_to(sc,beinstaffvouchertod,'beinstaffvouchertod')

sdf_register(beinstaffvouchertod_sdf,'beinstaffvouchertod_sdf')


#=========================================




library(SparkR)
# Get the existing SparkSession that is already initiated on the cluster.
sparkR.session()

# ---------------- Second processing as on 20th May 2024 , to read from delta table

AssetIDMappingTableV1 = SparkR::read.df("/user/hive/warehouse/assetidmappingtable_current", source = "delta")

AssetIDMappingTable = SparkR::distinct(AssetIDMappingTableV1 )

createOrReplaceTempView(AssetIDMappingTable, "AssetIDMappingTable")


#----------------- Reading and Writing back to Delat table ============


# =================== Reading data before 
# ---------------- Second processing as on 20th May 2024 , to read from delta table
# hive_metastore.default.final_f1_mapping_table
ffinal_f1_mapping_table = SparkR::read.df("/user/hive/warehouse/final_f1_mapping_table", source = "delta")

# AssetIDMappingTable = SparkR::distinct(AssetIDMappingTableV1 )

createOrReplaceTempView(ffinal_f1_mapping_table, "ffinal_f1_mapping_table")

# hive_metastore.default.assetidmappingtable_current
# display(AssetIDMappingTable)

ffinal_f1_mapping_table = sdf_sql(sc,"select * from ffinal_f1_mapping_table") %>% sdf_collect()

final_f1_mapping_tableDF = as.DataFrame(ffinal_f1_mapping_table)

TBName = paste("Final_F1_Mapping_Table",gsub("-","_",lubridate::date(Sys.time())),sep='_')

SparkR::saveAsTable(df= final_f1_mapping_tableDF , tableName=TBName, mode="overwrite" )


#======================== writing even if the schema is not matching that is any additional column added and overwriting the data will not merge due to additiaonal column but with option of "mergeschema" we can overwrite with existing table even if their is additional column ================



#==============================================================================================================================================================================
													Sparklyr
#================================================================================================================================================================================

Final_Survi = dplyr::inner_join(Final_Q1_Users,Active_Eligible_UserBase_AprSep,by=c('Customer_External_ID'))

#---------------------- writing sparklyr dataframe directly to Databrick catalog ------------------------

sparklyr::spark_write_table(Final_Survi,name='Final_Survi_Profile_Apr_Sep_11Sep2025',mode='overwrite')


#---------------------- reading sparklyr dataframe directly to Databrick catalog ------------------------


Final_Survi = sparklyr::spark_read_table(sc,name='Final_Survi_Profile_Apr_Sep_11Sep2025')



#======================== Sparklyr ==================

Final_F1_Data_DF = Final_F1_Data %>% sdf_collect()

Final_F1_Data_DF = SparkR::createDataFrame(Final_F1_Data_DF)

SparkR::saveAsTable(df= Final_F1_Data_DF , tableName="Final_F1_Viewership_Data_Chrsint_Aug24", mode="overwrite" )


#=================== Writing to delta using sparklyr =================================

# Convert R data frame to Spark DataFrame
spark_df <- sparklyr::copy_to(sc, AllCompetition_SD_V4, "AllCompetition_SD_V4")

# Write the Spark DataFrame to Delta Lake
sparklyr::spark_write_delta(
  spark_df,
  path = "/user/hive/warehouse/F1Viewership",
  mode = "append"
)


#--------------------- using sparkly to write to delta table -------------------------

ContentList_DF_V2 = FinalDF %>% dplyr::select(.,asset_id,asset1,sportname,competitionName) %>% sdf_distinct()%>%
                    dplyr::mutate(.,Remove = dplyr::if_else((is.na(sportname)|sportname=='null'|sportname=='NULL'|sportname==''|sportname=='Others'),1,0),Remove2 = dplyr::if_else((is.na(asset1)|asset1=='null'|asset1=='NULL'|asset1==''),1,0))%>%
                    dplyr::filter(.,Remove==0)%>%
                    dplyr::select(.,-Remove)



# ------------------ Writing the Content list ----------------------------------
#************************************************************************************
# #writing the spark table without converting into R dataframe using collect method
#************************************************************************************

sparklyr::spark_write_table(ContentList_DF_V2,name = 'FinalDF_test',mode = 'overwrite')



#----------------------- Second Method ----------------------------


# --------------------- Category of the Group ---------------

FinalViewingMinutes1 = AllCompetition_SD_PreviousData_v3 %>% 
            dplyr::mutate(., CompetitionMainNames = dplyr::case_when(
                            grepl('2. Bundesliga|Bundesliga', competitionName) ~ "Bundesliga",
                            grepl('AFC Champions League|AFC Champions League Elite', competitionName) ~ "AFC Champions League",
                            grepl('AFC Champions League Two|AFC Cup', competitionName) ~ "AFC Champions League Two",
                            grepl('Carabao Cup|EFL Cup', competitionName) ~ "Carabao Cup",
                            grepl('La Liga|LaLiga|Primera División', competitionName) ~ "La Liga",
                            grepl('La Liga2|Segunda División', competitionName) ~ "La Liga2",
                            grepl('Super Lig|Süper Lig', competitionName) ~ "Super Lig",
                            grepl('UEFA  Nations League|UEFA Nations League', competitionName) ~ "UEFA Nations League",
                            TRUE ~ competitionName
            ),
            sportnameMainNames = dplyr::case_when(
              grepl('Football|Soccer',sportname)~'Football',
              TRUE ~ sportname
            ))

# display(FinalViewingMinutes1)

sdf_register(FinalViewingMinutes1,'FinalViewingMinutes')

path = paste("/user/hive/warehouse/FinalViewingMinutes_DizziAlgeria",lubridate::date(Sys.time()),sep='_')

# SparkR::write.df(FinalViewingMinutes1,path, source = "delta",mode="overwrite")

spark_write_delta(FinalViewingMinutes1, path, mode = "overwrite")


# ============================== writing using sparklyr  and reading spark dataframe without converting into R dataframe =====================================

  Final_Partner_Direct_Merg_DIMSubs = sdf_sql(sc,"select * from DIM_SubscribersInfo_SD ") %>%
    dplyr::filter(Offer_type=='Subscription',Offer_period=='monthly',Subscription_latest==1)%>%#!Source_system %in% c('Partner')
    dplyr::mutate(.,MonthP = lubridate::month(Subscription_start_date),
                    YearP = lubridate::year(Subscription_start_date))%>%
    dplyr::select(.,Customer_External_ID,Customer_ID,Subscription_ID,Country,`Offer_type`,`Source_system`,Product_name,Offer_period,Subscription_start_date,Expiry_date,Winback_type,Tier,MonthP,YearP,Subscription_latest)%>%
    dplyr::arrange(.,Customer_External_ID,Subscription_start_date)%>%
    dplyr::left_join(Final_Partner_Direct,.,by=c('Customer_External_ID','Subscription_ID','Customer_ID'))%>%#'Customer_External_ID',
    dplyr::arrange(.,Customer_ID,`Subscription_start_date`)


    display(Final_Partner_Direct_Merg_DIMSubs)
	
#----------- writing to Delta table ----------


path = paste("/user/hive/warehouse/Final_Partner_Direct_Merg_DIMSubs")

 spark_write_delta(Final_Partner_Direct_Merg_DIMSubs,path,mode="overwrite")
 
 
 #------------- Reading Sparklyr library --------------------------
 
 
 delta_table_path="/user/hive/warehouse/Final_Partner_Direct_Merg_DIMSubs"

Final_Partner_Direct_Merg_DIMSubs <- spark_read_delta(sc, path = delta_table_path, name = "Final_Partner_Direct_Merg_DIMSubs")

display(Final_Partner_Direct_Merg_DIMSubs)




#----------------- Writing directly to unity catalog ------------

Cancellation_viewer_Analysis = sdf_sql(sc,'select * from Final_Viewer_cancellation_SDF ')%>% dplyr::filter(.,Flag_Viewership_Month==1)

sparklyr::spark_write_table(Cancellation_viewer_Analysis,name='Cancellation_viewer_Analysis_Filter_one',mode='overwrite')

#------------ Reading using sparklyr -------------------

Final_Survi = sparklyr::spark_read_table(sc,name='Final_Survi_Profile_Apr_Sep_11Sep2025')


#=================== Writing chunk format ==================================

library(sparklyr)
library(dplyr)

# Assuming `df` is your Spark DataFrame and `chunk_size` is the number of rows per chunk
chunk_size <- 500
total_rows <- sdf_nrow(ContentList_DF_V22)
num_chunks <- ceiling(total_rows / chunk_size)

# Partition the DataFrame into chunks
partitions <- sdf_partition(ContentList_DF_V22, 
                            chunk_size = chunk_size, 
                            num_partitions = num_chunks)

# Write each chunk to a separate table
for (i in seq_along(partitions)) {
  chunk <- partitions[[i]]
  table_name <- paste0("table_chunk_", i - 1)
  
  # Create the table if it does not exist
  DBI::dbExecute(sc, paste0("CREATE TABLE IF NOT EXISTS ", table_name, " (asset_id STRING, asset1 STRING, sportname STRING, competitionName STRING)"))
  
  # Write the chunk to the table
  sparklyr::spark_write_table(chunk, name = table_name, mode = "append")
}



#===================== Writing to blob Storage ======================



transactions <- plyr::ddply(FinalDF_V1_RDF, c("viewerId", "startdate"), function(df) paste(df$asset_name, collapse = ","))


transactions_tbl = dplyr::copy_to(sc,transactions,'transactions2')

sdf_register(transactions_tbl,'transactions_tbl')


 %python

 #display(Final_Sampler_Engager_Sdf)
transactions_tbl_sdf = spark.sql("select * from  transactions_tbl ")

transactions_tbl_sdf_Pdf  = transactions_tbl_sdf.toPandas()


%python

import pandas as pd
import pyspark
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
from pyspark.sql.functions import sum
from pyspark.sql.functions import countDistinct
from datetime import date, datetime, timedelta
from pyspark.sql.types import IntegerType

from azure.core.credentials import AzureNamedKeyCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import xlsxwriter
import io

writer = pd.ExcelWriter('RecommendationEngine.xlsx',engine='xlsxwriter')

access_key = dbutils.secrets.get(scope="BlobAccessMLBMan07",key="SportsBlobPS01")
credential = AzureNamedKeyCredential("mlblobman01", access_key)

account_name = 'mlblobman01'
access_key = dbutils.secrets.get(scope="mlstorage7",key="mlman7")
container_name = 'formulafinaldata'
 # blob_name = 'test.xlsx'
account_url = 'https://mlblobman01.blob.core.windows.net/'


blob_service_client = BlobServiceClient(account_url, credential=credential)


output = io.BytesIO()

 # Use the BytesIO object as the filehandle.
writer = pd.ExcelWriter(output, engine='xlsxwriter')

def write_excel(df, row,col,headertext):

     # Write the data frame to the BytesIO object.
    df.to_excel(writer, startrow=row, startcol=col,index=False)
    text = headertext
    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']
    worksheet.write(row-1, col, text)

frames = { 'transactions_tbl_sdf_Pdf':transactions_tbl_sdf_Pdf}

for sheet, frame in  frames.items():    
    frame.to_excel(writer, sheet_name = sheet)

    writer.close()

xlsx_data = output.getvalue()



%python

from datetime import date, datetime, timedelta
import datetime

filename = datetime.datetime.now().strftime("%H:%M:%S")
 # filename = '12122023'
monthPpy = datetime.datetime.strptime(start_date_m, "%Y-%m-%d").strftime("%b")


 # Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(container=container_name, blob='Recommendation.xlsx')

 # Upload the created file
blob_client.upload_blob(xlsx_data, overwrite=True)






================================== Writing to the Blob  ======================================================

sdf_register(Final_V1, 'Final_V1_SDF')

query <- "SELECT viewerId, asset, deviceos, country, startdate FROM Final_V1_SDF "

result <- sql(query)

display(result)



library(AzureStor)
library(dplyr)

# Ensure the 'result' dataframe contains the 'country' column
if (!"country" %in% colnames(result)) {
  stop("The 'result' dataframe does not contain a 'country' column.")
}

bl_endp_key2 <- storage_endpoint(
  "https://mlblobman01.blob.core.windows.net/",
  key = dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01")
)

cont <- storage_container(bl_endp_key2, "test2")

# Get distinct countries
Countries <- sql("SELECT DISTINCT country FROM Final_V1_SDF where country in ('qatar')") %>% collect(.) %>% as.character(.)


# Countries = c('united arab emirates','qatar')

# Write each subset to a separate CSV file in the blob storage
for (country in Countries) {
  subset_query <- paste0("SELECT * FROM Final_V1_SDF WHERE country = '", country, "'")
  subset_data <- sql(subset_query)
  subset_data_r <- as.data.frame(subset_data)
  storage_write_csv(subset_data_r, cont, paste0("AssetIDMappingTable_", country, "_Dec2024.csv"))
}




#=========================== Reading using SQL Editor in Databricks =========================

----select * from hive_metastore.default.f1_asset_id_mapping_table_v2

select * from hive_metastore.default.final_overlapping_sport_ent_2025qtr limit 10;

#------- shows table 

SHOW TABLES IN hive_metastore.default LIKE 'Final_Overlapping_Sport_Ent_2025';















#========================== Converting R dataframe into Spark data frame and writing and reading to Databricks catalog ============================
# ---------------- Converting to Sparklyr Dataframe - First menthod a----------------------


# ------------- Total Subscription , Unique subscption ID, Total Type of product purchased  ----------------


Summary_on_Subscription = Analysis_Order_Subscp_Datewise %>% 
dplyr::select(.,Customer_External_ID,Customer_ID,Country,Product_name,Subscription_ID,Subscription_start_date,Expiry_date,SubsRank,MonthP,YearP) %>% 
dplyr::mutate(.,Product_name = dplyr::if_else(grepl('TOD 4k|TOD 4K',Product_name),'TOD 4K',Product_name))%>%
dplyr::group_by(.,Customer_External_ID,Product_name)%>%
dplyr::summarise(.,TotalUniqueSubscription=dplyr::n_distinct(Subscription_ID),.groups='drop')%>%
sdf_collect()%>%
reshape2::dcast(.,Customer_External_ID~Product_name,value.var='TotalUniqueSubscription')%>%
replace(is.na(.),0)%>%
dplyr::group_by(.,Customer_External_ID)%>%
dplyr::mutate(., TotalSubscriptionPerSubscriber = rowSums(dplyr::across(is.numeric)), 
Percentage_4K_Total = round((`TOD 4K`/TotalSubscriptionPerSubscriber)*100,2))

display(Summary_on_Subscription)


# =============== Renaming the column dynamically =======================

# -------------- Converting R dataframe into 

Summary_on_Subscription_DF2 = as.DataFrame(Summary_on_Subscription)

# Rename columns with invalid characters (e.g., spaces)
col_names <- colnames(Summary_on_Subscription_DF2)
new_col_names <- gsub(" ", "_", col_names)
for (i in seq_along(col_names)) {
  Summary_on_Subscription_DF2 <- SparkR::withColumnRenamed(
    Summary_on_Subscription_DF2,
    col_names[i],
    new_col_names[i]
  )
}


# ---------------- Converting to Sparklyr Dataframe - Second menthod a----------------------

library(SparkR)

createOrReplaceTempView(Summary_on_Subscription_DF2, "Summary_on_Subscription_DF2")


#============================== Converting dataframe 
#---------- below sample example ---------------------
#================= Total Minutes Viewing ================================

viewer_table_m_tbl_V3 = viewer_table_m_tbl_V34


TotalMinutesViewed = viewer_table_m_tbl_V3%>%
                           dplyr::select(viewerId,MonthPart_DP,playingtimems_AggregateToMins)%>%
                            dplyr::group_by(.,MonthPart_DP)%>%
                            dplyr::summarise(.,TotalMinutesViewed = sum(playingtimems_AggregateToMins),.groups='drop')

                            


TotalMinutesViewed_tbl = dplyr::copy_to(sc,TotalMinutesViewed,'TotalMinutesViewed')

sdf_register(TotalMinutesViewed_tbl,'TotalMinutesViewed_tbl')

=============================== Dcast ===========================

VideoId_table_sdf = storage_read_csv(cont, "Final_Sports_AssetName4_2Final.csv")

VideoId_table_df =   data.frame(VideoId_table_sdf)%>%
           dplyr::distinct()%>% dplyr::group_by(.,asset_id)%>%dplyr::mutate(.,ID=1,Rank = cumsum(ID))%>% ungroup() %>% dplyr::select(.,-ID)

VideoId_table_df2 = VideoId_table_df %>% 
				    reshape2::dcast(.,asset_id+AssetNameEng+SportsType~Rank,,value.var='Competition')%>%
					replace(is.na(.),0)%>%
					dplyr::rename(.,CompetitionName1 = `1`,CompetitionName2 = `2`,CompetitionName3 = `3`,CompetitionName4 = `4`)


#------------ second version of dcast

#head(Final_Data_Merging_LeagueName,20)

Final_Data_Merging_LeagueName_V1 = Final_Data_Merging_LeagueName%>%dplyr::select(.,viewerId,TypeLeague,playingtimems_AggregateToMins)%>%
                                                                   dplyr::group_by(.,viewerId,TypeLeague)%>%
                                                                   dplyr::summarise(.,TotalPlayingTime = sum(playingtimems_AggregateToMins,na.rm=TRUE),.groups='drop')%>%
                                                                   reshape2::dcast(.,viewerId~TypeLeague,value.var='TotalPlayingTime',fun.aggregate = sum, na.rm = TRUE )

========================= Reading from Databricks delta table ====================

Final_F1_Mapping_Table_SDF = SparkR::read.df("/user/hive/warehouse/f1_asset_id_mapping_table_v2", source = "delta")

createOrReplaceTempView(Final_F1_Mapping_Table_SDF, "Final_F1_Mapping_Table_SDF")

Final_F1_Mapping_Table_DF = sdf_sql(sc,"select * from Final_F1_Mapping_Table_SDF") %>% sdf_collect() %>%
                              dplyr::arrange(.,Date,STdate,Grand_Prix)%>%
                            dplyr::group_by(.,asset_id)%>% dplyr::mutate(.,ID=1, RnkCum = cumsum(ID))%>%
                            dplyr::filter(.,RnkCum==1)%>%
                            dplyr::distinct()%>%
                            dplyr::select(.,-ID,-RnkCum)#-
							



============== Sparklyr Cacheing ===================


---------------- Example 1 -------------------

library(sparklyr)
library(dplyr)

# Assuming you have a Spark DataFrame named 'df'
df <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)

# Cache the table
tbl_cache(sc, "iris_tbl")


------------------- Real scenario ---------------------




Final_AssetName_AssetID = dplyr::inner_join(AllCompetition_SD_df_Final22,FinalCompetitionNames,by=c('AssetNameC1'),copy=TRUE)%>%
                          dplyr::mutate(.,MonthP =  monthp)%>%
                          dplyr::rename(.,AssetName=AssetNameC1,CompetitionName=`1`)%>%
                          dplyr::select(.,asset_id,AssetName,CompetitionName,MonthP)

Final_AssetName_AssetID_DF = dplyr::copy_to(sc,Final_AssetName_AssetID,'Final_AssetName_AssetID', overwrite = TRUE)

sdf_register(Final_AssetName_AssetID_DF,'Final_AssetName_AssetID_DF')

# Cache the table
tbl_cache(sc, "Final_AssetName_AssetID_DF")




#----------------- Scenario 2 -----------------------------------

DIM_SubscribersInfo_RDF = sdf_sql(sc,'select * from DIM_SubscribersInfo_SD where Dir_indir in ("Direct") ')

sdf_register(DIM_SubscribersInfo_RDF,'DIM_SubscribersInfo_SDF2')


tbl_cache(sc, 'DIM_SubscribersInfo_SDF2', force = TRUE)


DIM_SubscribersInfo_cache  <- tbl(sc,"DIM_SubscribersInfo_SDF2")




# ---------------- Converting to Sparklyr Dataframe  and writing to Databricks Catalog ----------------------

library(SparkR)

createOrReplaceTempView(Summary_on_Subscription_DF2, "Summary_on_Subscription_DF2")

Summary_CustomerLevel_Info_on_Subscription_DF2_SDF = sdf_sql(sc,'select * from Summary_on_Subscription_DF2')


#---------------------- writing sparklyr dataframe directly to Databrick catalog ------------------------

sparklyr::spark_write_table(Summary_CustomerLevel_Info_on_Subscription_DF2_SDF,name='Summary_CustomerLevel_Info_on_Subscription_DF2_SDF',mode='overwrite')

#----------- writing to dbms ----------


 path = paste("/user/hive/warehouse/Summary_on_Subscription_DF2",lubridate::date(Sys.time()),sep='_')

 SparkR::write.df(Summary_on_Subscription_DF2,path, source = "delta",mode="overwrite")


# ----------------- Sparklyr way writing to dbms -----------------

path = paste("/user/hive/warehouse/Summary_on_Subscription_Upgrade_Downgrade_4K")

 spark_write_delta(Summary_on_Subscription,path,mode="overwrite")





# # ---------------- Converting to Sparklyr Dataframe ----------------------

# library(SparkR)

# createOrReplaceTempView(Summary_on_Subscription_DF2, "Summary_on_Subscription_DF2")

# Summary_CustomerLevel_Info_on_Subscription_DF2_SDF = sdf_sql(sc,'select * from Summary_on_Subscription_DF2')

# sparklyr::spark_write_table(Summary_CustomerLevel_Info_on_Subscription_DF2_SDF,name='Summary_CustomerLevel_Info_on_Subscription_DF2_SDF',mode='overwrite')

Summary_CustomerLevel_Info_on_Subscription_DF2_SDF = SparkR::read.df("/user/hive/warehouse/summary_customerlevel_info_on_subscription_df2_sdf", source = "delta")









#================================= Parquet file =======================================

The most effective way to write a Parquet file to Azure Blob Storage using R is by utilizing the **`arrow`** package, which has direct support for both the Parquet format and cloud storage.

Here is a step-by-step guide:

-----

## 1\. Prerequisites (Install and Load Packages)

You'll need the `arrow` package for reading/writing Parquet and the `AzureStor` package for generating the necessary Azure credentials, if you are not relying solely on environment variables.

```r
# Install packages if you haven't already
# install.packages(c("arrow", "AzureStor", "dplyr"))

# Load necessary libraries
library(arrow)
library(AzureStor)
library(dplyr)
```

-----

## 2\. Authenticate and Connect to Azure Blob

To securely access your Azure Blob Storage, you typically use a **Shared Access Signature (SAS) token** or a **Storage Account Key**. The `AzureStor` package is helpful for managing this connection.

### Option A: Using a SAS Token (Recommended)

A SAS token grants time-limited access to your storage container.

```r
# 1. Define your storage container URL and SAS token
container_url <- "https://yourstorageaccount.blob.core.windows.net/yourcontainer"
sas_token <- "?sv=2023-01-01&ss=b&srt=sco&sp=..." # Your actual SAS token

# 2. Authenticate and create an Azure Storage endpoint object
bl_endp <- blob_endpoint(container_url, key=sas_token)

# 3. Create a blob container object (the destination)
cont <- blob_container(bl_endp, "yourcontainer")
```


#----------------------- Ideal ---------------------------------

### Option B: Using the Storage Account Key

```r
# 1. Define your storage account and key
storage_account <- "yourstorageaccount"
storage_key <- "your_storage_account_key" # The full account key

# 2. Create a blob container object
cont_url <- paste0("https://", storage_account, ".blob.core.windows.net/yourcontainer")
cont <- blob_container(cont_url, key=storage_key)
```

-----

## 3\. Write Parquet File to Azure Blob

Once the connection is established, use the `write_parquet()` function from the `arrow` package, passing the Azure Blob URL as the destination path.

### Step 3.1: Prepare the Data

Create a sample data frame to write.

```r
# Create a sample data frame
my_data <- data.frame(
  ID = 1:5,
  Name = c("Alice", "Bob", "Charlie", "David", "Eve"),
  Value = runif(5, 10, 100),
  Date = as.Date(Sys.Date() - 4:0)
)
```

### Step 3.2: Define the Destination Path

The `write_parquet()` function requires a full URL path, which includes the connection details (the container object `cont`).

```r
# Define the path *within* the container
blob_path <- "data/output_file.parquet"

# Create the final destination path using the container object
destination_uri <- file.path(cont, blob_path) 
```

### Step 3.3: Write the Parquet File

Use the `write_parquet()` function, providing the data and the full destination URI. The `arrow` package is smart enough to use the credentials you set up via the `AzureStor` package.

```r
# Write the data frame to the Azure Blob as a Parquet file
write_parquet(my_data, destination_uri)

cat(paste0("Successfully wrote Parquet file to Azure Blob at: ", destination_uri, "\n"))
```

### Alternative: Writing a Partitioned Dataset

For very large datasets, you can write a partitioned dataset using `write_dataset()`. This creates multiple Parquet files (one per partition folder), which is optimal for analytics.

```r
# Create a larger, partitionable data frame
large_data <- data.frame(
  Country = rep(c("USA", "CAN", "MEX"), each = 100),
  City = paste0("City_", 1:300),
  Sales = rnorm(300, 500, 50)
)

# Define the destination folder for the partitioned dataset
partitioned_folder_uri <- file.path(cont, "data/sales_partitions")

# Write the partitioned dataset, using 'Country' as the partition key
# This will create folders like "data/sales_partitions/Country=USA/" etc.
write_dataset(
  dataset = large_data,
  path = partitioned_folder_uri,
  format = "parquet",
  partitioning = "Country"
)

cat(paste0("Successfully wrote partitioned Parquet files to Azure Blob at: ", partitioned_folder_uri, "\n"))
```



#=================================== Actual Scenario ============================================================


# Databricks notebook source
# Load necessary libraries
library(arrow)
library(AzureStor)
library(dplyr)

# COMMAND ----------

library(AzureStor)#survivalanalysis

bl_endp_key2 <- storage_endpoint("https://mlblobman01.blob.core.windows.net/", key= dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01"))

list_storage_containers(bl_endp_key2)

# COMMAND ----------

# 1. Define your storage account and key
storage_account <- "mlblobman01"

# 2. Create a blob container object
cont_url <- "https://mlblobman01.blob.core.windows.net/test2"
cont <- blob_container(cont_url, key= dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01"))

list_blobs(cont)

# COMMAND ----------

 %python
 dbutils.fs.mount(
   source = "wasbs://test2@mlblobman01.blob.core.windows.net/",
   mount_point = "/mnt/test2",
   extra_configs = {
     "fs.azure.account.key.mlblobman01.blob.core.windows.net": dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01")
   }
 )



# COMMAND ----------

 
dbutils.fs.ls("/mnt/test2")

# COMMAND ----------

# COMMAND ----------

library(sparklyr)
library(dplyr)

# Connect to the Databricks cluster
sc <- spark_connect(method = "databricks")

# Create example Spark DataFrame
df_spark <- copy_to(sc, iris, "iris_tbl", overwrite = TRUE) 

# Write Parquet directly to Azure Blob (mounted container)
spark_write_parquet(
  df_spark,
  path = "dbfs:/mnt/test2/parquet/iris2",
  mode = "overwrite"
)




# COMMAND ----------

Df = # Write Parquet directly to Azure Blob (mounted container)
sparklyr::spark_read_parquet(
  sc,
  path = "dbfs:/mnt/test2/parquet/iris2"
)

# COMMAND ----------

display(Df)

# COMMAND ----------



# COMMAND ----------

 %python

 # ==== Creating mountpoint to access blob storage ====
 # ---- parquettest container name --------
 # ----- /mnt/parquettest ---- mnt is mount point 

 dbutils.fs.mount(
   source = "wasbs://parquettest@mlblobman01.blob.core.windows.net/",
   mount_point = "/mnt/parquettest",
   extra_configs = {
     "fs.azure.account.key.mlblobman01.blob.core.windows.net": dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01")
   }
 )


# COMMAND ----------

dbutils.fs.ls("/mnt/parquettest")

# COMMAND ----------

# ============== Second test for writing parquet file ===========

# COMMAND ----------

library(sparklyr)
library(dplyr)

# Connect to the Databricks cluster
sc <- spark_connect(method = "databricks")

# Create example Spark DataFrame
df_spark <- copy_to(sc, iris, "iris_tbl", overwrite = TRUE) 

# Write Parquet directly to Azure Blob (mounted container)
spark_write_parquet(
  df_spark,
  path = "dbfs:/mnt/parquettest/parquet/iris2",
  mode = "overwrite"
)





# COMMAND ----------

# Write CSV directly to Azure Blob (mounted container)

# Create example Spark DataFrame
df_spark_csv <- copy_to(sc, iris, "iris_tbl", overwrite = TRUE) 

df_single <- sdf_coalesce(df_spark_csv, 1)

sparklyr::spark_write_csv(
  df_single,
  path = "dbfs:/mnt/test2/CSV/iriscsv",
  mode = "overwrite"
)

# COMMAND ----------







