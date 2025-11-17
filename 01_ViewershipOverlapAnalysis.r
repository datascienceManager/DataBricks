# Databricks notebook source
# MAGIC %python
# MAGIC
# MAGIC
# MAGIC import calendar
# MAGIC import numpy as np
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.functions import *
# MAGIC import pyspark
# MAGIC from pyspark.sql.functions import countDistinct, sum, col, when, min, broadcast
# MAGIC import datetime
# MAGIC from pyspark.sql.types import StringType
# MAGIC import pandas as pd
# MAGIC from azure.core.credentials import AzureNamedKeyCredential
# MAGIC from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# MAGIC import xlsxwriter
# MAGIC import io
# MAGIC
# MAGIC jdbcHostname = "beflixdwh-01.sql.azuresynapse.net" 
# MAGIC jdbcPort = 1433 
# MAGIC jdbcDatabase = "DWH" 
# MAGIC
# MAGIC # SQLAccessKey7
# MAGIC
# MAGIC jdbcUsername = dbutils.secrets.get(scope = "SQLAccessKey7", key = "UsName1")  #SQLAccessKey  #SqlKeys_Qatar_Databricks
# MAGIC jdbcPassword = dbutils.secrets.get(scope = "SQLAccessKey7", key = "PssName2")
# MAGIC # jdbcUsername = dbutils.secrets.get(scope = "SQLAccessKey", key = "UsName")  #SQLAccessKey  #SqlKeys_Qatar_Databricks
# MAGIC # jdbcPassword = dbutils.secrets.get(scope = "SQLAccessKey", key = "PssName")
# MAGIC
# MAGIC # Create JDBC URL
# MAGIC jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
# MAGIC
# MAGIC # connectionProperties = {
# MAGIC #   "user" : jdbcUsername,
# MAGIC #   "password" : jdbcPassword,
# MAGIC #   "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC # }
# MAGIC
# MAGIC
# MAGIC
# MAGIC import datetime
# MAGIC
# MAGIC def get_month_dates(month_name, year):
# MAGIC     """
# MAGIC     Returns the start and end dates for a given month and year.
# MAGIC
# MAGIC     Args:
# MAGIC         month_name: The name of the month (e.g., "March").
# MAGIC         year: The year (e.g., 2025).
# MAGIC
# MAGIC     Returns:
# MAGIC         A dictionary containing the start and end dates in "dd-mm-yyyy" format.
# MAGIC     """
# MAGIC     month_num = datetime.datetime.strptime(month_name, "%B").month
# MAGIC     start_date = datetime.date(year, month_num, 1)
# MAGIC
# MAGIC     if month_num == 12:
# MAGIC         end_date = datetime.date(year, month_num, 31)
# MAGIC     else:
# MAGIC         end_date = datetime.date(year, month_num + 1, 1) - datetime.timedelta(days=1)
# MAGIC
# MAGIC     return {
# MAGIC         "StartDate": start_date.strftime("%Y-%m-%d"),
# MAGIC         "EndDate": end_date.strftime("%Y-%m-%d"),
# MAGIC     }
# MAGIC
# MAGIC # Example usage:
# MAGIC # result = get_month_dates("February", 2025)
# MAGIC
# MAGIC
# MAGIC # -------- Kindly input the month name and year
# MAGIC # MonthP = "March"
# MAGIC # YearP = 2025
# MAGIC
# MAGIC # ---------- Below is the function ----------------
# MAGIC
# MAGIC StartMonth = get_month_dates("January", 2025)
# MAGIC EndMonth = get_month_dates("March", 2025)
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# YearPart = paste0('January',"-",'2024')

YearPart = paste0('2025')

print(YearPart)

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # ----------------- For range of the dates if we are looking for any particular F1 Championship ---------
# MAGIC
# MAGIC start_date_m=StartMonth["StartDate"]
# MAGIC end_date_m = EndMonth["EndDate"]
# MAGIC
# MAGIC EntertainmentMovies = f"""select * ,case 
# MAGIC         when sports=1 and sportname in('Football','Soccer') then 'Football'
# MAGIC         when sports=1 and sportname not in ('Football','Soccer','NULL','Uncategorized') then 'Non-Football'
# MAGIC         when sports=1 and sportname in ('Uncategorized') then 'Uncategorized'
# MAGIC         when sports=1 and sportname is null then 'NULL'
# MAGIC         when sports=0  then 'Entertainment' 
# MAGIC         else 'Unrecognised' 
# MAGIC     end as ContentType2  from  [dbviews].[ViewerSessions]  (nolock) where country in (
# MAGIC         'algeria','bahrain', 'egypt', 'iraq', 'jordan', 'kuwait', 'lebanon', 
# MAGIC         'libya', 'morocco', 'oman', 'qatar', 'saudi arabia', 'syria', 'tunisia', 
# MAGIC         'united arab emirates', 'yemen', 'occupied palestinian territory', 
# MAGIC         'south sudan', 'mauritania', 'sudan', 'somalia', 'chad'
# MAGIC     )  and     startdate >= '{start_date_m}' 
# MAGIC     and startdate <= '{end_date_m}' 
# MAGIC     and viewerid not like '%.%'  """
# MAGIC
# MAGIC
# MAGIC EntertainmentMovies_SD = (spark.read
# MAGIC   .format("jdbc")
# MAGIC   .option("url", jdbcUrl)
# MAGIC #   .option("dbtable","[dbviews].[ViewerSessions]" )
# MAGIC   .option("user",jdbcUsername)
# MAGIC   .option("password",  jdbcPassword)
# MAGIC   .option("query", EntertainmentMovies) 
# MAGIC   .load()
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC BothSportsEnt = f"""
# MAGIC select 
# MAGIC     viewerId,
# MAGIC     startdate,
# MAGIC     asset_id,
# MAGIC     asset,
# MAGIC     asset1,
# MAGIC     competitionName,
# MAGIC     sports,
# MAGIC     convivasessionid,
# MAGIC     month(startdate) as Month_,
# MAGIC     year(startdate) as Year_,
# MAGIC     sum(CAST(playingunixtimems AS float) / 60000) as total_mins,
# MAGIC     case 
# MAGIC         when sports=1 and sportname in('Football','Soccer') then 'Football'
# MAGIC         when sports=1 and sportname not in ('Football','Soccer','NULL','Uncategorized') then 'Non-Football'
# MAGIC         when sports=1 and sportname in ('Uncategorized') then 'Uncategorized'
# MAGIC         when sports=1 and sportname is null then 'NULL'
# MAGIC         when sports=0  then 'Entertainment' 
# MAGIC         else 'Unrecognised' 
# MAGIC     end as ContentType 
# MAGIC from [dbviews].[ViewerSessions] 
# MAGIC where 
# MAGIC     startdate >= '{start_date_m}' 
# MAGIC     and startdate <= '{end_date_m}' 
# MAGIC     and viewerid not like '%.%' 
# MAGIC     and country in (
# MAGIC         'algeria','bahrain', 'egypt', 'iraq', 'jordan', 'kuwait', 'lebanon', 
# MAGIC         'libya', 'morocco', 'oman', 'qatar', 'saudi arabia', 'syria', 'tunisia', 
# MAGIC         'united arab emirates', 'yemen', 'occupied palestinian territory', 
# MAGIC         'south sudan', 'mauritania', 'sudan', 'somalia', 'chad'
# MAGIC     )  
# MAGIC group by 
# MAGIC     viewerId, asset_id,asset, asset1, competitionName, startdate, playingunixtimems, 
# MAGIC     convivasessionid, month(startdate), year(startdate), sports,
# MAGIC     case 
# MAGIC         when sports=1 and sportname in('Football','Soccer') then 'Football'
# MAGIC         when sports=1 and sportname not in ('Football','Soccer','NULL','Uncategorized') then 'Non-Football'
# MAGIC         when sports=1 and sportname in ('Uncategorized') then 'Uncategorized'
# MAGIC         when sports=1 and sportname is null then 'NULL'
# MAGIC         when sports=0  then 'Entertainment' 
# MAGIC         else 'Unrecognised'  
# MAGIC     end
# MAGIC """
# MAGIC BothSportsEntSD = (spark.read
# MAGIC                  .format("jdbc")
# MAGIC                  .option("url", jdbcUrl)
# MAGIC #   .option("dbtable","[dbviews].[ViewerSessions]" )
# MAGIC                 .option("user",jdbcUsername)
# MAGIC                 .option("password",  jdbcPassword)
# MAGIC                 .option("query", BothSportsEnt) 
# MAGIC                 .load()
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
# MAGIC ContentList = f"select * from [dbviews].[TodContent] (nolock)"
# MAGIC
# MAGIC ContentList_SD = (spark.read
# MAGIC   .format("jdbc")
# MAGIC   .option("url", jdbcUrl)
# MAGIC #   .option("dbtable","[dbviews].[ViewerSessions]" )
# MAGIC   .option("user",jdbcUsername)
# MAGIC   .option("password",  jdbcPassword)
# MAGIC   .option("query", ContentList) 
# MAGIC   .load()
# MAGIC )
# MAGIC
# MAGIC
# MAGIC EntertainmentSession= f"select * from [DIM].[EntertainmentSession](nolock)"
# MAGIC
# MAGIC EntertainmentSession_SD = (spark.read
# MAGIC   .format("jdbc")
# MAGIC   .option("url", jdbcUrl)
# MAGIC #   .option("dbtable","[dbviews].[ViewerSessions]" )
# MAGIC   .option("user",jdbcUsername)
# MAGIC   .option("password",  jdbcPassword)
# MAGIC   .option("query", EntertainmentSession) 
# MAGIC   .load()
# MAGIC )
# MAGIC
# MAGIC EntertainmentMovies_SD.createOrReplaceTempView("EntertainmentMovies_SD")# AllCompetition_SD
# MAGIC BothSportsEntSD.createOrReplaceTempView("BothSportsEntSD")# AllCompetition_SD
# MAGIC ContentList_SD.createOrReplaceTempView("ContentList_SD")# AllCompetition_SD
# MAGIC EntertainmentSession_SD.createOrReplaceTempView("EntertainmentSession_SD")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC
# MAGIC # display(start_date_m)
# MAGIC display(end_date_m)

# COMMAND ----------

library(data.table)
library(dplyr)
library(lubridate)
library(sparklyr)
library(SparkR)
library(AzureStor)
library(arrow)
library(stringr)


# COMMAND ----------

sc <- spark_connect(method = "databricks")

# COMMAND ----------


# ************************* Above Temporary is Enabled then disable below piece of code , if incase ***************************
#=================== Viewer session table =====================================



EntertainmentMovies_SDF=sdf_sql(sc,"select viewerId,startdate,convivasessionid,sportname,asset_id,asset,assetname,country,ContentType2,`playingtimemin`,((playingunixtimems) / 60000) as playingtimems_AggregateToMins_V2 from EntertainmentMovies_SD ")#%>%
# dplyr::filter(.,viewerId %in% c('fca04b6b-3adb-4257-8cf1-713547f78dc2','d128cde6-17da-40a9-be89-b2bb1f976af7','003c1bbd-e2f2-487f-afda-1a206c69f736','003e7484-99fc-4783-b6a6-18739e5d26e6','03797fc3-c6bd-4a36-b81c-bcdd18b27a57','09c6eded-9aab-4890-9178-81f1fc5936f2'))


# display(EntertainmentMovies_SDF)

# COMMAND ----------

# ===================== Final part of merging all ===============

pattern_Pro = c('Trailer|Trailer|trailer')


Final_V1 = EntertainmentMovies_SDF%>%
         dplyr::filter(.,!asset %rlike% pattern_Pro)%>% dplyr::mutate(., MissingAssetId = dplyr::if_else(asset_id==''|is.na(asset_id)|is.null(asset_id)|asset_id=='null',1,0))%>%
                    dplyr::filter(.,MissingAssetId ==0)

# COMMAND ----------

#========================= Calculating only on the Quality Session ==========================

Final_V1_2 = Final_V1 %>%  
 dplyr::group_by(.,viewerId,startdate,convivasessionid,ContentType2) %>% 
 dplyr::summarise(.,playingtimems_AggregateToMins1 = sum(playingtimems_AggregateToMins_V2),.groups='drop')%>%
  dplyr::filter(.,playingtimems_AggregateToMins1 >=1)%>%
                    dplyr::mutate(.,QualityFlag = 1)%>%
                    dplyr::select(.,viewerId,startdate,convivasessionid,ContentType2,playingtimems_AggregateToMins1,QualityFlag)



# COMMAND ----------

# ====================== Country ===========================


ViewerCountryFreq = Final_V1 %>% 
                    dplyr::select(.,viewerId,country)%>% 
                    dplyr::mutate(country = toupper(country)) %>%
                    dplyr::group_by(.,viewerId,country) %>%
                    dplyr::summarise(.,TotalNumCountry = dplyr::n(),.groups='drop')%>%
                    dplyr::group_by(.,viewerId)%>%
                    dplyr::mutate(.,CountryMax = max(TotalNumCountry),Flag= dplyr::if_else(TotalNumCountry==CountryMax,1,0))%>%
                    dplyr::filter(.,Flag==1)


ViewerCountryFreq_v2 = ViewerCountryFreq %>% dplyr::select(.,viewerId,country)


# display(ViewerCountryFreq_v2)

# COMMAND ----------

# display(ViewerCountryFreq_v2%>%dplyr::group_by(.,viewerId)%>%dplyr::summarise(.,TotalCNT = dplyr::n()))

# COMMAND ----------



# COMMAND ----------

# === Need to start from here

# COMMAND ----------


# =============== 1 : Extracting only Quality Viewers============

Final_V2 = Final_V1_2%>%
           dplyr::mutate(.,MonthP = lubridate::month(startdate),YearP = lubridate::year(startdate),Flag=1
                                           )%>%
           dplyr::select(.,viewerId,MonthP,YearP,ContentType2,Flag)%>%
           sdf_distinct()


# display(Final_V2)

# COMMAND ----------



# COMMAND ----------

# ================= Writing to Delta Table =====================

# path = paste("Final_Overlapping_Sport_Ent_V2",YearPart,sep='_')

# sparklyr::spark_write_table(Final_V2,name = path,mode = 'overwrite')

# COMMAND ----------





# COMMAND ----------

# Final_V2_DF = Final_V2 %>% as.data.frame()

# storage_write_csv(Final_V2_DF, cont, "Final_V2_2024.csv")

# COMMAND ----------

# =============== 3 : Final ViewerID Country ============

Final_V2_1 = Final_V1_2%>%
           dplyr::mutate(.,MonthP = lubridate::month(startdate),YearP = lubridate::year(startdate),Flag=1)%>%
           dplyr::select(.,viewerId,ContentType2,Flag)%>%
           sdf_distinct()%>%
           sdf_collect()

Final_V4 = Final_V2_1 %>% 
          reshape2::dcast(.,viewerId~ContentType2,value.var='Flag')%>%
          replace(is.na(.),0)%>%
          dplyr::left_join(.,ViewerCountryFreq_v2,by=c('viewerId'),copy=TRUE)


#------------- Archive---------------

# Final_V3 = dplyr::inner_join(Final_V1,NonQualitySession,by=c('convivasessionid'),copy=TRUE)%>%
#            dplyr::select(.,viewerId_x,startdate,ContentType2,QualityFlag)%>%
#            dplyr::rename(.,Flag = QualityFlag)%>%
#            dplyr::left_join(.,ViewerCountryFreq_v2,by=c('viewerId_x'='viewerId'),copy='TRUE')%>%
#            dplyr::mutate(.,MonthP = lubridate::month(startdate))%>%
#            dplyr::select(.,viewerId,country,ContentType2,Flag)%>%
#            dplyr::rename(.,viewerId = viewerId_x)%>%
#            sdf_distinct()%>%
#            sdf_collect()
# display(Final_V4)

# COMMAND ----------

Final_V4_1 = Final_V4 %>%dplyr::mutate(.,ID=1)%>% dplyr::group_by(.,viewerId)%>%dplyr::mutate(.,FlagNumRep = cumsum(ID))%>%
dplyr::arrange(.,desc(FlagNumRep),viewerId)  %>% dplyr::filter(., ID==FlagNumRep)%>%dplyr::select(.,viewerId,country,Entertainment,Football,`Non-Football`)

display(Final_V4_1)

# COMMAND ----------

# ================= Reading 

library(SparkR)
# Get the existing SparkSession that is already initiated on the cluster.
sparkR.session()

# ---------------- Second processing as on 20th May 2024 , to read from delta table

# Final_V4_1 = SparkR::read.df("/user/hive/warehouse/final_overlapping_sport_ent_v3_2024_qtr", source = "delta")

# createOrReplaceTempView(Final_V4_1, "Final_V4_1")


# Final_V4_1 = sdf_sql(sc,"select * from Final_V4_1")

# COMMAND ----------

# display(Final_V4_1)

# COMMAND ----------

# Final_V4_1 = Final_V4_1 %>% sdf_collect()

# COMMAND ----------

Final_V4_2 = Final_V4_1 %>% 
dplyr::mutate(.,Football = dplyr::if_else((Entertainment==0 & Football==0 &`Non-Football`==0),1,Football)) %>%                                            
dplyr::mutate(.,Description = dplyr::case_when((Entertainment==1 & Football==0 & `Non-Football`==0)~'Entertainment‑only-Viewers',
                                                 (Entertainment==1 & Football==1 & `Non-Football`==0)~'Football&Entertainment(not Oth.)',
                                                 (Entertainment==1 & Football==1 & `Non-Football`==1)~'All three types',
                                                 (Entertainment==0 & Football==1 & `Non-Football`==0)~'Football‑only-Viewers',
                                                 (Entertainment==0 & Football==1 & `Non-Football`==1)~'Football & Other Sports (not Ent.)',
                                                 (Entertainment==0 & Football==0 & `Non-Football`==1)~'Other‑Sports‑only viewers',
                                                 (Entertainment==1 & Football==0 & `Non-Football`==1)~'Other‑Sports&Entertainment(not Football)'))


display(Final_V4_2)

# COMMAND ----------

# ================= Writing to Delta Table =====================

Final_Overlapping_Sport_Ent = SparkR::createDataFrame(Final_V4_2)

# SparkR::saveAsTable(df= Final_Overlapping_Sport_Ent , tableName="Final_Overlapping_Sport_Ent_2025Qtr", mode="overwrite" )

path = paste("Final_Overlapping_Sport_Ent_V4",YearPart,"Qtr",sep='_')

SparkR::saveAsTable(df= Final_Overlapping_Sport_Ent , tableName=path, mode="overwrite" )

# COMMAND ----------

bl_endp_key2 <- storage_endpoint("https://mlblobman01.blob.core.windows.net/", key= dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01"))

list_storage_containers(bl_endp_key2)

cont <- storage_container(bl_endp_key2, "overlappingsportent")

Final_Overlapping_Sport_Ent_DF = Final_V4_2 %>% as.data.frame()

path = paste("Final_Overlapping_Sport_Ent_V4",YearPart,"Qtr.csv",sep='_')

storage_write_csv(Final_Overlapping_Sport_Ent_DF, cont, path)

# COMMAND ----------



# COMMAND ----------

# =================== Summary =========================

Final_V4_2_Summary = Final_V4_2 %>%dplyr::group_by(.,Description) %>% dplyr::summarise(.,TotalCNT = dplyr::n())

display(Final_V4_2_Summary)

# COMMAND ----------

# Final_V2_Month_Qtr_Year = Final_V2 %>% dplyr::rename(.,viewerId=viewerId_x)%>%sdf_distinct()%>% sdf_collect() %>% reshape2::dcast(.,viewerId+MonthP+YearP+QuarterPart~ContentType2,value.var='Flag')%>%replace(is.na(.),0)



# COMMAND ----------





# display(AllCompetition_SD_PreviousData_v3)

# COMMAND ----------



# COMMAND ----------



# ============== Excluding Non Quality sessions ===============

# NonQualitySession = Final_V1 %>% 
#                     dplyr::select(.,viewerId,startdate,convivasessionid,ContentType2,playingtimems_AggregateToMins_V2)%>% 
#                     dplyr::group_by(.,viewerId,startdate,convivasessionid) %>%
#                     dplyr::summarise(.,TotalMins_v2 = round(sum(playingtimems_AggregateToMins_V2),2),.groups='drop')%>%
#                     dplyr::filter(.,TotalMins_v2>=1)%>%
#                     dplyr::mutate(.,QualityFlag = 1)%>%
#                     dplyr::select(.,viewerId,convivasessionid,QualityFlag)%>%
#                     sdf_distinct()



# display(NonQualitySession)