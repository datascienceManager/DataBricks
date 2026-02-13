# Databricks notebook source
# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.functions import countDistinct, sum, col, when, min, broadcast
# MAGIC import datetime
# MAGIC from pyspark.sql.types import StringType
# MAGIC import pandas as pd
# MAGIC from azure.core.credentials import AzureNamedKeyCredential
# MAGIC from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# MAGIC import xlsxwriter
# MAGIC import io
# MAGIC import calendar
# MAGIC import numpy as np
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.functions import *
# MAGIC import pyspark
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
# MAGIC
# MAGIC # start_date = get_month_dates("March", 2025)
# MAGIC # end_date = get_month_dates("March", 2025)
# MAGIC

# COMMAND ----------


library(data.table)
library(dplyr)
library(lubridate)
library(sparklyr)
library(SparkR)
library(AzureStor)
library(arrow)


# COMMAND ----------

# MAGIC %python
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
# MAGIC
# MAGIC
# MAGIC # connectionProperties = {
# MAGIC #   "user" : jdbcUsername,
# MAGIC #   "password" : jdbcPassword,
# MAGIC #   "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC # }

# COMMAND ----------

# MAGIC %python
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC start_date = get_month_dates("October", 2025)
# MAGIC end_date = get_month_dates("October", 2025)
# MAGIC
# MAGIC # ================ OR =========================================
# MAGIC
# MAGIC # start_date = '2025-08-01'
# MAGIC # end_date =  '2025-08-31'
# MAGIC
# MAGIC start_date_m=start_date["StartDate"]
# MAGIC end_date_m = end_date["EndDate"]
# MAGIC
# MAGIC # start_date_m=start_date["StartDate"]
# MAGIC # end_date_m = end_date["EndDate"]
# MAGIC
# MAGIC print(start_date_m)
# MAGIC print(end_date_m)
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC
# MAGIC
# MAGIC # ----------------- For range of the dates if we are looking for any particular F1 Championship ---------
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
# MAGIC     ) and (asset_id LIKE '%-%' OR asset_id NOT LIKE '%[^0-9]%')
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
# MAGIC BothSportsEnt = (spark.read
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
# MAGIC     and viewerid not like '%.%' and (asset_id LIKE '%-%' OR asset_id NOT LIKE '%[^0-9]%')  """
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
# MAGIC # ====================== Eligible Subscribers =========================================
# MAGIC
# MAGIC # /* active subs */
# MAGIC
# MAGIC StgpassReport_df = f""" select count(distinct Customer_External_ID) as distinct_customer_count from (select distinct Customer_External_ID, country, [Offer_title] from Cleeng.StgpassReport where cast(Pass_End_Date_Time as date) >= '2024-11-01' and cast(Pass_Start_Date_Time as date)  <= '2024-11-30' union All select distinct Customer_External_ID , country , [Offer_title] from Cleeng.StgSubscriptionReport where cast(Expiry_date as date) >= '2024-11-01' and cast(Subscription_Start_Date as date)  <= '2024-11-30') tb where Customer_External_ID IS NOT NULL """
# MAGIC
# MAGIC Only_Aggregated_StgpassReport_sdf = (spark.read
# MAGIC                  .format("jdbc")
# MAGIC                  .option("url", jdbcUrl)
# MAGIC #   .option("dbtable","[dbviews].[ViewerSessions]" )
# MAGIC                 .option("user",jdbcUsername)
# MAGIC                 .option("password",  jdbcPassword)
# MAGIC                 .option("query", StgpassReport_df) 
# MAGIC                 .load()
# MAGIC )
# MAGIC
# MAGIC
# MAGIC #================ /* active subs detailed */ ============================
# MAGIC
# MAGIC # Detail_StgpassReport_Subs =f""" 
# MAGIC # select distinct Customer_External_ID, Offer_title , [Pass_ID],[Subscription_ID] from (
# MAGIC # select distinct Customer_External_ID, country, [Offer_title] , [Pass_ID], null as Subscription_ID
# MAGIC # from Cleeng.StgpassReport
# MAGIC # where cast(Pass_End_Date_Time as date) >= '2024-11-01' and cast(Pass_Start_Date_Time as date)  <= '2024-11-30' /*and Customer_External_ID != NULL*/
# MAGIC  
# MAGIC # union all
# MAGIC  
# MAGIC # select distinct Customer_External_ID , country , [Offer_title],null as Pass_ID ,[Subscription_ID]
# MAGIC # from Cleeng.StgSubscriptionReport
# MAGIC # where cast(Expiry_date as date) >= '2024-11-01' and cast(Subscription_Start_Date as date)  <= '2024-11-30'
# MAGIC # ) tb  where Customer_External_ID IS NOT NULL"""
# MAGIC
# MAGIC # Detail_StgpassReport_Subs_sdf = (spark.read
# MAGIC #                  .format("jdbc")
# MAGIC #                  .option("url", jdbcUrl)
# MAGIC # #   .option("dbtable","[dbviews].[ViewerSessions]" )
# MAGIC #                 .option("user",jdbcUsername)
# MAGIC #                 .option("password",  jdbcPassword)
# MAGIC #                 .option("query", Detail_StgpassReport_Subs) 
# MAGIC #                 .load()
# MAGIC # )
# MAGIC
# MAGIC BothSportsEnt.createOrReplaceTempView("BothSportsEnt")
# MAGIC EntertainmentMovies_SD.createOrReplaceTempView("EntertainmentMovies_SD")
# MAGIC Only_Aggregated_StgpassReport_sdf.createOrReplaceTempView("Only_Aggregated_StgpassReport_sdf")
# MAGIC # Detail_StgpassReport_Subs_sdf.createOrReplaceTempView("Detail_StgpassReport_Subs_sdf")
# MAGIC

# COMMAND ----------

sc <- spark_connect(method = "databricks")

# COMMAND ----------



# ================== Overall Data ===============================

# dplyr::filter(.,!asset %rlike% pattern_Pro)

BothSportsEnt_SD_V1 = sdf_sql(sc,'select *  from BothSportsEnt')%>%
            dplyr::mutate(., CompetitionMainNames = dplyr::case_when(
                            grepl('2. Bundesliga|Bundesliga', competitionName) ~ "Bundesliga",
                            grepl('AFC Champions League|AFC Champions League Elite', competitionName) ~ "AFC Champions League",
                            grepl('AFC Champions League Two|AFC Cup', competitionName) ~ "AFC Champions League Two",
                            grepl('Carabao Cup|EFL Cup', competitionName) ~ "Carabao Cup",
                            grepl('La Liga|LaLiga|Primera División', competitionName) ~ "La Liga",
                            grepl('La Liga2|Segunda División', competitionName) ~ "La Liga2",
                            grepl('Super Lig|Süper Lig', competitionName) ~ "Super Lig",
                            grepl('UEFA  Nations League|UEFA Nations League', competitionName) ~ "UEFA Nations League",
                            TRUE ~ competitionName))%>%
                #     #------------------------ Removing unrelated assetId------------------------------
                    dplyr::mutate(., MissingAssetId = dplyr::if_else(asset_id==''|is.na(asset_id)|is.null(asset_id)|asset_id=='null',1,0))%>%
                    dplyr::filter(.,MissingAssetId ==0)%>%
                    #------------------------ Removing unwanted viewerId ------------------------------
                    dplyr::mutate(., MissingviewerId = dplyr::if_else(viewerId==''|is.na(viewerId)|is.null(viewerId)|
                    viewerId=='null',1,0))%>%
                    dplyr::filter(.,MissingviewerId ==0)



# display(BothSportsEnt_SD_V1)

# COMMAND ----------

# Delta table 

SportsAssetIdCompetition = SparkR::read.df("/user/hive/warehouse/final_assetname_assetid_december", source = "delta")

# AssetIDMappingTable = SparkR::distinct(AssetIDMappingTableV1 )

createOrReplaceTempView(SportsAssetIdCompetition, "SportsAssetIdCompetition")

# hive_metastore.default.assetidmappingtable_current
# display(AssetIDMappingTable)

SportsAssetIdCompetition = sdf_sql(sc,"select * from SportsAssetIdCompetition")%>% 
                            dplyr::rename(.,CompetitionNameC1 = `1`)%>% 
                            dplyr::select(.,-`2`)

display(SportsAssetIdCompetition)



# COMMAND ----------

# ====================== Total Minutes for Entertainment and Sports for 1+ minutes ======================
# viewerId,asset_id,asset,competitionName,startdate,playingunixtimems, convivasessionid,month  ,year ,Football, NonFootball 

# pattern_Pro = c('Trailer|trailer')

pattern_Pro = paste0("(?i)", c('Trailer|Trailer|trailer') )

BothSportsEnt_1Plus_TotalMinutes = BothSportsEnt_SD_V1 %>% 
                                  dplyr::filter(.,!asset1 %rlike% pattern_Pro) %>%
                                  #  dplyr::filter(., convivasessionid %in% c('1961919595:395311779:398255337:743670104:1344160389','990194192:2090548053:973175529:1584632764:490683205'))%>%
                                   dplyr::group_by(.,convivasessionid,viewerId)%>%
                                   dplyr::summarise(.,TotalM = sum(total_mins))%>%ungroup()%>%
                                   dplyr::filter(.,TotalM>=1)%>%
                                   dplyr::summarise(.,`TotalMinutes(In Mins)` = round(sum(TotalM),2),UV = dplyr::n_distinct(viewerId),.groups='drop')

display(BothSportsEnt_1Plus_TotalMinutes)



# -------------------------- Validation ------------------------
                                  #  dplyr::filter(., 
                                  #  convivasessionid %in% c('11648126:208337067:847992426:2001372969:444946579','1033357885:768355977:1849546281:746890872:2119640429','3374701516:2912684775:676957305:2997490608:820290983')) 

# COMMAND ----------

Channels_SDF = SparkR::read.df("/user/hive/warehouse/channels", source = "delta")

createOrReplaceTempView(Channels_SDF, "Channels_SDF")

SportsEnterainmentChannels_Table_DF = sdf_sql(sc,"select * from Channels_SDF")#%>%dplyr::select(.,`Channel Name `,`Asset Number`)

SportsChannels_Table_DF = sdf_sql(sc,"select * from Channels_SDF")%>% dplyr::filter(.,`channel type`=='Sports')#%>%dplyr::select(.,`Channel Name `,`Asset Number`)

display(SportsChannels_Table_DF)

# COMMAND ----------

# ========================= Sports only Excluding channels ====================================


# --------------- excluding all the channels & keeping only sports asset ------------------------
Sports_Exclude_Channel = dplyr::anti_join(BothSportsEnt_SD_V1,SportsEnterainmentChannels_Table_DF,by=c('asset_id'='Asset Number'))%>%
                         dplyr::filter(.,sports==1)

# ---------------- only Sports Channel
Sports_OnlySportsChannel = dplyr::inner_join(BothSportsEnt_SD_V1,SportsChannels_Table_DF,by=c('asset_id'='Asset Number'))%>%
                           dplyr::mutate(., ContentType = dplyr::if_else(ContentType=='Entertainment','Channel',ContentType))


# ======================== Sports Live VOD and Channel    =====================================

FinalSportsEnt = sdf_bind_rows(Sports_Exclude_Channel,Sports_OnlySportsChannel)%>%dplyr::rename(.,ChannelName =`Channel Name ` )

# display(Sports_OnlySportsChannel)

# COMMAND ----------

display(Sports_OnlySportsChannel)

# COMMAND ----------

# MAGIC %md
# MAGIC Channel Info

# COMMAND ----------

# ================================ Channel related information ======================================


# display(FinalSportsEnt %>%            
#                                    dplyr::group_by(.,viewerId,convivasessionid,ChannelName,ContentType,Month_,sports)%>%
#                                     dplyr::summarise(.,TotalM = sum(total_mins),.groups='drop')%>%ungroup()%>%
#                                    dplyr::filter(.,TotalM>=1)%>%
#                                    dplyr::select(.,viewerId,TotalM,ChannelName,Month_,sports)%>%
#                                    dplyr::group_by(.,ChannelName )%>%
#                                                                       dplyr::summarise(.,TotalMinutesSportsMinutes = round(sum(TotalM),2), UV = dplyr::n_distinct(viewerId),.groups='drop'))

# COMMAND ----------


# ======================== Sports 1+ minutes Total Minutes ==========================


Sports_1Plus_TotalMinutes = FinalSportsEnt %>% 
                                  dplyr::filter(.,ContentType!='Entertainment')%>%
                                  # dplyr::filter(.,sports==1)%>%
                                   dplyr::group_by(.,viewerId,convivasessionid,ContentType,Month_)%>%
                                    dplyr::summarise(.,TotalM = sum(total_mins),.groups='drop')%>%ungroup()%>%
                                   dplyr::filter(.,TotalM>=1)%>%
                                   dplyr::select(.,viewerId,TotalM,ContentType,Month_)%>%
                                   dplyr::group_by(.,Month_)%>%
                                   dplyr::summarise(.,TotalMinutesSportsMinutes = round(sum(TotalM),2), UV = dplyr::n_distinct(viewerId),.groups='drop')%>%
                                   dplyr::mutate(`Avg Minutes Per Viewer` = round(TotalMinutesSportsMinutes / UV, 2))

display(Sports_1Plus_TotalMinutes)


# --------------- validation -----------------------
                                # dplyr::filter(., 
                                #    convivasessionid %in% c('11648126:208337067:847992426:2001372969:444946579','1033357885:768355977:1849546281:746890872:2119640429','3374701516:2912684775:676957305:2997490608:820290983'))%>%

# COMMAND ----------

# =================== Football and NonFootball Category ===================

FootballNonFootball_1PlusMinutes = FinalSportsEnt %>% 
                                                                    dplyr::filter(.,ContentType!='Entertainment')%>%
                                   dplyr::group_by(.,viewerId,convivasessionid,ContentType,Month_)%>%
                                    dplyr::summarise(.,TotalM = sum(total_mins),.groups='drop')%>%ungroup()%>%
                                   dplyr::filter(.,TotalM>=1)%>%
                                   dplyr::select(.,viewerId,TotalM,ContentType,Month_)%>%
                                   dplyr::group_by(.,ContentType,Month_)%>%
                                   dplyr::summarise(.,TotalMinutesSportsMinutes = round(sum(TotalM),2), UV = dplyr::n_distinct(viewerId),.groups='drop')%>%
                                   dplyr::mutate(`Avg Minutes Per Viewer` = round(TotalMinutesSportsMinutes / UV, 2))

display(FootballNonFootball_1PlusMinutes)


# ------------------- Archived ----------------------------
# FootballNonFootball = AllSportsAssets %>% dplyr::group_by(.,SportsType,Month_)%>%dplyr::summarise(.,TotalMinutesSportsMinutes = round(sum(total_mins),2), UV = dplyr::n_distinct(viewerId))%>%
#   dplyr::mutate(`Avg Minutes Per Viewer` = round(TotalMinutesSportsMinutes / UV, 2))

# ------------------ validation -----------------------
                                # dplyr::filter(., 
                                #    convivasessionid %in% c('11648126:208337067:847992426:2001372969:444946579','1033357885:768355977:1849546281:746890872:2119640429','3374701516:2912684775:676957305:2997490608:820290983'))%>%

# display(FootballNonFootball)

# COMMAND ----------

# =================== Football_NonFootball  Category  ===================
# ------------------- Assets              -------------------
# AssetName1,CompetitionNameC1

Football_Non_Football_Asset_1PlusMinutes = FinalSportsEnt %>% 
                                                                    dplyr::filter(.,ContentType %in% c('Football','Non-Football'))%>%
                                   dplyr::group_by(.,viewerId,asset_id,CompetitionMainNames,convivasessionid,ContentType,asset1,Month_)%>%
                                    dplyr::summarise(.,TotalM = sum(total_mins),.groups='drop')%>%ungroup()%>%
                                   dplyr::filter(.,TotalM>=1)%>%
                                  #  dplyr::left_join(.,SportsAssetIdCompetition,by=c('asset_id'),copy=TRUE)%>%
                                  #  dplyr::select(.,viewerId,TotalM,ContentType,asset_id,asset1,AssetName1,Month_)%>%
                                   dplyr::select(.,viewerId,TotalM,CompetitionMainNames,asset1,ContentType,Month_)%>%
                                   dplyr::group_by(.,ContentType,CompetitionMainNames,asset1,Month_)%>%
                                   dplyr::summarise(.,TotalMinutesSportsMinutes = round(sum(TotalM),2), UV = dplyr::n_distinct(viewerId),.groups='drop')%>%
                                   dplyr::mutate(`Avg Minutes Per Viewer` = round(TotalMinutesSportsMinutes / UV, 2))%>%
                                   dplyr::arrange(., desc(TotalMinutesSportsMinutes ))
                                   

display(Football_Non_Football_Asset_1PlusMinutes)



# COMMAND ----------


# =================== Football-NonFootball  Category  ===================
# ------------------- Competition              -------------------


Football_NonFootball_competitionName_1PlusMinutes = FinalSportsEnt %>% 
                                                                    dplyr::filter(.,ContentType %in% c('Football','Non-Football'))%>%
                                   dplyr::group_by(.,viewerId,asset_id,CompetitionMainNames,convivasessionid,ContentType,asset1,Month_)%>%
                                    dplyr::summarise(.,TotalM = sum(total_mins),.groups='drop')%>%ungroup()%>%
                                   dplyr::filter(.,TotalM>=1)%>%
                                   dplyr::select(.,viewerId,TotalM,ContentType,CompetitionMainNames,Month_)%>%
                                   dplyr::group_by(.,ContentType,CompetitionMainNames,Month_)%>%
                                   dplyr::summarise(.,TotalMinutesSportsMinutes = round(sum(TotalM),2), UV = dplyr::n_distinct(viewerId),.groups='drop')%>%
                                   dplyr::mutate(`Avg Minutes Per Viewer` = round(TotalMinutesSportsMinutes / UV, 2))%>%
                                   dplyr::arrange(., desc(TotalMinutesSportsMinutes ))
                                   

display(Football_NonFootball_competitionName_1PlusMinutes)

# COMMAND ----------

# =================== Football  Category  ===================
# ------------------- Assets              -------------------
# AssetName1,CompetitionNameC1

Football_Asset_1PlusMinutes = FinalSportsEnt %>% 
                                                                    dplyr::filter(.,ContentType=='Football')%>%
                                   dplyr::group_by(.,viewerId,asset_id,CompetitionMainNames,convivasessionid,ContentType,asset1,Month_)%>%
                                    dplyr::summarise(.,TotalM = sum(total_mins),.groups='drop')%>%ungroup()%>%
                                   dplyr::filter(.,TotalM>=1)%>%
                                  #  dplyr::left_join(.,SportsAssetIdCompetition,by=c('asset_id'),copy=TRUE)%>%
                                  #  dplyr::select(.,viewerId,TotalM,ContentType,asset_id,asset1,AssetName1,Month_)%>%
                                   dplyr::select(.,viewerId,TotalM,CompetitionMainNames,asset1,ContentType,Month_)%>%
                                   dplyr::group_by(.,ContentType,CompetitionMainNames,asset1,Month_)%>%
                                   dplyr::summarise(.,TotalMinutesSportsMinutes = round(sum(TotalM),2), UV = dplyr::n_distinct(viewerId),.groups='drop')%>%
                                   dplyr::mutate(`Avg Minutes Per Viewer` = round(TotalMinutesSportsMinutes / UV, 2))%>%
                                   dplyr::arrange(., desc(TotalMinutesSportsMinutes ))
                                   

display(Football_Asset_1PlusMinutes)



# COMMAND ----------

# =================== Football  Category  ===================
# ------------------- Competition              -------------------


Football_competitionName_1PlusMinutes = FinalSportsEnt %>% 
                                                                    dplyr::filter(.,ContentType=='Football',)%>%
                                   dplyr::group_by(.,viewerId,asset_id,CompetitionMainNames,convivasessionid,ContentType,asset1,Month_)%>%
                                    dplyr::summarise(.,TotalM = sum(total_mins),.groups='drop')%>%ungroup()%>%
                                   dplyr::filter(.,TotalM>=1)%>%
                                   dplyr::select(.,viewerId,TotalM,ContentType,CompetitionMainNames,Month_)%>%
                                   dplyr::group_by(.,ContentType,CompetitionMainNames,Month_)%>%
                                   dplyr::summarise(.,TotalMinutesSportsMinutes = round(sum(TotalM),2), UV = dplyr::n_distinct(viewerId),.groups='drop')%>%
                                   dplyr::mutate(`Avg Minutes Per Viewer` = round(TotalMinutesSportsMinutes / UV, 2))%>%
                                   dplyr::arrange(., desc(TotalMinutesSportsMinutes ))
                                   

display(Football_competitionName_1PlusMinutes)




# COMMAND ----------

# =================== Non-Football  Category  ===================
# ------------------- Assets              -------------------


NonFootball_Asset_1PlusMinutes = FinalSportsEnt %>% 
                                                                    dplyr::filter(.,ContentType=='Non-Football',)%>%
                                   dplyr::group_by(.,viewerId,CompetitionMainNames,asset_id,convivasessionid,ContentType,asset1,Month_)%>%
                                    dplyr::summarise(.,TotalM = sum(total_mins),.groups='drop')%>%ungroup()%>%
                                   dplyr::filter(.,TotalM>=1)%>%
                                   dplyr::select(.,viewerId,CompetitionMainNames,TotalM,ContentType,asset1,Month_)%>%
                                   dplyr::group_by(.,ContentType,CompetitionMainNames,asset1,Month_)%>%
                                   dplyr::summarise(.,TotalMinutesSportsMinutes = round(sum(TotalM),2), UV = dplyr::n_distinct(viewerId),.groups='drop')%>%
                                   dplyr::mutate(`Avg Minutes Per Viewer` = round(TotalMinutesSportsMinutes / UV, 2))%>%
                                   dplyr::arrange(., desc(TotalMinutesSportsMinutes ))
                                   

display(NonFootball_Asset_1PlusMinutes)



# COMMAND ----------

# =================== Football  Category  ===================
# ------------------- Competition              -------------------


NonFootball_competitionName_1PlusMinutes = FinalSportsEnt %>% 
                                    dplyr::filter(.,ContentType=='Non-Football',)%>%
                                   dplyr::group_by(.,viewerId,asset_id,competitionName,convivasessionid,ContentType,asset1,Month_)%>%
                                    dplyr::summarise(.,TotalM = sum(total_mins),.groups='drop')%>%ungroup()%>%
                                   dplyr::filter(.,TotalM>=1)%>%
                                   dplyr::select(.,viewerId,TotalM,ContentType,competitionName,Month_)%>%
                                   dplyr::group_by(.,ContentType,competitionName,Month_)%>%
                                   dplyr::summarise(.,TotalMinutesSportsMinutes = round(sum(TotalM),2), UV = dplyr::n_distinct(viewerId),.groups='drop')%>%
                                   dplyr::mutate(`Avg Minutes Per Viewer` = round(TotalMinutesSportsMinutes / UV, 2))%>%
                                   dplyr::arrange(., desc(TotalMinutesSportsMinutes ))
                                   

display(NonFootball_competitionName_1PlusMinutes)




# COMMAND ----------



sdf_register(Football_Non_Football_Asset_1PlusMinutes,'Football_Non_Football_Asset_1PlusMinutes')

sdf_register(Football_NonFootball_competitionName_1PlusMinutes,'Football_NonFootball_competitionName_1PlusMinutes')

sdf_register(BothSportsEnt_1Plus_TotalMinutes,'BothSportsEnt_1Plus_TotalMinutes')

sdf_register(Sports_1Plus_TotalMinutes,'Sports_1Plus_TotalMinutes')

sdf_register(FootballNonFootball_1PlusMinutes,'FootballNonFootball_1PlusMinutes')

sdf_register(Football_Asset_1PlusMinutes,'Football_Asset_1PlusMinutes')

sdf_register(Football_competitionName_1PlusMinutes,'Football_competitionName_1PlusMinutes')

sdf_register(NonFootball_Asset_1PlusMinutes,'NonFootball_Asset_1PlusMinutes')

sdf_register(NonFootball_competitionName_1PlusMinutes,'NonFootball_competitionName_1PlusMinutes')



# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC Football_Non_Football_Asset_1PlusMinutes_sdf = spark.sql("select * from  Football_Non_Football_Asset_1PlusMinutes ")
# MAGIC Football_NonFootball_competitionName_1PlusMinutes_sdf = spark.sql("select * from  Football_NonFootball_competitionName_1PlusMinutes ")
# MAGIC BothSportsEnt_1Plus_TotalMinutes_sdf = spark.sql("select * from  BothSportsEnt_1Plus_TotalMinutes ")
# MAGIC Sports_1Plus_TotalMinutes_sdf = spark.sql("select * from  Sports_1Plus_TotalMinutes ")
# MAGIC FootballNonFootball_1PlusMinutes_sdf = spark.sql("select * from  FootballNonFootball_1PlusMinutes ")
# MAGIC Football_Asset_1PlusMinutes_sdf = spark.sql("select * from  Football_Asset_1PlusMinutes ")
# MAGIC Football_competitionName_1PlusMinutes_sdf = spark.sql("select * from  Football_competitionName_1PlusMinutes ")
# MAGIC NonFootball_Asset_1PlusMinutes_sdf = spark.sql("select * from  NonFootball_Asset_1PlusMinutes ")
# MAGIC NonFootball_competitionName_1PlusMinutes_sdf = spark.sql("select * from  NonFootball_competitionName_1PlusMinutes ")
# MAGIC
# MAGIC Football_Non_Football_Asset_1PlusMinutes_sdf_Pdf  = Football_Non_Football_Asset_1PlusMinutes_sdf.toPandas()
# MAGIC Football_NonFootball_competitionName_1PlusMinutes_sdf_Pdf  = Football_NonFootball_competitionName_1PlusMinutes_sdf.toPandas()
# MAGIC BothSportsEnt_1Plus_TotalMinutes_sdf_Pdf  = BothSportsEnt_1Plus_TotalMinutes_sdf.toPandas()
# MAGIC Sports_1Plus_TotalMinutes_sdf_Pdf  = Sports_1Plus_TotalMinutes_sdf.toPandas()
# MAGIC FootballNonFootball_1PlusMinutes_sdf_Pdf  = FootballNonFootball_1PlusMinutes_sdf.toPandas()
# MAGIC Football_Asset_1PlusMinutes_sdf_Pdf  = Football_Asset_1PlusMinutes_sdf.toPandas()
# MAGIC Football_competitionName_1PlusMinutes_sdf_Pdf  = Football_competitionName_1PlusMinutes_sdf.toPandas()
# MAGIC NonFootball_Asset_1PlusMinutes_sdf_Pdf  = NonFootball_Asset_1PlusMinutes_sdf.toPandas()
# MAGIC NonFootball_competitionName_1PlusMinutes_sdf_Pdf  = NonFootball_competitionName_1PlusMinutes_sdf.toPandas()
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC import pandas as pd
# MAGIC import pyspark
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.functions import col, when
# MAGIC from pyspark.sql.functions import sum
# MAGIC from pyspark.sql.functions import countDistinct
# MAGIC from datetime import date, datetime, timedelta
# MAGIC from pyspark.sql.types import IntegerType
# MAGIC from azure.core.credentials import AzureNamedKeyCredential
# MAGIC from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# MAGIC import xlsxwriter
# MAGIC import io
# MAGIC
# MAGIC # writer = pd.ExcelWriter('FinalViewingMinutes9Jan2025.xlsx',engine='xlsxwriter')
# MAGIC
# MAGIC access_key = dbutils.secrets.get(scope="BlobAccessMLBMan07",key="SportsBlobPS01")
# MAGIC credential = AzureNamedKeyCredential("mlblobman01", access_key)
# MAGIC
# MAGIC account_name = 'mlblobman01'
# MAGIC access_key = dbutils.secrets.get(scope="mlstorage7",key="mlman7")
# MAGIC container_name = 'gceosportsmonthly'
# MAGIC  # blob_name = 'test.xlsx'
# MAGIC account_url = 'https://mlblobman01.blob.core.windows.net/'
# MAGIC
# MAGIC blob_service_client = BlobServiceClient(account_url, credential=credential)
# MAGIC
# MAGIC
# MAGIC output = io.BytesIO()
# MAGIC
# MAGIC  # Use the BytesIO object as the filehandle.
# MAGIC writer = pd.ExcelWriter(output, engine='xlsxwriter')
# MAGIC
# MAGIC def write_excel(df, row,col,headertext):
# MAGIC
# MAGIC      # Write the data frame to the BytesIO object.
# MAGIC     df.to_excel(writer, startrow=row, startcol=col,index=False)
# MAGIC     text = headertext
# MAGIC     workbook  = writer.book
# MAGIC     worksheet = writer.sheets['Sheet1']
# MAGIC     worksheet.write(row-1, col, text)
# MAGIC
# MAGIC
# MAGIC Football_Non_Football_Asset_1PlusMinutes_sdf_Pdf  = Football_Non_Football_Asset_1PlusMinutes_sdf.toPandas()
# MAGIC Football_NonFootball_competitionName_1PlusMinutes_sdf_Pdf  = Football_NonFootball_competitionName_1PlusMinutes_sdf.toPandas()
# MAGIC BothSportsEnt_1Plus_TotalMinutes_sdf_Pdf  = BothSportsEnt_1Plus_TotalMinutes_sdf.toPandas()
# MAGIC Sports_1Plus_TotalMinutes_sdf_Pdf  = Sports_1Plus_TotalMinutes_sdf.toPandas()
# MAGIC FootballNonFootball_1PlusMinutes_sdf_Pdf  = FootballNonFootball_1PlusMinutes_sdf.toPandas()
# MAGIC Football_Asset_1PlusMinutes_sdf_Pdf  = Football_Asset_1PlusMinutes_sdf.toPandas()
# MAGIC Football_competitionName_1PlusMinutes_sdf_Pdf  = Football_competitionName_1PlusMinutes_sdf.toPandas()
# MAGIC NonFootball_Asset_1PlusMinutes_sdf_Pdf  = NonFootball_Asset_1PlusMinutes_sdf.toPandas()
# MAGIC NonFootball_competitionName_1PlusMinutes_sdf_Pdf  = NonFootball_competitionName_1PlusMinutes_sdf.toPandas()
# MAGIC
# MAGIC frames = { 'TotalMinutes':BothSportsEnt_1Plus_TotalMinutes_sdf_Pdf,'Sports1PlusMinutes':Sports_1Plus_TotalMinutes_sdf_Pdf,'FootballNonFootball':FootballNonFootball_1PlusMinutes_sdf_Pdf,'FootballAsset':Football_Asset_1PlusMinutes_sdf_Pdf,
# MAGIC 'FootballCompetition':Football_competitionName_1PlusMinutes_sdf_Pdf,'NonFootballAsset':NonFootball_Asset_1PlusMinutes_sdf_Pdf,
# MAGIC 'NonFootballCompetition':NonFootball_competitionName_1PlusMinutes_sdf_Pdf,'Ftball_Non_Ftball_Ast':Football_Non_Football_Asset_1PlusMinutes_sdf_Pdf,'Ftball_Non_Ftball_Comp':Football_NonFootball_competitionName_1PlusMinutes_sdf_Pdf}
# MAGIC
# MAGIC for sheet, frame in  frames.items():    
# MAGIC     frame.to_excel(writer, sheet_name = sheet)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from datetime import date, datetime, timedelta
# MAGIC import datetime
# MAGIC
# MAGIC writer.close()
# MAGIC
# MAGIC xlsx_data = output.getvalue()
# MAGIC
# MAGIC filename = datetime.datetime.now().strftime("%H:%M:%S")
# MAGIC  # filename = '12122023'
# MAGIC monthPpy = datetime.datetime.strptime(start_date_m, "%Y-%m-%d").strftime("%b")
# MAGIC
# MAGIC
# MAGIC  # Create a blob client using the local file name as the name for the blob
# MAGIC blob_client = blob_service_client.get_blob_client(container=container_name,blob='GCEO_Sports_Content_'+monthPpy+filename+'.xlsx')
# MAGIC
# MAGIC  # Upload the created file
# MAGIC blob_client.upload_blob(xlsx_data, overwrite=True)
# MAGIC
