# Databricks notebook source
# MAGIC %python
# MAGIC #---------------------------------- Geting the month start and end date
# MAGIC
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
# MAGIC
# MAGIC start_date = get_month_dates("August", 2025) 
# MAGIC end_date = get_month_dates("August", 2025)
# MAGIC
# MAGIC
# MAGIC start_date_m=start_date["StartDate"]
# MAGIC end_date_m = end_date["EndDate"]
# MAGIC
# MAGIC
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
# MAGIC
# MAGIC DIM_SubscribersInfo = f'select * from  DIM.Subscribers'
# MAGIC
# MAGIC DIM_SubscribersInfo_SD = (spark.read
# MAGIC   .format("jdbc")
# MAGIC   .option("url", jdbcUrl)
# MAGIC #   .option("dbtable","[dbviews].[ViewerSessions]" )
# MAGIC   .option("user",jdbcUsername)
# MAGIC   .option("password",  jdbcPassword)
# MAGIC   .option("query", DIM_SubscribersInfo) 
# MAGIC   .load()
# MAGIC )
# MAGIC
# MAGIC DIM_SubscribersInfo_SD.createOrReplaceTempView("DIM_SubscribersInfo_SD")

# COMMAND ----------


#----------------------- R library --------------------

library(data.table)
library(dplyr)
library(lubridate)
library(sparklyr)
library(SparkR)
library(AzureStor)
library(arrow)

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# # ========== Taking top ten from each subscription category for Subscriber table ===============================

DIM_SubscribersInfo_SDF = sdf_sql(sc,"select * from DIM_SubscribersInfo_SD where Customer_External_ID IS NOT NULL") 

# sparklyr::spark_write_table(DIM_Subsc_Detail_Information,'DIM_Subsc_Detail_Information_17Sep2025',mode='overwrite')

#------------------- sdf_registring ------------
sdf_register(DIM_SubscribersInfo_SDF,'DIM_SubscribersInfo_SDF')


display(DIM_SubscribersInfo_SDF)

# COMMAND ----------

# ===================== DIM write to catalog ========

# sparklyr::spark_write_table(DIM_SubscribersInfo_SDF,'DIM_SubscribersInfo_SDF_23Sep2025',mode='overwrite')


DIM_SubscribersInfo_SDF = sparklyr::spark_read_table(sc,'DIM_SubscribersInfo_SDF_23Sep2025')

# COMMAND ----------





# #------------------  Sparklyr Caching  ------------------------------
# tbl_cache(sc,'DIM_Subsc_Detail_Information_V1')


# test_cache_tbl = tbl(sc,'DIM_Subsc_Detail_Information_V1')


# ============== Selecting some columns ===============

Subscription_Eligible_SDF = DIM_SubscribersInfo_SDF %>%dplyr::select(.,Customer_ID,Customer_External_ID,Subscription_ID,Subscription_latest,Subscription_status,Subscription_type,Subscription_start_date,Expiry_date,Subscription_calender_date)#%>% dplyr::filter(.,Subscription_latest==1)


display(Subscription_Eligible_SDF)

# COMMAND ----------




# ------------------------------- 1 Qtr Gross Adds (1st Jan - 31st Mar 2025)--------------------------------------
# # ----------------- Active base for Aug -------

# Closing_Active_Subscriber = DIM_SubscribersInfo_SDF %>% dplyr::mutate(.,
# SubsStatus0= dplyr::if_else(Subscription_calender_date<='2025-08-31'& Expiry_date>='2025-08-31',1,0))%>%
# dplyr::filter(.,SubsStatus0==1)%>%
# dplyr::summarise(.,TotalActive=dplyr::n_distinct(Customer_External_ID))

#=================================================================================================
# =================  New  ============================
#=================================================================================================


# ============ NEw customer for gross adds -----------%>% 

# ------------ New Adds 
New_Q1 = Subscription_Eligible_SDF %>% 
dplyr::arrange(.,Customer_External_ID,Subscription_calender_date) %>% 
dplyr::group_by(.,Customer_External_ID) %>% 
dplyr::mutate(.,ID=1,RankFirstSubs = cumsum(ID))%>% 
ungroup()%>%
dplyr::filter(., RankFirstSubs==1)%>%
dplyr::filter(.,dplyr::between(Subscription_calender_date,'2025-02-01','2025-02-28'))%>% #'2025-01-01','2025-03-31'
dplyr::select(.,Customer_External_ID,Subscription_ID,Subscription_calender_date,RankFirstSubs)
# dplyr::select(.,Customer_External_ID,Subscription_ID,Subscription_start_date,RankFirstSubs)

# ======= summary=========
New_Q1_1 = New_Q1 %>% dplyr::summarise(.,Total_UV_Subcribers = dplyr::n_distinct(Customer_External_ID), Total_UV_Subscrip = dplyr::n_distinct(Subscription_ID),Total = n())


# =============== New Customer ID =================
New_Actual_Q1 = New_Q1 %>% dplyr::select(.,Customer_External_ID) %>% sdf_distinct()%>%dplyr::mutate(., Type='New')

# display(New_Actual_Q1)

# display(New_Q1_1)

#=================================================================================================
# ================= Winback ============================
#=================================================================================================


# ChurnedBeforePeriod
Churned_Q1 = Subscription_Eligible_SDF %>% 
dplyr::arrange(.,Customer_External_ID,Subscription_calender_date) %>% 
dplyr::filter(., Subscription_latest==1,Subscription_status=='churned')%>%
dplyr::filter(.,Subscription_calender_date<'2025-02-01')%>%#'2025-01-01'
dplyr::select(.,Customer_External_ID,Subscription_ID,Subscription_calender_date)%>% 
                  dplyr::select(.,Customer_External_ID)%>% 
                  sdf_distinct(.,Customer_External_ID)

 Churned_Q1_1 = Churned_Q1 %>% dplyr::summarise(.,TotalCNT = dplyr::n_distinct(Customer_External_ID))

#  display(Churned_2)




WinB_Q1 = Subscription_Eligible_SDF %>% 
dplyr::arrange(.,Customer_External_ID,Subscription_calender_date) %>% 
dplyr::filter(., Subscription_latest==1,Subscription_status=='active')%>%
dplyr::filter(.,Subscription_calender_date<'2025-02-01')%>%#'2025-01-01'
dplyr::select(.,Customer_External_ID,Subscription_ID,Subscription_calender_date)



ChurnedBefore_Q1 = dplyr::anti_join(Churned_Q1,WinB_Q1,by=c("Customer_External_ID")) %>% 
                  dplyr::select(.,Customer_External_ID)%>% 
                  sdf_distinct(.,Customer_External_ID)



RejoinedInPeriod_Q1 = Subscription_Eligible_SDF %>% 
dplyr::arrange(.,Customer_External_ID,Subscription_calender_date) %>% 
dplyr::filter(., Subscription_latest==1,Subscription_status=='active')%>%
dplyr::filter(.,dplyr::between(Subscription_calender_date,'2025-02-01','2025-02-28'))%>%#====== Please add the date based on the #'2025-01-01','2025-03-31'
dplyr::select(.,Customer_External_ID,Subscription_ID,Subscription_calender_date)


Winback_Q1= dplyr::inner_join(ChurnedBefore_Q1,RejoinedInPeriod_Q1,by=c('Customer_External_ID'))%>% 
                  dplyr::select(.,Customer_External_ID)%>% 
                  sdf_distinct(.,Customer_External_ID)


Winback_Actual_Q1 = Winback_Q1 %>% dplyr::select(.,Customer_External_ID) %>% sdf_distinct()%>%dplyr::mutate(., Type='Winback')

# ========== Winback summary ============
WinSumb=Winback_Actual_Q1 %>% dplyr::summarise(.,TotalCNT = dplyr::n_distinct(Customer_External_ID))

# =============== Final External ID Q1  ==========================
Final_New_Winback_Q1 = sdf_bind_rows(New_Actual_Q1,Winback_Actual_Q1)


display(Final_New_Winback_Q1)


# COMMAND ----------

display(Final_New_Winback_Q1 %>% dplyr::group_by(.,Type) %>% dplyr::summarise(.,TotalCNT = n_distinct(Customer_External_ID)))#--------- Validation 

# COMMAND ----------

# ======================== Calculating the number of days of subscription====================================


# ==== Selecting only the Gross and New adds from the Q1 ==============
Subscription_Eligible_SDF = dplyr::inner_join(Subscription_Eligible_SDF,Final_New_Winback_Q1,by=c('Customer_External_ID'),copy='TRUE')

Final_Partner_Direct_Avg_Leng_Subs= Subscription_Eligible_SDF %>% 
 dplyr::filter(.,Subscription_latest ==1)%>%
    dplyr::mutate(.,MonthP = lubridate::month(Subscription_start_date),
                    YearP = lubridate::year(Subscription_start_date))%>%
                             dplyr::select(.,Customer_External_ID,Subscription_ID,MonthP,YearP,Expiry_date,Subscription_start_date,Subscription_latest)%>%
                                       dplyr::mutate(.,SubStartD = to_date(Subscription_start_date,"yyyy-MM-dd"),SubEndD = to_date(Expiry_date,"yyyy-MM-dd"))%>%
                                      dplyr::group_by(.,Customer_External_ID,MonthP,YearP)%>%
                                      dplyr::mutate(.,SubsNoDays = dplyr::if_else(datediff(SubEndD,SubStartD)==0,1, datediff(SubEndD,SubStartD)), FinalExpiry= SubStartD+as.integer(SubsNoDays))%>%
                                      ungroup()%>%
                                      dplyr::arrange(.,Customer_External_ID,Subscription_start_date)

display(Final_Partner_Direct_Avg_Leng_Subs)





# COMMAND ----------

# ============================= Creating Sequential Dataframe for the month year which is required for CLV creation ===================
# Define the years
years <- c(seq(2023,2029,1))

# Define the months (1 to 12)
months <- 1:12

# Create an empty data frame to store the results
# It's more efficient to pre-allocate or build a list of rows and then combine
# Initialize lists to store month and year values
all_months <- c()
all_years <- c()

# Loop through each year and each month to create combinations
for (year in years) {
  for (month in months) {
    all_months <- c(all_months, month)
    all_years <- c(all_years, year)
  }
}

# Create the data frame
SequenceDF <- data.frame(
  YearP = all_years,
  MonthP = all_months
)%>% 
dplyr::mutate(.,ID=1)%>%
dplyr::mutate(.,SeqID = cumsum(ID))%>% dplyr::select(.,-ID)


SequenceDFd = dplyr::copy_to(sc,SequenceDF,'SequenceDF')

sdf_register(SequenceDFd,'SequenceDFd')

display(SequenceDFd)

# COMMAND ----------



# COMMAND ----------

# =========== reading to catalog ================

# EntertainViewer_Surviver_Cohort_Analysis_V1

# EntertainViewer_Surviver_Cohort_Analysis_V1 = sparklyr::spark_read_table(sc,'EntertainViewer_Surviver_Cohort_Analysis_V1_3')


# display(EntertainViewer_Surviver_Cohort_Analysis_V1 %>% 
#               dplyr::select(.,Customer_External_ID)%>%
#               sdf_distinct()%>%
#               dplyr::summarise(., TotalCNT = n())
# )

# COMMAND ----------


# ================ For survival analysis converting number of months more than 12 months limiting to 12

EntertainViewer_Surviver_Cohort_Analysis_V1 = Final_Partner_Direct_Avg_Leng_Subs %>% 
                                              dplyr::mutate(.,NumMonthIteration = as.integer(round(SubsNoDays / 30, 0)))%>%
                                              dplyr::select(.,Customer_External_ID,MonthP,YearP,NumMonthIteration)%>%
                                              dplyr::mutate(.,NumMonthIteration= dplyr::if_else(NumMonthIteration==0,1,NumMonthIteration),
                                            NumMonthIteration=dplyr::if_else(NumMonthIteration>12,(13-MonthP),NumMonthIteration))%>%
                                               #--- subtracting 13 from the Month to keep it at 12
                                               
                                              dplyr::select(.,Customer_External_ID,MonthP,YearP,NumMonthIteration)%>%
                                              sdf_distinct()%>%
                                              dplyr::arrange(.,Customer_External_ID,MonthP,YearP)

display(EntertainViewer_Surviver_Cohort_Analysis_V1)




# COMMAND ----------

# --------------- Iterating the month as per the existance-------------

MonthAbbre = data.frame(N=c(1,2,3,4,5,6,7,8,9,10,11,12),
MonAb=c('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'))

EntertainViewer_Surviver_Cohort_Analysis_V2 = EntertainViewer_Surviver_Cohort_Analysis_V1 %>% 
                                              dplyr::filter(.,YearP==2025)%>%
                                              sdf_collect()%>%
                                              dplyr::rowwise()%>%
                                          dplyr::mutate(MonthP = list(MonthP:(MonthP + NumMonthIteration - 1)),ID=1) %>%
                                        tidyr::unnest(cols = c(MonthP))%>%
                                        dplyr::left_join(.,MonthAbbre,by=c('MonthP'='N'),copy='TRUE')%>%
                                        dplyr::select(.,-NumMonthIteration)%>%
                                                dplyr::distinct()

EntertainViewer_Surviver_Cohort_Analysis_V2 = dplyr::copy_to(sc,EntertainViewer_Surviver_Cohort_Analysis_V2,'EntertainViewer_Surviver_Cohort_Analysis_V2')

sdf_register(EntertainViewer_Surviver_Cohort_Analysis_V2,'EntertainViewer_Surviver_Cohort_Analysis_V2_SDF')

    display(EntertainViewer_Surviver_Cohort_Analysis_V2)

# COMMAND ----------

# MAGIC %md
# MAGIC Sequential Analysis

# COMMAND ----------

# ===================================================================================================
#                   Suvival Profile or subscription starting month till they exist in the system 
#==================================================================================================

# ============================= Creating Sequential Dataframe for the month year which is required for CLV creation ===================
# Define the years
years <- c(seq(2023,2029,1))

# Define the months (1 to 12)
months <- 1:12

# Create an empty data frame to store the results
# It's more efficient to pre-allocate or build a list of rows and then combine
# Initialize lists to store month and year values
all_months <- c()
all_years <- c()

# Loop through each year and each month to create combinations
for (year in years) {
  for (month in months) {
    all_months <- c(all_months, month)
    all_years <- c(all_years, year)
  }
}

# Create the data frame
SequenceDF <- data.frame(
  YearP = all_years,
  MonthP = all_months
)%>% 
dplyr::mutate(.,ID=1)%>%
dplyr::mutate(.,SeqID = cumsum(ID))%>% dplyr::select(.,-ID)


SequenceDFd = dplyr::copy_to(sc,SequenceDF,'SequenceDF')

sdf_register(SequenceDFd,'SequenceDFd')


# =============== Merging Sequential month from 2024 till 2029 for each of the month with Final Dataset ===================

Final_Partner_Direct_Merg_DIMSubs_V3 = EntertainViewer_Surviver_Cohort_Analysis_V2 %>% dplyr::left_join(.,SequenceDFd,by=c('YearP','MonthP'))%>% dplyr::arrange(.,Customer_External_ID,YearP,MonthP)

display(Final_Partner_Direct_Merg_DIMSubs_V3)

# COMMAND ----------

# ========================================================================================================================================
#                   Suvival Profile or subscription starting month till they exist in the system 
#==========================================================================================================================================

# =================== Will get the sequence of the starting month of subscription and continuing 

Calculating_Survival_Cohort_SDF <- Final_Partner_Direct_Merg_DIMSubs_V3 %>%
                dplyr::arrange(.,Customer_External_ID,YearP,MonthP)%>%
  dplyr::group_by(Customer_External_ID) %>% # Group by column 'A' as FF calculation is separate for each 'A' value
  dplyr::mutate(
    # Identify the start of a new sequence in column 'B'
    # A new sequence starts if it's the first row in the group (row_number() == 1)
    # OR if the current 'B' value is not (previous 'B' value + 1)
    sequence_start = (row_number() == 1) | (SeqID - lag(SeqID) != 1),sequence_id = cumsum(as.integer(sequence_start))
    
    # Create a unique ID for each sequence within the current 'A' group.
    # cumsum(sequence_start) increments the ID every time a 'sequence_start' is TRUE.
    
  )%>%
  dplyr::select(.,-SeqID)%>%
  dplyr::group_by(.,Customer_External_ID,sequence_id)%>%
  dplyr::mutate(.,StartMonth=cumsum(ID), StartingMonthCohort = dplyr::if_else(StartMonth==1,MonAb,""))%>%#--- gets the first month or starting month 
  ungroup()%>%
  dplyr::select(.,-sequence_start,)
  

# --------------- Starting Cohort mapping table ---------

Starting_Month_Cohort = Calculating_Survival_Cohort_SDF%>%dplyr::select(.,Customer_External_ID,sequence_id,StartingMonthCohort)%>%sdf_distinct()%>%
                        dplyr::filter(., StartingMonthCohort != "")%>%dplyr::rename(.,  StartingMonthCohort2=StartingMonthCohort)

# display(Calculating_Survival_Cohort_SDF)
display(Starting_Month_Cohort)

# COMMAND ----------

# ===================================== Joining the Final Table with Cohort Mapping Table =============

Final_Chorot_Mapping = dplyr::left_join(Calculating_Survival_Cohort_SDF,Starting_Month_Cohort,by=c('Customer_External_ID','sequence_id'),copy='TRUE')%>%dplyr::select(.,-StartingMonthCohort,-StartingMonthCohort)%>% dplyr::filter(.,dplyr::between(MonthP,1,12),YearP==2025)

display(Final_Chorot_Mapping)

# COMMAND ----------

# ======================= Looping to get the respective month cohort Analysis =========================


Final_Chorot_Mapping_v1 = Final_Chorot_Mapping %>% 
                        # dplyr::filter(.,Customer_External_ID %in% c('0005724e-c51f-4106-8511-b9f65ce6b773','000e72ce-eba2-46e8-9a38-8114de5a7e19','0010fa2f-a700-4f9b-9b90-647609b3b8e7','007498c5-a3d8-4bc1-9560-eb5f6f3d9be7'))%>%
                        dplyr::select(.,Customer_External_ID,StartingMonthCohort2,MonAb,ID)%>%
                        sdf_collect()%>%
                        reshape2::dcast(.,Customer_External_ID+StartingMonthCohort2~MonAb,value.var='ID')%>%
                        dplyr::select(.,Customer_External_ID,StartingMonthCohort2,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec)%>%
                        replace(is.na(.),0)%>%
                        dplyr::arrange(.,StartingMonthCohort2)

# 1. Define the correct order of month abbreviations
month_levels <- c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

Final_Chorot_Mapping_v1 <- Final_Chorot_Mapping_v1 %>%
  dplyr::mutate(StartingMonthCohort2 = factor(StartingMonthCohort2, levels = month_levels)) %>%
  dplyr::arrange(StartingMonthCohort2)

display(Final_Chorot_Mapping_v1)




# COMMAND ----------

library(AzureStor)

bl_endp_key2 <- storage_endpoint("https://mlblobman01.blob.core.windows.net/", key= dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01"))

list_storage_containers(bl_endp_key2)

cont <- storage_container(bl_endp_key2, "profileinfouser")

list_storage_files(cont)

profileinfouser <- storage_read_csv(cont, "FinalProfileDetails_9Sep2025.csv") %>% data.frame() 

profileinfouser_sdf = dplyr::copy_to(sc,profileinfouser,'profileinfouser',overwrite = TRUE)

sdf_register(profileinfouser_sdf,'profileinfouser_sdf')

# ============ New dataset for profile
profileinfouser_V2_SDF=sdf_sql(sc,"select * from profileinfouser_sdf")

 profileinfouser_SDF = profileinfouser_V2_SDF %>% 
                        dplyr::rename(.,"user_profile_id" ="USER_PROFILE_ID" ,   "cleeng_customer_id"="CLEENG_CUSTOMER_ID", "adb2c_id"  ="ADB2C_ID" ,        
  "profile_type" ="PROFILE_TYPE"  ,      "profile_name" ="PROFILE_NAME",      "is_main_profile"  ="IS_MAIN_PROFILE" , 
  "creation_date" ="CREATION_DATE"  ,      
 "is_active"="IS_ACTIVE" )


 # ================ Number of Profile created by Users  =============


NumberofProfileCreated_PerUser = profileinfouser_SDF %>%# profileinfouser_SDF_Aug %>%# profileinfouser_sdf ---------- Earlier Data before Fatma facilitated the information
dplyr::select(.,cleeng_customer_id,adb2c_id,is_active)%>% 
# dplyr::filter(adb2c_id %in% c(,'713f19c9-94fb-4e20-b5a8-31450ed237d5','c0a41819-e641-4366-8d3a-b6e3d0205a07','eb6ae033-7551-4324-ae08-03c07a8b02a0','e28053d9-be3e-4929-a502-5c3824df85b9') )%>% 
dplyr::filter(.,is_active==1)%>%

dplyr::group_by(.,cleeng_customer_id,adb2c_id)%>% 
dplyr::summarise(., TotalNumProfile = dplyr::n(),.groups='drop')%>% 
dplyr::arrange(.,desc(TotalNumProfile))%>%
dplyr::mutate(.,Group_Profile_Users = dplyr::case_when(TotalNumProfile==1 ~ '1-Profile',
                                                        TotalNumProfile==2 ~ '2-Profile',
                                                        TotalNumProfile==3 ~ '3-Profile',
                                                        TotalNumProfile==4 ~ '4-Profile',
                                                         TotalNumProfile==5 ~ '5-Profile',
                                                          TotalNumProfile>5&TotalNumProfile<=10~ 'Greater 5 -10 Profile',
                                                          TotalNumProfile>10 ~ 'Greater 10 Profile'))




display(NumberofProfileCreated_PerUser)

# --------------- Only selecting CustomerID and Number of Profile Type -----------

NumberofProfileCreated_PerUser_V1 = NumberofProfileCreated_PerUser %>% dplyr::select(.,adb2c_id,Group_Profile_Users) 

display(NumberofProfileCreated_PerUser_V1)

# ---------------- Validaton Archive ------

# NumberofProfileCreated_PerUser_V1 = NumberofProfileCreated_PerUser %>% dplyr::select(.,adb2c_id,Group_Profile_Users) %>% dplyr::group_by(.,adb2c_id)%>% dplyr::mutate(., CNT= dplyr::n())

# COMMAND ----------

library(AzureStor)#survivalanalysis

bl_endp_key2 <- storage_endpoint("https://mlblobman01.blob.core.windows.net/", key= dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01"))

list_storage_containers(bl_endp_key2)

cont <- storage_container(bl_endp_key2, "survivalanalysis")

# Final_Chorot_Mapping_v1 = Final_Chorot_Mapping_v1 %>% as.data.frame()

# COMMAND ----------

# storage_write_csv(Final_Chorot_Mapping_v1, cont, "Final_Chorot_Mapping_v1_3_24Sep2025.csv")

Final_Chorot_Mapping_v1 = storage_read_csv(cont, "Final_Chorot_Mapping_v1_3_24Sep2025.csv")%>%data.frame()


Final_Chorot_Mapping_v1_SDF = dplyr::copy_to(sc,Final_Chorot_Mapping_v1,'Final_Chorot_Mapping_v1')

sdf_register(Final_Chorot_Mapping_v1_SDF,'Final_Chorot_Mapping_v1_SDF')

Final_Chorot_Mapping_SDF_V2 = sdf_sql(sc,'select * from Final_Chorot_Mapping_v1_SDF')


# COMMAND ----------

Final_Chorot_Mapping_SDF_V3 = dplyr::left_join(Final_Chorot_Mapping_SDF_V2,NumberofProfileCreated_PerUser_V1,by=c('Customer_External_ID'='adb2c_id'),copy='TRUE')


# sparklyr::spark_write_table(Final_Chorot_Mapping_SDF_V3,'Final_Chorot_Profile_SDF',mode='overwrite')

display(Final_Chorot_Mapping_SDF_V3)

# COMMAND ----------


# 1. Define the correct order of month abbreviations
month_levels <- c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


Final_Chorot_Mapping_SDF_V4 = Final_Chorot_Mapping_SDF_V3 %>% dplyr::group_by(.,StartingMonthCohort2,Group_Profile_Users)%>% dplyr::summarise(.,dplyr::across((Jan:Dec),~sum(.x,na.rm=TRUE)))%>%
# dplyr::mutate(.,StartingMonthCohort3= factor(StartingMonthCohort2, levels = month_levels))%>%
dplyr::arrange(.,StartingMonthCohort2,Group_Profile_Users)


display(Final_Chorot_Mapping_SDF_V4)



# COMMAND ----------



# COMMAND ----------


# ===== Q1 =======


Final_Chorot_Mapping_Q1 = Final_Chorot_Mapping_v1 %>% 
dplyr::mutate(.,Qtr1=dplyr::case_when((Jan==1|Feb==1|Mar==1)~1,TRUE~0))%>%
dplyr::filter(.,Qtr1==1)

display(Final_Chorot_Mapping_Q1 %>% dplyr::summarise(.,TOTALCNT = dplyr::n_distinct(Customer_External_ID)))

Final_Chorot_Mapping_Q1 = Final_Chorot_Mapping_Q1 %>% as.data.frame()

# COMMAND ----------

# storage_write_csv(Final_Chorot_Mapping_Q1, cont, "Final_Chorot_Mapping_Q1.csv")

# COMMAND ----------

Final_Chorot_Mapping_Q2 = Final_Chorot_Mapping_v1 %>% 
dplyr::mutate(.,Qtr2=dplyr::case_when((Apr==1|May==1|Jun==1)~1,TRUE~0))%>%
dplyr::filter(.,Qtr2==1)

display(Final_Chorot_Mapping_Q2 %>% dplyr::summarise(.,TOTALCNT = dplyr::n_distinct(Customer_External_ID)))


Final_Chorot_Mapping_Q2 = Final_Chorot_Mapping_Q2 %>% as.data.frame()

# COMMAND ----------

# storage_write_csv(Final_Chorot_Mapping_Q2, cont, "Final_Chorot_Mapping_Q2.csv")

# COMMAND ----------

Final_Chorot_Mapping_Q3 = Final_Chorot_Mapping_v1 %>% 
dplyr::mutate(.,Qtr3=dplyr::case_when((Jul==1|Aug==1|Sep==1)~1,TRUE~0))%>%
dplyr::filter(.,Qtr3==1)

display(Final_Chorot_Mapping_Q3 %>% dplyr::summarise(.,TOTALCNT = dplyr::n_distinct(Customer_External_ID)))

Final_Chorot_Mapping_Q3 = Final_Chorot_Mapping_Q3 %>% as.data.frame()

# COMMAND ----------

# storage_write_csv(Final_Chorot_Mapping_Q3, cont, "Final_Chorot_Mapping_Q3.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Q1 TO Q2

# COMMAND ----------


# ====== Customer who are present in both Q1 & Q2 

Final_Chorot_Mapping_Q1_1 = Final_Chorot_Mapping_Q1 %>% dplyr::select('Customer_External_ID')%>% dplyr::distinct()

Final_Chorot_Mapping_Q2_1 = Final_Chorot_Mapping_Q2 %>% dplyr::select('Customer_External_ID')%>% dplyr::distinct()


Q1_Q2_Present_InBoth = dplyr::inner_join(Final_Chorot_Mapping_Q1_1,Final_Chorot_Mapping_Q2_1,by=c('Customer_External_ID') ,copy='TRUE')

Q1_Q2_Present_InBoth_SDF = dplyr::copy_to(sc,Q1_Q2_Present_InBoth,'Q1_Q2_Present_InBoth')

sdf_register(Q1_Q2_Present_InBoth_SDF,'Q1_Q2_Present_InBoth_SDF')

display(Q1_Q2_Present_InBoth_SDF %>% dplyr::summarise(.,TotalCNT = dplyr::n(),.groups='drop'))

# sparklyr::spark_write_table(Q1_Q2_Present_InBoth_SDF,'Q1_Q2_Present_InBoth_SDF',mode='overwrite')

# storage_write_csv(Q1_Q2, cont, "Q1_Q2.csv")


# COMMAND ----------


# ====== Customer who are present in both Q2 & Q3

# Final_Chorot_Mapping_Q2_1 = Final_Chorot_Mapping_Q2 %>% dplyr::select('Customer_External_ID')%>% dplyr::distinct()

Final_Chorot_Mapping_Q3_1 = Final_Chorot_Mapping_Q3 %>% dplyr::select('Customer_External_ID')%>% dplyr::distinct()


Q2_Q3_Present_Both = dplyr::inner_join(Q1_Q2_Present_InBoth,Final_Chorot_Mapping_Q3_1,by=c('Customer_External_ID') ,copy='TRUE')

Q2_Q3_Present_Both_SDF = dplyr::copy_to(sc,Q2_Q3_Present_Both,'Q2_Q3_Present_Both')

sdf_register(Q2_Q3_Present_Both_SDF,'Q2_Q3_Present_Both_SDF')

# sparklyr::spark_write_table(Q2_Q3_Present_Both_SDF,'Q2_Q3_Present_Both_SDF',mode='overwrite')

display(Q2_Q3_Present_Both_SDF %>% dplyr::summarise(.,TotalCNT = dplyr::n(),.groups='drop'))

# storage_write_csv(Q2_Q3, cont, "Q2_Q3.csv")

# COMMAND ----------

#----- Present only in Q1 not in Q2 


Final_Chorot_Mapping_Q1_1 = Final_Chorot_Mapping_Q1 %>% dplyr::select('Customer_External_ID')%>% dplyr::distinct()

Final_Chorot_Mapping_Q2_1 = Final_Chorot_Mapping_Q2 %>% dplyr::select('Customer_External_ID')%>% dplyr::distinct()


Present_only_in_Q1 = dplyr::anti_join(Final_Chorot_Mapping_Q1_1,Final_Chorot_Mapping_Q2_1,by=c('Customer_External_ID') ,copy='TRUE')

Present_only_in_Q1_SDF = dplyr::copy_to(sc,Present_only_in_Q1,'Present_only_in_Q1')

sdf_register(Present_only_in_Q1_SDF,'Present_only_in_Q1_SDF')

# sparklyr::spark_write_table(Present_only_in_Q1_SDF,'Present_only_in_Q1_SDF',mode='overwrite')

display(Present_only_in_Q1_SDF %>% dplyr::summarise(.,TotalCNT = dplyr::n(),.groups='drop'))

# storage_write_csv(Q1_Q2, cont, "Q1_Q2.csv")


# COMMAND ----------




# ====== Customer who are present in only in Q2 & not in Q3

Final_Chorot_Mapping_Q2_1 = Final_Chorot_Mapping_Q2 %>% dplyr::select('Customer_External_ID')%>% dplyr::distinct()

Final_Chorot_Mapping_Q3_1 = Final_Chorot_Mapping_Q3 %>% dplyr::select('Customer_External_ID')%>% dplyr::distinct()


Present_only_in_Q2 = dplyr::anti_join(Final_Chorot_Mapping_Q2_1,Final_Chorot_Mapping_Q3_1,by=c('Customer_External_ID') ,copy='TRUE')

Present_only_in_Q2_SDF = dplyr::copy_to(sc,Present_only_in_Q2,'Present_only_in_Q2')

sdf_register(Present_only_in_Q2_SDF,'Present_only_in_Q2_SDF')

display(Present_only_in_Q2_SDF %>% dplyr::summarise(.,TotalCNT = dplyr::n(),.groups='drop'))

sparklyr::spark_write_table(Present_only_in_Q2_SDF,'Present_only_in_Q2_SDF',mode='overwrite')


# storage_write_csv(Q2_Q3, cont, "Q2_Q3.csv")

# COMMAND ----------

# Final_Chorot_Mapping_Q1_1 = Final_Chorot_Mapping_Q1 %>% dplyr::select('Customer_External_ID')%>% dplyr::distinct()

# Final_Chorot_Mapping_Q3_1 = Final_Chorot_Mapping_Q3 %>% dplyr::select('Customer_External_ID')%>% dplyr::distinct()


# Q1_Q3 = dplyr::inner_join(Final_Chorot_Mapping_Q1_1,Final_Chorot_Mapping_Q3_1,by=c('Customer_External_ID') ,copy='TRUE')

# display(Q1_Q3 %>% dplyr::summarise(.,TotalCNT = dplyr::n(),.groups='drop'))

# storage_write_csv(Q1_Q3, cont, "Q2_Q3.csv")