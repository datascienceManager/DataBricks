library(dplyr)
library(data.table)

df = fread("Data - Sheet1.csv")


library(dplyr)
library(lubridate)

df_winback <- df %>%
  mutate(
    StartDate = mdy(StartDate),
    ExpD      = mdy(ExpD)
  ) %>%
  group_by(ID) %>%
  mutate(
    prev_max_ExpD = as.Date(
      lag(cummax(as.numeric(ExpD))),
      origin = "1970-01-01"
    ),
    `Diff between StartD-ExpD` = if_else(
      !is.na(prev_max_ExpD) & StartDate > prev_max_ExpD,
      as.integer(StartDate - prev_max_ExpD),
      NA_integer_
    )
  ) %>%
  ungroup()



#------ Sparklyr 

library(dplyr)
library(sparklyr)

df_winback <- df %>%
  # 1. Create row order (VERY IMPORTANT)
  mutate(row_id = monotonically_increasing_id()) %>%
  
  # 2. Convert to Date
  mutate(
    StartDate = to_date(StartDate, "MM/dd/yyyy"),
    ExpD      = to_date(ExpD, "MM/dd/yyyy")
  ) %>%
  
  group_by(ID) %>%
  
  # 3. Preserve original row order
  arrange(row_id, .by_group = TRUE) %>%
  
  # 4. Compute previous max ExpD
  mutate(
    prev_max_ExpD = lag(max(ExpD) %>% over(
      partition_by(ID),
      order_by(row_id),
      frame = c(-Inf, -1)
    )),
    
    `Diff between StartD-ExpD` = if_else(
      !is.na(prev_max_ExpD) & StartDate > prev_max_ExpD,
      datediff(StartDate, prev_max_ExpD),
      as.integer(NA)
    )
  ) %>%
  ungroup()


