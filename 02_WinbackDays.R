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
