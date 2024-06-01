# install.packages("tidyverse")
#install.packages("sparklyr")
#install.packages("rmarkdown")
#install.packages("knitr")
#install.packages("pandoc")

library(sparklyr)
library(dplyr)
library(knitr)
library(rmarkdown)
library(pandoc)

# Install
spark_install()

# Connect
sc <- spark_connect(master = "local")


ds.path <- "~/FAX/RVPUII/projekat/data"
ds.df <- spark_read_csv(sc, name = "my_data", path = ds.path, header = TRUE, infer_schema = TRUE)
ds.filtered <- ds.df %>% 
    select(-airport_fee, -congestion_surcharge)  %>%
    filter_all(any_vars(is.na(.)))

# Training data
ds.data <- ds.filtered %>% sdf_partition(train = 0.8, test = 0.2, seed = 1099)

# Classification models
model.first <- tip_amount ~ trip_distance + passenger_count + total_amount + VendorID + RatecodeID
model.second <- tip_amount ~ trip_distance + payment_type + mta_tax + improvement_surcharge
model.third <- tip_amount ~ tpep_pickup_datetime + tpep_dropoff_datetime + passenger_count + RatecodeID + store_and_fwd_flag

# Linear regression
classification.lr.first <- ds.data$train %>%
    ml_linear_regression(model.first)

classification.lr.second <- ds.data$train %>%
    ml_linear_regression(model.second)

classification.lr.third <- ds.data$train %>%
    ml_linear_regression(model.third)

predictions.lr.first <- classification.lr.first %>%
    ml_predict(ds.data$test)

predictions.lr.second <- classification.lr.second %>%
    ml_predict(ds.data$test)

predictions.lr.third <- classification.lr.third %>%
    ml_predict(ds.data$test)

predicitions.lr.first
predicitions.lr.second
predicitions.lr.third

spark_disconnect(sc)
