library(sparklyr)
library(dplyr)
library(knitr)
library(rmarkdown)
library(pandoc)

# Install
spark_install()

conf <- spark_config()
conf["spark.executor.memory"] <- "6G"
conf["sparklyr.shell.driver-memory"] <- "6G"
sc <- spark_connect(master = "local", config = conf)

knitr::opts_knit$set(root.dir = file.path(getwd()))

ds.path <- "~/FAX/RVPUII/projekat/data"
ds.df <- spark_read_csv(sc, name = "my_data", path = ds.path, header = TRUE, infer_schema = TRUE, memory = TRUE)
ds.filtered <- ds.df %>% 
    select(-airport_fee, -congestion_surcharge, ~store_and_fwd_flag) %>%
    filter(
      !is.na(VendorID) || !is.na(tpep_pickup_datetime) || !is.na(tpep_dropoff_datetime) || !is.na(passenger_count) || 
      !is.na(trip_distance) || !is.na(RatecodeID) || !is.na(PULocationID) || !is.na(DOLocationID) ||
      !is.na(payment_type) || !is.na(fare_amount) || !is.na(extra) || !is.na(mta_tax) || !is.na(tip_amount) || 
      !is.na(tolls_amount) || !is.na(improvement_surcharge) || !is.na(total_amount)
    )

data <- ds.filtered %>% 
    mutate(received_tip = ifelse(tip_amount > 0, T, F)) %>%
    mutate(payed_with_card = ifelse(payment_type == 1, T, F))

ds <- NULL

# Every attribute represented
VendorIDs <- pull(data, VendorID)

# Classification models
model.first <- tip_amount ~ trip_distance + passenger_count + total_amount + VendorID + RatecodeID
model.second <- tip_amount ~ trip_distance + payment_type + mta_tax + improvement_surcharge
model.third <- tip_amount ~ tpep_pickup_datetime + tpep_dropoff_datetime + passenger_count + RatecodeID + store_and_fwd_flag

# Linear regression
classification.lr.first <- lm(model.first, data=ds.filtered)
summary(classification.lr.first)
predictions.lr.first <- predict(classification.lr.first, select(ds.filtered[1,], trip_distance, passenger_count, total_amount, VendorID, RatecodeID))
predictions.lr.first


classification.lr.second <- ds.data$train %>%
    ml_linear_regression(model.second)

classification.lr.third <- ds.data$train %>%
    ml_linear_regression(model.third)


predictions.lr.second <- classification.lr.second %>%
    ml_predi2ct(ds.data$test)

predictions.lr.third <- classification.lr.third %>%
    ml_predict(ds.data$test)

predicitions.lr.first
predicitions.lr.second
predicitions.lr.third

spark_disconnect(sc)
