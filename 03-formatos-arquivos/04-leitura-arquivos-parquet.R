library(sparklyr)
library(dbplyr)
library(readr)
library(dplyr)
library(stringr)
library(purrr)
library(DBI)
library(sparklyr.nested)

Sys.setenv(SPARK_HOME="/usr/lib/spark")
sc <- spark_connect(master = "yarn-client", version = "2.3.1")

ratings_music <- spark_read_parquet(sc, name = "ratings_music_parquet", path = "subset_meta_Digital_Music.parquet", memory = FALSE)

ratings_music %>%
  select(asin, description, imUrl, price, title) %>%
  show_query()

ratings_music %>%
  select(asin, description, imUrl, price, title)

ratings_music %>%
  select(asin, description, imUrl, price, title) %>%
  filter(price > 15)

ratings_music %>%
  sdf_select(asin, related$bought_together) %>%
  sdf_explode(bought_together) %>%
  arrange(asin) %>%
  count(asin, sort = TRUE)

ratings_music %>%
  sdf_select(asin, related$bought_together) %>%
  sdf_schema()

ratings_music %>%
  sdf_select(asin, related$bought_together) %>%
  show_query()

ratings_music %>%
  sdf_select(asin, related$bought_together) %>%
  sdf_explode(bought_together) %>%
  sdf_schema()

spark_disconnect_all()
