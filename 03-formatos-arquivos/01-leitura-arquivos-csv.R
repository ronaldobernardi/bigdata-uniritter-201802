library(sparklyr)
library(dbplyr)
library(readr)
library(dplyr)
library(stringr)
library(purrr)

Sys.setenv(SPARK_HOME="/usr/lib/spark")
sc <- spark_connect(master = "yarn-client", version = "2.3.1")

ted_main <- spark_read_csv( sc, name = "ted_main", path = "/user/hadoop/ted_main.csv"
                            , header = TRUE, infer_schema = TRUE, memory = FALSE )

head(ted_main)

ted_main <- spark_read_csv( sc, name = "ted_main", path = "/user/hadoop/ted_main.csv"
                            , header = TRUE, infer_schema = TRUE, memory = FALSE, delimiter = ",", quote = '\\"', escape = '\\')

ted_main <- spark_read_csv( sc, name = "ted_main", path = "/user/hadoop/ted_main.csv"
                            , header = TRUE, infer_schema = TRUE, memory = FALSE
                            , columns = c( "comments", "description", "duration", "event", "film_date", "languages", "main_speaker", "name"
                                           , "num_speaker", "published_date", "speaker_occupation", "title", "url", "views" ))

local_ted_main <- read_csv("/home/hadoop/ted_main.csv")

local_ted_main %>%
  mutate(url = str_replace_all(url, "\n", "")) %>%
  mutate_if(is_character, ~ str_replace_all(.x, ",", " ")) %>%
  select(-ratings, -related_talks, -tags) -> prepared_ted_main

write_csv(prepared_ted_main, path = "~/ted_main_no_json.csv")

## NO TERMINAL, COPIAR O ARQUIVO PARA O HDFS

## AJUSTAR CAMINHO PARA GRUPO EM USO ##
ted_main <- spark_read_csv( sc, name = "ted_main_no_json", path = "/user/grupo6/ted_main_no_json.csv"
                            , header = TRUE, infer_schema = TRUE, memory = FALSE )

head(ted_main)

ted_main %>%
  mutate_at(vars(comments, duration, languages, published_date), as.integer) %>% 
  head()

spark_disconnect(sc)
