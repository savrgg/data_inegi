
# 0) load libraries and dotenv --------------------------------------------

library(tidyverse)
library(AzureStor)
library(arrow)
library(dotenv)

load_dot_env()

url <- Sys.getenv("KEY_BLOB_STORAGE")
archivo <- Sys.getenv("FILE_DENUE")
key <- Sys.getenv("KEY_BLOB_STORAGE")
endpoint <- Sys.getenv("ENDPOINT_BLOB_STORAGE")
catalogo_archivos <- readxl::read_excel(archivo)

# 1) process data in denue ------------------------------------------------

data_inegi <- map(1:nrow(catalogo_archivos), function(row){
  # row <- 23
  register <- catalogo_archivos[row,]
  file_zip <- register$filename
  file_csv <- register$filename_csv
  periodo <- register$periodo
  actividad <- register$actividad
  
  file <- paste0(url, file_zip)
  download.file(file, "tmp.zip")
  unzip("tmp.zip")
  datos <- 
    read_csv(paste0("conjunto_de_datos/", file_csv), col_types = cols(.default = "c")) %>% 
    mutate(periodo = periodo, actividad = actividad)
  unlink("tmp.zip")
  unlink("conjunto_de_datos/", recursive = T) 
  unlink("diccionario_de_datos/", recursive = T)
  unlink("metadatos/", recursive = T)
  datos
})

data_inegi <- bind_rows(data_inegi)

data_inegi_1122 <- data_inegi %>% 
  filter(periodo == "2022-11")
write_parquet(data_inegi_1122, "data_denue_2211.parquet")

data_inegi_0522 <- data_inegi %>% 
  filter(periodo == "2022-05")
write_parquet(data_inegi_0522, "data_denue_2205.parquet")

data_inegi_1121 <- data_inegi %>% 
  filter(periodo == "2021-11")
write_parquet(data_inegi_1121, "data_denue_2111.parquet")

# 2) upload data to azure sa ----------------------------------------------

bl <- storage_endpoint(enpoint, key=key)
cont <- storage_container(bl, "inegidenue")

storage_upload(cont, "data_denue_2211.parquet", "data_denue_2211.parquet")
storage_upload(cont, "data_denue_2205.parquet", "data_denue_2205.parquet")
storage_upload(cont, "data_denue_2111.parquet", "data_denue_2111.parquet")

