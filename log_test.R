library(tidyverse)
getwd()
df = readLines('python_file/stock_log.txt')
df <- df %>% 
  as_tibble()

df <- df %>% 
  filter(str_detect(value, "_"))

df <- df %>% 
  separate(value, into = c('a','b','type', 'ticker', 'name', 'status') , sep = '_')

df %>% 
  filter(status == 'fail')

df$ticker %>% unique() %>% length()
