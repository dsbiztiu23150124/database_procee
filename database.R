if (!require(duckdb)) install.packages("duckdb")
# インメモリデータベースに接続
#con <- dbConnect(duckdb())

# データベースに接続
# (DBを他のプロセスと共有するときは、read_only = TRUEにし書き込みを禁止する。)
con <- dbConnect(duckdb(), dbdir = 'a.duckdb', read_only = FALSE)

# データベースにあるテーブルを表示
dbListTables(con)





# 入力するデータを作成
d <- data.frame(name   = c('Taro', 'Jiro'),
                salary = c(600, 550))

# 既にテーブルが存在している場合は削除
#if ( dbExistsTable(con, 'items') ) dbRemoveTable(con, 'items')

# データベースにテーブルを作成
#【オプション】追記モード： append = T、#上書きモード： overwrite = T
dbWriteTable(con, 'items', d, append = T)

# テーブルからデータを取得
res <- dbGetQuery(con, "SELECT * FROM items")

dbDisconnect(con, shutdown = TRUE)

# 取得したデータを表示
# 本コードでは追記モードにしているのでプログラム実行のたびに同じレコード追加される。
print(res)





if (!require(nycflights13)) install.packages("nycflights13")
data("flights", package = "nycflights13") # データの取得





con <- dbConnect(duckdb()) # インメモリデータベースを作成、接続
duckdb_register(con, "flights", flights) # filightsを紐付け（DuckDBのテーブルとして扱う）

res <- dbGetQuery(con,
                  'SELECT origin, dest, n
  FROM (
    SELECT q01.*, RANK() OVER (PARTITION BY origin ORDER BY n DESC) AS col01
    FROM (
      SELECT origin, dest, COUNT(*) AS n
      FROM flights
      GROUP BY origin, dest
    ) q01
  ) q01
  WHERE (col01 <= 3) ORDER BY origin')

duckdb_unregister(con, "flights")  # fligthtsの紐付け解除
dbDisconnect(con, shutdown = TRUE) # データベースの接続解除
print(res) # 結果表示





if (!require(tidyverse)) install.packages("tidyverse")
con <- dbConnect(duckdb()) # インメモリデータベースを作成、接続
duckdb_register(con, "flights", flights) # filightsを紐付け（DuckDBのテーブルとして扱う）

# DuckDBライブラリの機能でクエリを表示（show_query）できる。
#tbl(con, 'flights') |> group_by(origin) |> count(dest) |> slice_max(n, n = 3) |> arrange(origin) |> show_query()
tbl(con, 'flights') |> 
  group_by(origin) |> 
  count(dest) |>
  slice_max(n, n = 3) |> 
  arrange(origin) -> res

print(res) # 結果表示





res |> collect() |> as.data.frame() -> d.out # Rオブジェクトにするときはcollect関数を使う。
duckdb_unregister(con, "flights")  # fligthtsの紐付け解除
dbDisconnect(con, shutdown = TRUE) # データベースの接続解除




#----------------------------------------------------------------------------------------------------
library(tidyverse)

d <- data.frame(
  name = c("太郎", "花子", "三郎", "良子", "次郎", "桜子", "四郎", "松子", "愛子"),
  school = c("南", "南", "南", "南", "南", "東", "東", "東", "東"),
  teacher = c("竹田", "竹田", "竹田", "竹田",  "佐藤", "佐藤", "佐藤", "鈴木", "鈴木"),
  gender = c("男", "女", "男", "女", "男", "女", "男", "女", "女"),
  math = c(4, 3, 2, 4, 3, 4, 5, 4, 5),
  reading = c(1, 5, 2, 4, 5, 4, 1, 5, 4) )
d



#1
d |> select(name, math)


#2
d |> select(-gender)


#3
d |> slice(3:6)


#4
d |> arrange(name)


#5
d |> arrange(desc(math))


#6
d |> arrange(desc(math), desc(reading))


#7
d |> select(name, reading)


#8
d |> summarise(mean_math = mean(math))


#9
d |> group_by(teacher) |> summarise(mean_math = mean(math))


#10
d |> filter(gender == "女") |> select(name, math)


#11
d |> filter(school == "南", gender == "男") |> select(name, reading)


#12
d |> group_by(teacher) |> filter(n() >= 3)


#13
d |> mutate(total = math + reading)


#14
d |> mutate(math100 = math * (100 / 5))