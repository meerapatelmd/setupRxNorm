library(tidyverse)

sql_statement <-
"
create schema if not exists rxrel;

DROP TABLE IF EXISTS rxrel.tmp_rxnorm_validity_lookup0;
CREATE TABLE rxrel.tmp_rxnorm_validity_lookup0 AS (
  select distinct
  COALESCE(r.rxcui, arc.rxcui) AS rxcui_lookup,
  COALESCE(r.sab, arc.sab) AS rxcui_sab_lookup,
  COALESCE(r.tty, arc.tty) AS rxcui_tty_lookup,
  COALESCE(r.str, arc.str) AS rxcui_str_lookup,
  r.rxcui AS rxnconso_rxcui,
  arc.rxcui AS rxnatomarchive_rxcui,
  arc.merged_to_rxcui,
  COALESCE(arc.merged_to_rxcui, arc.rxcui, r.rxcui) AS maps_to_rxcui,
  CASE
  WHEN r.rxcui IS NOT NULL AND arc.rxcui IS NULL THEN 'Valid'
  WHEN r.rxcui IS NULL AND arc.rxcui IS NOT NULL AND arc.rxcui = arc.merged_to_rxcui THEN 'Deprecated'
  WHEN  r.rxcui IS NOT NULL AND r.rxcui = arc.rxcui AND arc.rxcui <> arc.merged_to_rxcui THEN 'Updated'
  WHEN r.rxcui IS NULL AND arc.rxcui IS NOT NULL AND arc.rxcui <> arc.merged_to_rxcui THEN 'Updated'
  WHEN r.rxcui = arc.rxcui AND arc.rxcui = arc.merged_to_rxcui THEN 'Valid'
  ELSE 'Invalid'
  END validity
  from rxnorm.rxnconso r
  full join rxnorm.rxnatomarchive arc
  on r.rxcui = arc.rxcui
)
;
"
pg13::send(conn_fun = "pg13::local_connect()",
           sql_statement = sql_statement)

sql_statement <-
  glue::glue(
    "
    DROP TABLE IF EXISTS rxrel.tmp_rxnorm_validity_lookup0;
    CREATE TABLE rxrel.tmp_rxnorm_validity_lookup0 AS (
    SELECT DISTINCT
      rxcui_lookup AS rxcui_lookup,
      maps_to_rxcui AS maps_to_rxcui,
      maps_to_rxcui AS maps_to_rxcui0,
      validity AS validity
    FROM rxrel.tmp_rxnorm_validity_lookup
    )
    "
  )

pg13::send(conn_fun = "pg13::local_connect()",
           sql_statement = sql_statement)

for (i in 1:10) {
sql_statement <-
  glue::glue(
    "
    DROP TABLE IF EXISTS rxrel.tmp_rxnorm_validity_lookup{i}_b;
    CREATE TABLE rxrel.tmp_rxnorm_validity_lookup{i}_b AS (
    SELECT DISTINCT
      '{i}' AS level_of_separation,
      a.maps_to_rxcui{i-1},
      b.validity     AS validity,
      b.maps_to_rxcui AS maps_to_rxcui{i}
    FROM rxrel.tmp_rxnorm_validity_lookup{i-1} a
    LEFT JOIN rxrel.tmp_rxnorm_validity_lookup0 b
    ON a.maps_to_rxcui{i-1} = b.rxcui_lookup
    WHERE a.validity = 'Updated' AND b.validity = 'Updated'
    );

    DROP TABLE IF EXISTS rxrel.tmp_rxnorm_validity_lookup{i};
    CREATE TABLE rxrel.tmp_rxnorm_validity_lookup{i} AS (
        SELECT *
        FROM rxrel.tmp_rxnorm_validity_lookup{i}_b b
        WHERE b.maps_to_rxcui{i-1} NOT IN (SELECT DISTINCT maps_to_rxcui{i} FROM rxrel.tmp_rxnorm_validity_lookup{i}_b)

    );

    DROP TABLE rxrel.tmp_rxnorm_validity_lookup{i}_b;
    "
  )

pg13::send(conn_fun = "pg13::local_connect()",
           sql_statement = sql_statement)

row_count <-
  pg13::query(sql_statement = glue::glue("SELECT COUNT(*) FROM rxrel.tmp_rxnorm_validity_lookup{i};")) %>%
  unlist() %>%
  unname()

if (row_count == 0) {
  pg13::send(conn_fun = "pg13::local_connect()",
             sql_statement = glue::glue("DROP TABLE rxrel.tmp_rxnorm_validity_lookup{i};"))

  final_tables <- sprintf("tmp_rxnorm_validity_lookup%s", 1:(i-1))

  output <-
    vector(mode = "list",
           length = length(final_tables))
  names(output) <- final_tables

  for (final_table in final_tables) {
    output[[final_table]] <-
      pg13::query(
        sql_statement = glue::glue("SELECT * FROM rxrel.{final_table};")
      )


  }


  output <-
    output %>%
    map(select, -level_of_separation) %>%
    reduce(left_join) %>%
    distinct()

  final_a <-
  output %>%
    pivot_longer(cols = matches("[1-9]{1,}$"),
                 names_to = "level_of_separation",
                 names_prefix = "maps_to_rxcui",
                 values_to = "maps_to_rxcui",
                 values_drop_na = TRUE)

  final_b <-
  final_a %>%
    group_by(maps_to_rxcui0) %>%
    dplyr::arrange(desc(level_of_separation),
                   .by_group = TRUE) %>%
    dplyr::filter(row_number() == 1) %>%
    ungroup() %>%
    arrange(desc(level_of_separation))


  pg13::write_table(conn_fun = "pg13::local_connect()",
                    schema = "rxrel",
                    table_name = "rxnorm_updated_path",
                    data = final_a,
                    drop_existing = TRUE)

  pg13::write_table(conn_fun = "pg13::local_connect()",
                    schema = "rxrel",
                    table_name = "rxnorm_updated",
                    data = final_b,
                    drop_existing = TRUE)




  for (final_table in final_tables) {

    pg13::drop_table(conn_fun = "pg13::local_connect()",
                     schema = "rxrel",
                     table = final_table)

  }
  break
}


}


sql_statement <-
"
DROP TABLE IF EXISTS rxrel.rxnorm_validity_lookup2;
create table rxrel.rxnorm_validity_lookup2 AS (
  select
  l.rxcui_lookup,
  l.rxcui_sab_lookup,
  l.rxcui_tty_lookup,
  l.rxcui_str_lookup,
  l.rxnconso_rxcui,
  l.rxnatomarchive_rxcui,
  l.merged_to_rxcui,
  COALESCE(u.maps_to_rxcui, l.maps_to_rxcui) AS maps_to_rxcui,
  COALESCE(u.validity, l.validity) AS validity,
  u.level_of_separation
  from rxrel.tmp_rxnorm_validity_lookup l
  left join rxrel.rxnorm_updated u
  ON u.maps_to_rxcui0 = l.maps_to_rxcui
)
;
"

pg13::send(conn_fun = "pg13::local_connect()",
           sql_statement = sql_statement)


sql_statement <-
"
DROP TABLE IF EXISTS rxrel.rxnorm_validity_lookup;
CREATE TABLE rxrel.rxnorm_validity_lookup AS (
SELECT DISTINCT
  l.*,
  r.rxcui_str_lookup AS maps_to_str,
  r.rxcui_sab_lookup AS maps_to_sab,
  r.rxcui_tty_lookup AS maps_to_tty
FROM rxrel.rxnorm_validity_lookup2 l
LEFT JOIN rxrel.rxnorm_validity_lookup2 r
ON r.rxcui_lookup = l.maps_to_rxcui
);
"

pg13::send(conn_fun = "pg13::local_connect()",
           sql_statement = sql_statement)

