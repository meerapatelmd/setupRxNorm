# QA extracted nodes, edges, and concept_ancestors
library(tidyverse)


class_types =
  c(
    "MESHPA",
    "EPC",
    "MOA",
    "PE",
    "PK",
    "TC",
    "VA",
    "DISEASE",
    "DISPOS",
    "CHEM",
    "SCHEDULE",
    "STRUCT")


for (class_type in class_types) {

graph_data <-
list(
  node = file.path("/Users/meerapatel/GitHub/packages/setupRxNorm/inst/RxClass API/07-Feb-2022/extracted/graph/raw", class_type,  "node.csv"),
  edge = file.path("/Users/meerapatel/GitHub/packages/setupRxNorm/inst/RxClass API/07-Feb-2022/extracted/graph/raw", class_type,  "edge.csv"),
  concept_ancestor = file.path("/Users/meerapatel/GitHub/packages/setupRxNorm/inst/RxClass API/07-Feb-2022/extracted/graph/processed",  class_type, "concept_ancestor.csv")) %>%
  map(broca::simply_read_csv)


# DISPOS returns no rows for nodes and edges
if (nrow(graph_data$node)==0 & nrow(graph_data$edge)==0) {

  cli::cli_alert("{.var class_type} {.emph {class_type}} returned 0 nodes and edges.")
  break

}

# Check if there are any NAs in edge
na_edge_values <-
graph_data$edge %>%
  dplyr::filter_all(any_vars(is.na(.)))

if (nrow(na_edge_values)>0) {

  cli::cli_alert("{.var class_type} {.emph {class_type}} has some NA values in edges.")

  print(na_edge_values)

  secretary::press_enter()


}

node_check <-
list(
graph_data$node %>%
  distinct(classId) %>%
  transmute(node_in_node_csv = classId,
            classId),
graph_data$edge %>%
  distinct(classId1) %>%
  transmute(node1_in_edge_csv = classId1,
            classId = classId1),
graph_data$edge %>%
  distinct(classId2) %>%
  transmute(node2_in_edge_csv = classId2,
            classId = classId2),
graph_data$concept_ancestor %>%
  distinct(ancestor_concept_code) %>%
  transmute(ancestor_in_ca_csv = ancestor_concept_code,
            classId = ancestor_concept_code),
graph_data$concept_ancestor %>%
  distinct(descendant_concept_code) %>%
  transmute(descendant_in_ca_csv = descendant_concept_code,
            classId = descendant_concept_code)) %>%
  reduce(full_join, by = "classId") %>%
  transmute(
    all_classId = classId,
    node_in_node_csv,
    node1_in_edge_csv,
    node2_in_edge_csv,
    ancestor_in_ca_csv,
    descendant_in_ca_csv
  )

node_check <-
node_check %>%
  rowid_to_column("rowid")


# 1. Are there any nodes in node.csv that are not found in edge.csv (1 or 2)
# Expected results has 0 rows
check_1 <-
node_check %>%
  transmute(all_classId,
            node_in_node_csv,
            node_in_edge_csv =
              coalesce(node1_in_edge_csv,
                       node2_in_edge_csv)) %>%
              distinct() %>%
  dplyr::filter_at(vars(!all_classId),
                   any_vars(is.na(.)))

if (nrow(check_1)>0) {

  cli::cli_alert_info("Some nodes missing in edge.csv")

  secretary::press_enter()

}

# 2. Are there any nodes in edge.csv that are not found in concept_ancestor?
check_2 <-
  node_check %>%
  transmute(all_classId,
            node_in_edge_csv =
              coalesce(node1_in_edge_csv,
                       node2_in_edge_csv),
            node_in_ca_csv =
              coalesce(ancestor_in_ca_csv,
                       descendant_in_ca_csv)) %>%
  distinct() %>%
  dplyr::filter_at(vars(!all_classId),
                   any_vars(is.na(.)))


if (nrow(check_2)>0) {

  cli::cli_alert_info("Some nodes missing in concept_ancestor.csv")

  secretary::press_enter()

}


node_check_results <-
left_join(
node_check,
node_check %>%
  tidyr::pivot_longer(cols = !rowid,
                      values_drop_na = FALSE) %>%
  group_by(rowid) %>%
  summarize(row_na_count = length(value[is.na(value)])),
by = "rowid") %>%
  mutate(
    node_class_type =
      case_when(
        is.na(node1_in_edge_csv) & !is.na(node2_in_edge_csv) ~ "Root",
        !is.na(node1_in_edge_csv) & is.na(node2_in_edge_csv) ~ "Leaf",
        !is.na(node1_in_edge_csv) & !is.na(node2_in_edge_csv) ~ "SubClass",
        TRUE ~ "(Node Not Found in Edge)")) %>%
  select(-rowid) %>%
  distinct()

# Counts
node_check_results_sum <-
node_check_results %>%
  count(row_na_count,
        node_class_type) %>%
  mutate(class_type = class_type)


View(node_check_results_sum)


secretary::press_enter()

}



