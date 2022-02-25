SET search_path TO {schema};

DROP TABLE IF EXISTS lookup;
CREATE TABLE lookup (
  class_type   varchar(20) NOT NULL,
  rela_sources varchar(25) NOT NULL,
  version_class_type varchar(1),
  version_rela_sources varchar(1)
)
;

COPY lookup FROM '{lookup_csv}'  CSV HEADER QUOTE '"';


DROP TABLE IF EXISTS concept;

CREATE TABLE concept (
  vocabulary_id varchar(20) NOT NULL,
  concept_code varchar(25) NOT NULL,
  concept_name varchar(255) NOT NULL,
  concept_class varchar(10) NOT NULL
)
;

COPY concept FROM '{concept_csv}' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b';

DROP TABLE IF EXISTS concept_relationship;
CREATE TABLE concept_relationship (
  concept_code_1 varchar(25) NOT NULL,
  relationship_id varchar(10) NOT NULL,
  relationship_source varchar(20) NOT NULL,
  relationship_type varchar(8) NOT NULL,
  concept_code_2 varchar(25) NOT NULL
)
;

COPY concept_relationship FROM '{concept_relationship_csv}'  CSV HEADER QUOTE '"';

DROP TABLE IF EXISTS concept_ancestor;
CREATE TABLE concept_ancestor (
  ancestor_concept_code varchar(25) NOT NULL,
  descendant_concept_code varchar(25) NOT NULL,
  min_levels_of_separation integer NOT NULL,
  max_levels_of_separation integer NOT NULL
)
;

COPY concept_ancestor FROM '{concept_ancestor_csv}'  CSV HEADER QUOTE '"';


