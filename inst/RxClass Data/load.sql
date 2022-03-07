SET search_path TO {schema};

DROP TABLE IF EXISTS tmp_lookup;
CREATE TABLE tmp_lookup (
  class_type   varchar(20) NOT NULL,
  rela_sources varchar(25) NOT NULL,
  version_class_type varchar(1),
  version_rela_sources varchar(1)
)
;

COPY tmp_lookup FROM '{lookup_csv}'  CSV HEADER QUOTE '"';


DROP TABLE IF EXISTS concept;

CREATE TABLE concept (
  concept_code varchar(25) NOT NULL,
  concept_name varchar(255) NOT NULL,
  vocabulary_id varchar(20) NOT NULL,
  concept_class_id varchar(25) NOT NULL,
  standard_concept varchar(1) NULL
)
;

COPY concept FROM '{concept_csv}' CSV HEADER QUOTE '"';


DROP TABLE IF EXISTS concept_synonym;

CREATE TABLE concept_synonym (
  concept_code varchar(25) NOT NULL,
  concept_name varchar(255) NOT NULL,
  vocabulary_id varchar(20) NOT NULL,
  concept_class_id varchar(25) NOT NULL,
  standard_concept varchar(1) NULL
)
;

COPY concept FROM '{concept_synonym_csv}' CSV HEADER QUOTE '"';

DROP TABLE IF EXISTS concept_relationship;
CREATE TABLE concept_relationship (
  concept_code_1 varchar(25) NOT NULL,
  relationship_id varchar(15) NOT NULL,
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



DROP TABLE IF EXISTS lookup;
CREATE TABLE lookup (
  class_type   varchar(20) NOT NULL,
  rela_sources varchar(25) NOT NULL,
  version_class_type varchar(1),
  version_rela_sources varchar(1),
  vocabulary_id varchar(20),
  relationship_source varchar(25),
  subsumes_ct integer,
  mapping_ct integer
)
;

WITH c AS (
SELECT c.vocabulary_id, cr.relationship_source, COUNT(*) AS subsumes_ct
FROM concept c
INNER JOIN (SELECT * FROM concept_relationship WHERE relationship_id NOT IN ('Mapped from', 'Maps to')) cr
ON cr.concept_code_1 = c.concept_code
GROUP BY c.vocabulary_id, cr.relationship_source
),
r AS (
SELECT c.vocabulary_id, cr.relationship_source, COUNT(*) AS mapped_ct
FROM concept c
INNER JOIN (SELECT * FROM concept_relationship WHERE relationship_id IN ('Mapped from', 'Maps to')) cr
ON cr.concept_code_1 = c.concept_code
GROUP BY c.vocabulary_id, cr.relationship_source
),
fin AS (
SELECT
  tmp_lookup.*,
  c.*,
  r.mapped_ct
FROM tmp_lookup
LEFT JOIN c
ON c.vocabulary_id = tmp_lookup.class_type AND c.relationship_source = tmp_lookup.rela_sources
LEFT JOIN r
ON r.vocabulary_id = c.vocabulary_id AND r.relationship_source = c.relationship_source
)

INSERT INTO lookup SELECT * FROM fin ORDER BY class_type, rela_sources
;

DROP TABLE tmp_lookup;


CREATE TABLE IF NOT EXISTS {log_schema}.{log_table_name} (
    srr_datetime TIMESTAMP without time zone,
    setuprxnorm_rxclass_version varchar(5) NOT NULL,
    setuprxnorm_rxclass_dt varchar(25) NOT NULL,
    rxclass_version varchar(20) NOT NULL,
    rxclass_api_version varchar(20) NOT NULL
)
;

INSERT INTO {log_schema}.{log_table_name}
VALUES(
  '{Sys.time()}',
  '{version_number}',
  '{version_timestamp}',
  '{rxclass_version}',
  '{rxclass_api_version}'
)
;
