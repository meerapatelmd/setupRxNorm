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


DROP TABLE IF EXISTS tmp_concept;

CREATE TABLE tmp_concept (
  concept_code varchar(25) NOT NULL,
  concept_name varchar(255) NOT NULL,
  class_type varchar(10) NOT NULL,
  concept_class_id varchar(25) NULL,
  standard_concept varchar(1) NULL,
  vocabulary_id varchar(20) NOT NULL
)
;

COPY tmp_concept FROM '{concept_csv}' CSV HEADER NULL AS 'NA' QUOTE '"';


DROP TABLE IF EXISTS concept;

CREATE TABLE concept (
  concept_code varchar(25) NOT NULL,
  concept_name varchar(255) NOT NULL,
  class_type varchar(10) NOT NULL,
  concept_class_id varchar(25) NULL,
  standard_concept varchar(1) NULL,
  vocabulary_id varchar(20) NOT NULL
)
;

WITH dupes AS (
select
  c2.*
from tmp_concept c
inner join tmp_concept c2
ON
  c2.concept_code = c.concept_code
  AND c2.concept_name = c.concept_name
where
  c.vocabulary_id = 'RxNorm'
  AND c.class_type = c2.class_type
  AND c.vocabulary_id <> c2.vocabulary_id
)

INSERT INTO concept
SELECT DISTINCT
  c.concept_code,
  c.concept_name,
  c.class_type,
  c.concept_class_id,
  c.standard_concept,
  c.vocabulary_id
FROM tmp_concept c
LEFT JOIN dupes d
ON
  c.concept_code = d.concept_code
  AND c.concept_name = d.concept_name
  AND c.class_type = d.class_type
  AND c.concept_class_id = d.concept_class_id
  AND c.vocabulary_id = d.vocabulary_id
WHERE d.concept_code IS NULL
;


DROP TABLE IF EXISTS concept_synonym;

CREATE TABLE concept_synonym (
  concept_code varchar(25) NOT NULL,
  concept_synonym_name varchar(255) NOT NULL,
  class_type varchar(10) NOT NULL
)
;

COPY concept_synonym FROM '{concept_synonym_csv}' CSV HEADER QUOTE '"';

DROP TABLE IF EXISTS concept_relationship;
CREATE TABLE concept_relationship (
  concept_code_1 varchar(25) NOT NULL,
  class_type_1 varchar(10) NOT NULL,
  relationship_id varchar(15) NOT NULL,
  relationship_source varchar(20) NOT NULL,
  relationship_type varchar(8),
  concept_code_2 varchar(25) NOT NULL,
  class_type_2 varchar(10) NOT NULL
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
  subsumes_ct integer,
  mapping_ct integer
)
;

WITH c AS (
SELECT c.class_type, c.vocabulary_id, cr.relationship_source, COUNT(*) AS subsumes_ct
FROM concept c
INNER JOIN (SELECT * FROM concept_relationship WHERE relationship_id NOT IN ('Mapped from', 'Maps to')) cr
ON cr.concept_code_1 = c.concept_code
GROUP BY c.class_type, c.vocabulary_id, cr.relationship_source
),
r AS (
SELECT c.class_type, c.vocabulary_id, cr.relationship_source, COUNT(*) AS mapped_ct
FROM concept c
INNER JOIN (SELECT * FROM concept_relationship WHERE relationship_id IN ('Mapped from', 'Maps to')) cr
ON cr.concept_code_1 = c.concept_code
GROUP BY c.class_type, c.vocabulary_id, cr.relationship_source
),
fin AS (
SELECT
  tmp_lookup.*,
  c.vocabulary_id,
  c.subsumes_ct,
  r.mapped_ct
FROM tmp_lookup
LEFT JOIN c
ON c.class_type = tmp_lookup.class_type AND c.relationship_source = tmp_lookup.rela_sources
LEFT JOIN r
ON r.class_type = c.class_type AND r.relationship_source = c.relationship_source AND r.vocabulary_id = c.vocabulary_id
)

INSERT INTO lookup SELECT * FROM fin ORDER BY class_type, rela_sources
;


DROP TABLE tmp_lookup;
DROP TABLE tmp_concept;


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
