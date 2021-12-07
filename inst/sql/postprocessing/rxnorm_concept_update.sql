CREATE OR REPLACE FUNCTION setup_rxnorm_concept_update() RETURNS void AS '
library(setupRxNorm)
library(pg13)
conn <- local_connect()
process_rxnorm_concept_update(conn = conn)
dc(conn = conn)
' LANGUAGE plr;


SELECT setup_rxnorm_concept_update();

