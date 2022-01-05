CREATE OR REPLACE FUNCTION setup_rxnorm_concept_update() RETURNS void AS '
library(setupRxNorm)
process_rxnorm_concept_update()
' LANGUAGE plr;


SELECT setup_rxnorm_concept_update();

