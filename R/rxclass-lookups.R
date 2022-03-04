get_lookup <-
  function() {
  tibble::tribble(
    ~classType, ~relaSources,
    "ATC1-4", "ATC",
    "CHEM", "DAILYMED",
    "CHEM", "FDASPL",
    "CHEM", "MEDRT",
    "DISEASE", "MEDRT",
    "DISPOS", "SNOMEDCT",
    "EPC", "DAILYMED",
    "EPC", "FDASPL",
    "MESHPA", "MESH",
    "MOA", "DAILYMED",
    "MOA", "FDASPL",
    "MOA", "MEDRT",
    "PE", "DAILYMED",
    "PE", "FDASPL",
    "PE", "MEDRT",
    "PK", "MEDRT",
    "SCHEDULE", "RXNORM",
    "STRUCT", "SNOMEDCT",
    "TC", "FMTSME",
    "VA", "VA")
}

omop_lookup <-
  tibble::tribble(
    ~`relaSources`, ~`relationship_vocabulary_id`,
    'ATC'         , 'ATC',
    'DAILYMED'    ,  NA_character_,
    'FDASPL'      , 'SPL',
    'MEDRT'       , 'MEDRT',
    'SNOMEDCT'    , 'SNOMED',
    'MESH'        , 'MeSH',
    'RXNORM'      , 'RxNorm',
    'FMTSME'      , NA_character_,
    'VA'          , NA_character_
  )
