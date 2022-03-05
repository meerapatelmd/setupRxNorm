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

tibble::tribble(
  ~`classType`, ~`vocabulary_id`,
  'ATC1-4'    , 'ATC'           ,
  'CHEM'      , 'MeSH'          ,
  'DISEASE'   , 'MeSH'          ,
  'DISPOS',   , 'SNOMED'        ,
  'EPC'       , 'MEDRT'         ,
  'MESHPA'    , 'MeSH'          ,
  'MOA'       , 'MEDRT'         ,
  'PE'        , 'MEDRT'         ,
  'PK'        , 'MEDRT'         ,
  'SCHEDULE'  , NA_character_   ,
  'STRUCT'    , 'SNOMED'        ,
  'TC'        , 'MEDRT'         ,
  'VA'        , 'NDFRT'
)

rela_vocabulary_lookup <-
  tibble::tribble(
    ~`relaSources`, ~`omop_vocabulary_id`, ~`custom_vocabulary_id`,
    'ATC'         , 'ATC'                ,
    'DAILYMED'    ,  NA_character_       , "DailyMed",
    'FDASPL'      , 'SPL'                ,
    'MEDRT'       , 'MEDRT'              ,
    'SNOMEDCT'    , 'SNOMED'             ,
    'MESH'        , 'MeSH'               ,
    'RXNORM'      , 'RxNorm'             ,
    'FMTSME'      , NA_character_        ,
    'VA'          , NA_character_
  )



relationship_source_dictionary <-
tibble::tribble(
  ~`relationship_source`, ~`relaSourcesDefinition`,
  'ATC', 'The fifth level of the ATC tree contains the drugs.',
  'DailyMed','The DailyMed API maps substances through their UNII Code values to EPC, Chem, MoA and PE classes.',
  'FDASPL','The FDASPL drug source maps drug concepts in MED-RT to EPC, Chem, MoA and PE classes.',
  'FMTSME','The FMTSME drug source maps drug concepts in MED-RT to TC classes.',
  'MEDRT','Drug concepts in MED-RT are associated to Disease, MoA, Chem, PE, and PK classes.',
  'MeSH','MeSH drugs are associated with the pharmacological actions.',
  'RxNorm','CSA schedules for drug products are contained in the RxNorm data set in several drug sources.',
  'SNOMEDCT','Drug classes (groupers) from the medicinal product hierarchy of SNOMED CT, linked to medicinal products (as RxNorm ingredients).',
  'VA','The VA drugs are associated with the VA classes.'
)
