CREATE SCHEMA IF NOT EXISTS rxrel;

CREATE TABLE IF NOT EXISTS public.setup_rxrel_log (
    srl_datetime timestamp without time zone,
    rxn_version character varying(255),
    target_table character varying(60),
    target_table_rows bigint
)
;

CREATE OR REPLACE FUNCTION check_if_rxnorm_requires_processing(target_table varchar)
RETURNS boolean
language plpgsql
AS
$$
DECLARE
  rxn_version varchar;
  log_rows int;
  requires_processing boolean;
BEGIN
  SELECT sr_release_date INTO rxn_version FROM public.setup_rxnorm_log WHERE sr_datetime IN (SELECT MAX(sr_datetime) FROM public.setup_rxnorm_log);
  EXECUTE format('SELECT COUNT(*) FROM public.setup_rxrel_log WHERE target_table = ''%s'' AND rxn_version = ''%s'';', target_table, rxn_version) INTO log_rows;

  IF log_rows = 0
    THEN requires_processing := TRUE;
    ELSE requires_processing := FALSE;
  END IF;

  RETURN requires_processing;

END;
$$
;

DO
$$
DECLARE
  requires_processing boolean;
  srl_datetime timestamp;
  rxn_version varchar;
  target_table varchar := 'rxnorm_to_brand_and_generic';
  target_table_rows bigint;
BEGIN
  SELECT check_if_rxnorm_requires_processing('rxnorm_to_brand_and_generic')
  INTO requires_processing;

  IF requires_processing THEN
    DROP TABLE IF EXISTS rxrel.rxnorm_to_brand_and_generic;
    CREATE TABLE rxrel.rxnorm_to_brand_and_generic (
        code varchar(100),
        code_system varchar(25),
        term text,
        brand_code varchar(100),
        brand_name text,
        brand_vocabulary varchar(25),
        generic_code varchar(100),
        generic_name text,
        generic_vocabulary varchar(25)
    );
    SET search_path TO rxnorm;
    with bn_to_min as (
        select distinct brand.str   brand_str,
                        brand.rxcui brand_rxcui,
                        brand.tty   brand_tty,
                        mixed.tty   generic_tty,
                        mixed.rxcui generic_rxcui,
                        mixed.str   generic_str
        from rxnconso brand
                 inner join rxnrel bn_sbd on brand.rxcui = bn_sbd.rxcui1 and bn_sbd.rela = 'has_ingredient'
                 inner join rxnrel sbd_scd on bn_sbd.rxcui2 = sbd_scd.rxcui1 and sbd_scd.rela = 'has_tradename'
                 inner join rxnrel scd_min on sbd_scd.rxcui2 = scd_min.rxcui1 and scd_min.rela = 'ingredients_of'
                 inner join rxnconso mixed on scd_min.rxcui2 = mixed.rxcui
        where brand.tty = 'BN'
          and brand.sab = 'RXNORM'
          and mixed.tty = 'MIN'
          and mixed.sab = 'RXNORM'
    ),
         bn_to_pin_for_adc_drugs as (
             select distinct brand.str   brand_str,
                             brand.rxcui brand_rxcui,
                             brand.tty   brand_tty,
                             pin.tty     generic_tty,
                             pin.rxcui   generic_rxcui,
                             pin.str     generic_str
             from rxnconso brand
                      inner join rxnrel bn_pin on brand.rxcui = bn_pin.rxcui1 and bn_pin.rela = 'precise_ingredient_of'
                      inner join rxnconso pin on bn_pin.rxcui2 = pin.rxcui
             where brand.tty = 'BN'
               and brand.sab = 'RXNORM'
               and brand.rxcui in ('1371046', '2360535', '2267577', '2268093')
               and pin.tty = 'PIN'
               and pin.sab = 'RXNORM'
         ),
         bn_to_in as (
             select distinct brand.str        brand_str,
                             brand.rxcui      brand_rxcui,
                             brand.tty        brand_tty,
                             ingredient.tty   generic_tty,
                             ingredient.rxcui generic_rxcui,
                             ingredient.str   generic_str
             from rxnconso brand
                      inner join rxnrel bn_in on brand.rxcui = bn_in.rxcui1 and bn_in.rela = 'has_tradename'
                      inner join rxnconso ingredient on bn_in.rxcui2 = ingredient.rxcui
             where brand.tty = 'BN'
               and brand.sab = 'RXNORM'
               and ingredient.tty = 'IN'
               and ingredient.sab = 'RXNORM'
         ),
         brand_to_generic as (
             select *
             from bn_to_min
             union
             select *
             from bn_to_pin_for_adc_drugs
             union
             select *
             from bn_to_in
             where brand_rxcui not in (select brand_rxcui from bn_to_min)
               and brand_rxcui not in (select brand_rxcui from bn_to_pin_for_adc_drugs)
         ),
         all_generics as (
             select distinct tty as generic_tty, rxcui as generic_rxcui, str as generic_str
             from rxnconso
             where sab = 'RXNORM'
               and (tty in ('MIN', 'IN')
                 or (tty = 'PIN' and rxcui in ('1371041', '2267574', '2360530', '2268306'))
                 )
         ),
    tmp_rxnorm_to_brand_and_generic as (
    select brand_rxcui   as code,
           'RxNorm'      as code_system,
           brand_str     as term,
           brand_rxcui   as brand_code,
           brand_str     as brand_name,
           'RxNorm'      as brand_vocabulary,
           generic_rxcui as generic_code,
           generic_str   as generic_name,
           'RxNorm'      as generic_vocabulary
    from brand_to_generic
    union
    select generic_rxcui as code,
           'RxNorm'      as code_system,
           generic_str   as term,
           null          as brand_code,
           null          as brand_name,
           null          as brand_vocabulary,
           generic_rxcui as generic_code,
           generic_str   as generic_name,
           'RxNorm'      as generic_vocabulary
    from all_generics)

    insert into rxrel.rxnorm_to_brand_and_generic
    select *
    from tmp_rxnorm_to_brand_and_generic;

  SELECT TIMEOFDAY()::timestamp INTO srl_datetime;
  SELECT sr_release_date INTO rxn_version FROM public.setup_rxnorm_log WHERE sr_datetime IN (SELECT MAX(sr_datetime) FROM public.setup_rxnorm_log);
  SELECT COUNT(*) INTO target_table_rows FROM rxrel.rxnorm_to_brand_and_generic;
  INSERT INTO public.setup_rxrel_log VALUES(srl_datetime, rxn_version, target_table, target_table_rows);

  END IF;
END
$$
;

