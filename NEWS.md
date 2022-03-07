# setupRxNorm 1.4.0.9000  

## RxNav RxClass  

### Features  

* Added option to point to a prior RxClass RxNav version 
with a `prior_version` argument. This allows to test out development 
using extracted data when a new version is available via that API and having 
to rerun all the API calls.  

* Added CONCEPT_SYNONYM table    


### Bugs  

* Fixed DDL to match new format  

* `NA` concept codes are filtered out of the CONCEPT table after 
confirming that they are duplicates, but the root cause of this error 
still needs to be investigated.  


# setupRxNorm 1.4.0  

* Added RxClass Data v12 in new format  

# setupRxNorm 1.3.0  

* Added RxClass Data v11 
* Updated RxClass CONCEPT table output to include concept_class_id  

# setupRxNorm 1.2.0  

* Added feature that converts RxClass API responses into relational database format  



