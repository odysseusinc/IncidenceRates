#' This package is the skeleton to build own packages with Cohort Characterization analyses. That package is able to run at any site that has access to an observational database in the Common Data Model.
#'
#' @docType package
#' @name SkeletonCohortCharacterization
NULL

.onLoad <- function(libname, pkgname) {
  rJava::.jpackage(pkgname, lib.loc = libname)
}


#source('run_ir_analysis.r')
#workDir <- getwd()

#cohorts <- list('sql/Angioedema across levetriacetam new users_2_target.sql', 'sql/Angioedema across levetriacetam new users_5_target.sql', 'sql/Angioedema across levetriacetam new users_3164_outcome.sql')

#run_ir_analysis(workDir, 3178, 'analysisDescription_3178.json', cohorts, 'postgresql', 'jdbc:postgresql://odysseusovh03.odysseusinc.com:5439/synpuf', 'ohdsi', 'ohdsi', 'public', 'results', 'public')
