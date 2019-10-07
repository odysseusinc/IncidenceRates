#
#' @export
run_ir_analysis <- function(basicDir, analysisId, cohortDefinitions, dbms, connectionString, user, password, driversPath, cdmDatabaseSchema, resultsDatabaseSchema, cohortsDatabaseSchema, tempDatabaseSchema = resultsDatabaseSchema, cohortTable = "cohort", outcomeTable = "cohort"){
  start.time <- Sys.time()
  library(SqlRender)
  library(DatabaseConnector)
  library(rJava)
  # Data extraction ---
  library("rjson")
  analysisDescription <- fromJSON(paste(readLines(system.file("settings", "StudySpecification.json", package = "IncidenceRateSkeleton")), collapse = ""))

  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                  connectionString = connectionString,
                                                                  user = user,
                                                                  password = password,
                                                                  pathToDriver = driversPath)
  connection <- DatabaseConnector::connect(connectionDetails)

  query <- SqlRender::readSql(system.file("sql/sql_server", "delete_strata.sql", package = "IncidenceRateSkeleton"))
  query <- SqlRender::render(query, tableQualifier = resultsDatabaseSchema, analysis_id = analysisId)
  query <- SqlRender::translate(query, targetDialect = connectionDetails$dbms, oracleTempSchema=tempDatabaseSchema)

  DatabaseConnector::executeSql(connection, query)

  # Cohort definitions
  for(cohortFile in cohortDefinitions){
    cf <- file.path(basicDir, cohortFile)
    sql <- readSql(cf)
    sql <- render(sql,
                     cdm_database_schema = cdmDatabaseSchema,
                     vocabulary_database_schema = cdmDatabaseSchema,
                     target_database_schema = cohortsDatabaseSchema,
                     target_cohort_table = cohortTable,
                     output = "output")
    sql <- translate(sql, targetDialect = connectionDetails$dbms, oracleTempSchema=tempDatabaseSchema)
    executeSql(connection, sql)
  }

  # Insert strata rules ---

  strataRules <- analysisDescription$strata

  for (i in seq_along(strataRules)) {
    strata <- strataRules[i]
    query <- SqlRender::readSql(system.file("sql/sql_server", "strata_rules.sql", package = "IncidenceRateSkeleton"))
    query <- SqlRender::render(query, results_schema = resultsDatabaseSchema,
                                  analysis_id = analysisId,
                                  strata_sequence = i,
                                  name = strata[[1]]$name,
                                  description = "")
    query <- SqlRender::translate(query, targetDialect = connectionDetails$dbms, oracleTempSchema=tempDatabaseSchema)
    DatabaseConnector::executeSql(connection, query)
  }

  #source('ir_analysis_query_builder.r')
  expressionSql <- buildAnalysisQuery(analysisDescription, analysisId, dbms, cdmDatabaseSchema, resultsDatabaseSchema, tempDatabaseSchema)
  translatedSql <- translate(expressionSql, targetDialect = dbms, oracleTempSchema=tempDatabaseSchema)
  DatabaseConnector::executeSql(connection, translatedSql)

  # Save results
  # Summary
  sql <- SqlRender::readSql(system.file("sql/sql_server", "analysis_summary.sql", package = "IncidenceRateSkeleton"))
  sql <- SqlRender::render(sql,
                   resultsSchema = resultsDatabaseSchema,
                   id = analysisId)
  sql <- SqlRender::translate(sql, targetDialect = dbms, oracleTempSchema=tempDatabaseSchema)
  result <- DatabaseConnector::querySql(connection, sql)
  write.csv(result, file.path(workDir, "ir_summary.csv"), na = "")

  # Dist
  sql <- SqlRender::readSql(system.file("sql/sql_server", "ir_dist.sql", package = "IncidenceRateSkeleton"))
  sql <- SqlRender::render(sql,
                              resultsSchema = resultsDatabaseSchema,
                              analysisId = analysisId)
  sql <- SqlRender::translate(sql, targetDialect = dbms, oracleTempSchema=tempDatabaseSchema)
  dist <- DatabaseConnector::querySql(connection, sql)
  write.csv(dist, file.path(workDir, "ir_dist.csv"), na = "")

  # Strata
  sql <- SqlRender::readSql(system.file("sql/sql_server", "strata_stats.sql", package = "IncidenceRateSkeleton"))
  sql <- SqlRender::render(sql,
                            results_database_schema = resultsDatabaseSchema,
                            analysis_id = analysisId)
  sql <- SqlRender::translate(sql, targetDialect = dbms, oracleTempSchema=tempDatabaseSchema)
  strata <- DatabaseConnector::querySql(connection, sql)
  write.csv(strata, file.path(workDir, "ir_strata.csv"), na = "")

  disconnect(connection)

  end.time <- Sys.time()
  time.taken <- end.time - start.time
  time.taken
}
