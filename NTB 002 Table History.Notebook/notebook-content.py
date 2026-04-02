# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

%run "NTB 000 Utils"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the delta table history for strTableName
def fnGetDeltaTableHistory (strTableName, strColumnName, strGroupByColumnName="", boolversionAsOf=True, intNrVersions=100, intIndentationLevel = 0):
    fltStartTime   = time.time()
    strNumSpaces   = fnGetIndentationString(intIndentationLevel)
    strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{strCurrentDate}: {strNumSpaces}Executing - fnGetDeltaTableHistory('{strTableName}', '{strColumnName}', '{strGroupByColumnName}', '{boolversionAsOf}', '{intNrVersions}', '{intIndentationLevel}').")

    # Set strAsOfProperty
    strAsOfProperty = "versionAsOf"
    if (~ boolversionAsOf):
        strAsOfProperty = "timestampAsOf"

    # Set strNewGroupByColumnName
    strNewGroupByColumnName = strGroupByColumnName
    # If empty then force group by existing column 'tableVersion'
    if (strGroupByColumnName == ""):
        strNewGroupByColumnName = "tableVersion"

    # Create empty dataframe
    dfTableHist = (
        spark
            .table(strTableName)
            .where(lit(False) == lit(True)) # Force empty row
            .withColumn("tableVersion", lit(0).cast("bigint"))
            .withColumn("tableTimestamp", lit(0).cast("timestamp"))
            .groupBy("tableVersion", "tableTimestamp", strNewGroupByColumnName)
            .agg(min(strColumnName).alias(f"min{strColumnName}"),
                 max(strColumnName).alias(f"max{strColumnName}"),
                 count(lit(1)).alias("NrRows")
            )
            .orderBy(strNewGroupByColumnName)
    )

    # Get all history for strTableName
    strDescribeHistory = f"describe history {strTableName}"

    # Create iterator for the history versions limited by intNrVersions
    iteTableHist = (
         spark
            .sql(strDescribeHistory)
            .limit(intNrVersions)
            .withColumn("tableVersion",   col("version"))
            .withColumn("tableTimestamp", col("timestamp"))
            .select("tableVersion",
                    "tableTimestamp"
            )
            .rdd
            .toLocalIterator()
    )

    # Iterate for all selected history versions
    try:
        for rowVersion in iteTableHist:
            strVersion   = rowVersion[0]
            strTimestamp = rowVersion[1]

            # Set strAsOfPropertyValue
            strAsOfPropertyValue = strVersion
            if (strAsOfProperty == "timestampAsOf"):
                strAsOfPropertyValue = strTimestamp

            dfTable = (
                spark
                    .read
                    .option(strAsOfProperty, strAsOfPropertyValue)
                    .table(strTableName)
                    .withColumn("tableVersion", lit(strVersion).cast("bigint"))
                    .withColumn("tableTimestamp", lit(strTimestamp).cast("timestamp"))
                    .groupBy("tableVersion", "tableTimestamp", strNewGroupByColumnName)
                    .agg(min(strColumnName).alias(f"min{strColumnName}"),
                         max(strColumnName).alias(f"max{strColumnName}"),
                         count(lit(1)).alias("NrRows")
                    )
                    .orderBy(strNewGroupByColumnName)
                    .cache()
            )

            dfTableHist = (
                dfTableHist
                    .union(dfTable
                )
                .cache()
            )

    except Exception as e:
        print(f"{e}")

    strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    strDuration    = fnGetDurationAsString(fltStartTime)
    print(f"{strCurrentDate}: {strNumSpaces}Finished  - fnGetDeltaTableHistory('{strTableName}', '{strColumnName}', '{strGroupByColumnName}', '{boolversionAsOf}', '{intNrVersions}', '{intIndentationLevel}') in '{strDuration}'.")

    return dfTableHist


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the delta table history for strTableName
# TBC - adding 'where clause' to be applied to the each table version to restrict the number of rows returned
def fnGetDeltaTableHistoryDynamic (strTableName, strColumnName, strListGroupByColumnNames="", boolversionAsOf=True, intNrVersions=100, intIndentationLevel = 0):
    fltStartTime   = time.time()
    strNumSpaces   = fnGetIndentationString(intIndentationLevel)
    strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{strCurrentDate}: {strNumSpaces}Executing - fnGetDeltaTableHistoryDynamic('{strTableName}', '{strColumnName}', '{strListGroupByColumnNames}', '{boolversionAsOf}, '{intNrVersions}', '{intIndentationLevel}').")

    # Set strAsOfProperty
    strAsOfProperty = "versionAsOf"
    if (~ boolversionAsOf):
        strAsOfProperty = "timestampAsOf"

    # Get all history for strTableName
    strDescribeHistory = f"describe history {strTableName}"

    # Set variables
    strVersion   = "0"
    strTimestamp = strCurrentDate

    # Needs to be defined before strSelect!
    # Set strNewListGroupByColumnNames
    strNewListGroupByColumnNames = strListGroupByColumnNames
    strGroupBy                   = ""

    # If not empty then add a ',' to the end
    if (strGroupByColumnName != ""):
        strNewListGroupByColumnNames = f"""
       {strListGroupByColumnNames},"""

        strGroupBy = f"""
 group by {strListGroupByColumnNames}
 order by {strListGroupByColumnNames}"""

    strSelect = f"""
select {strVersion}         as tableVersion,
       '{strTimestamp}'     as tableTimestamp,{strNewListGroupByColumnNames}
       min({strColumnName}) as min{strColumnName},
       max({strColumnName}) as max{strColumnName},
       count(*)             as NrRows
  from {strTableName}"""

    strWhere = f"""
 where 0 = 1"""

    strAsOf = f""" version as of {strVersion}"""
    if (~ boolversionAsOf):
        strAsOf = f""" timestamp as of '{strTimestamp}'"""

    strQuery = f"""
{strSelect}/*{strAsOf}*/{strWhere}{strGroupBy}
"""

    # Create empty dataframe
    dfTableHist = (
        spark
            .sql(strQuery)
            .where(lit(0) == lit(1))
            .cache()
    )

    # Get all history for strTableName
    strDescribeHistory = f"describe history {strTableName}"

    # Create iterator for the history versions limited by intNrVersions
    iteTableHist = (
         spark
            .sql(strDescribeHistory)
            .limit(intNrVersions)
            .withColumn("tableVersion",   col("version"))
            .withColumn("tableTimestamp", col("timestamp"))
            .select("tableVersion",
                    "tableTimestamp"
            )
            .rdd
            .toLocalIterator()
    )

    # Iterate for all selected history versions
    try:
        for rowVersion in iteTableHist:
            strVersion   = rowVersion[0]
            strTimestamp = rowVersion[1]

            strSelect = f"""
select {strVersion}         as tableVersion,
       '{strTimestamp}'     as tableTimestamp,{strNewListGroupByColumnNames}
       min({strColumnName}) as min{strColumnName},
       max({strColumnName}) as max{strColumnName},
       count(*)             as NrRows
  from {strTableName}"""

            strWhere = f"""
 where 1 = 1"""

            strAsOf = f""" version as of {strVersion}"""
            if (~ boolversionAsOf):
                strAsOf = f""" timestamp as of '{strTimestamp}'"""

            strQuery = f"""
{strSelect}{strAsOf}{strWhere}{strGroupBy}
"""

            dfTable = (
                spark
                    .sql(strQuery)
                    .cache()
            )

            dfTableHist = (
                dfTableHist
                    .union(dfTable
                )
                .cache()
            )

    except Exception as e:
        print(f"{e}")

    strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    strDuration    = fnGetDurationAsString(fltStartTime)
    print(f"{strCurrentDate}: {strNumSpaces}Finished  - fnGetDeltaTableHistoryDynamic('{strTableName}', '{strColumnName}', '{strListGroupByColumnNames}', '{boolversionAsOf}, '{intNrVersions}', '{intIndentationLevel}') in '{strDuration}'.")

    return dfTableHist


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
