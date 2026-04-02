# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# Imports
from pyspark.sql.functions import *
from pyspark.sql import Window
from delta.tables import *
import datetime
import time


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enable vorder
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enable zorder
spark.conf.set("spark.sql.parquet.zorder.enabled", "true")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set global variables
global intIndentationNumSpaces
global boolDebug

intIndentationNumSpaces = 2
boolDebug = False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Returns the number of defined spaces for the intIndentationLevel
def fnGetIndentationString (intIndentationLevel):
    global intIndentationNumSpaces
    global boolDebug

    strDebug = ""
    if (boolDebug):
        strDebug = "[DBG] "
    
    strIndentationString = " " * (intIndentationNumSpaces * intIndentationLevel)
    strFullIndentationString = f"{strDebug}{strIndentationString}"

    return strFullIndentationString


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return the duration between the parameter (fltStartTime) and current time as a string
def fnGetDurationAsString (fltStartTime):
    # Get current time
    fltEndTime = time.time()

    # Set query
    strSelect = f"""
  with TAB as
       (
        select --ElapsedTime,
               (int((int(ElapsedTime) - (int((int(ElapsedTime) - (int((int(ElapsedTime) - (int(ElapsedTime) % 60)) / 60) % 60)) % 3600)) / 24) / 86400)) as numDay,
               (int(((int(ElapsedTime) - (int((int(ElapsedTime) - (int(ElapsedTime) % 60)) / 60) % 60)) / 3600)) % 24)                                   as numHour,
               (int((int(ElapsedTime) - (int(ElapsedTime) % 60)) / 60) % 60)                                                                             as numMinutes,
               (int(ElapsedTime) % 60)                                                                                                                   as numSeconds,
               round((ElapsedTime - int(ElapsedTime)) * 1000)                                                                                            as numMiliSeconds
          from (select {fltEndTime - fltStartTime} as ElapsedTime)        
       )
select --ElapsedTime,
       concat(cast(TAB.numDay as string), ".",
              right(concat("00", cast(TAB.numHour as string)), 2), ":",
              right(concat("00", cast(TAB.numMinutes as string)), 2), ":",
              right(concat("00", cast(TAB.numSeconds as string)), 2), ".",
              right(concat("000", cast(TAB.numMiliSeconds as string)), 3), "h"
        ) as strDuration
  from TAB
"""

    # Calculate duration
    strDuration = (
        spark
            .sql(strSelect)
            .first()[0]
    )

    return strDuration


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the max value for column in table
def fnGetMaxColumnInTable (strColumnName, strTableName, intIndentationLevel = 0):
    fltStartTime   = time.time()
    strNumSpaces   = fnGetIndentationString(intIndentationLevel)
    strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{strCurrentDate}: {strNumSpaces}Executing - fnGetMaxColumnInTable('{strColumnName}', '{strTableName}', '{intIndentationLevel}').")

    # Get max value
    intMax = (
        spark
            .sql(f"select int(ifnull(max({strColumnName}), 0)) as intMax from {strTableName}")
            .first()[0]
    )

    strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    strDuration    = fnGetDurationAsString(fltStartTime)
    print(f"{strCurrentDate}: {strNumSpaces}Finished  - fnGetMaxColumnInTable('{strColumnName}', '{strTableName}', '{intIndentationLevel}') in '{strDuration}'.")

    return intMax


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the dataframe (dfTableData) into the delta table (strSchemaTableName), partitioning by strPartitionByColumn using mode strMode
def fnWriteToDeltaTable (strSchemaTableName, strMode, strPartitionByColumn, dfTableData, intIndentationLevel = 0):
    fltStartTime   = time.time()
    strNumSpaces   = fnGetIndentationString(intIndentationLevel)
    strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{strCurrentDate}: {strNumSpaces}Executing - fnWriteToDeltaTable('{strSchemaTableName}', '{strMode}', '{strPartitionByColumn}', 'dataframe', '{intIndentationLevel}').")

    # Set variables
    strFormat   = "delta"
    strLocation = f"Tables/{strSchemaTableName}"
    strSchemaDotTableName = strSchemaTableName.replace ("/", ".")
    
    # Create the table if not exists
    ## if (not(spark.catalog.tableExists(strSchemaDotTableName))): # Currently not working
    if (not(fnCheckSchemaTableExistance(strSchemaTableName, (intIndentationLevel + 1)))):
        
        strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Check if it is partitioned
        if (strPartitionByColumn == ""):
            print(f"{strCurrentDate}: Creating non-partitioned and inserting table '{strSchemaTableName}'.")

            (
                dfTableData
                    .write
                    .format(strFormat)
                    .save(strLocation)
            )
        else:
            print(f"{strCurrentDate}: Creating partitioned and inserting table '{strSchemaTableName}' by '{strPartitionByColumn}'.")

            (
                dfTableData
                    .write
                    .partitionBy(strPartitionByColumn)
                    .format(strFormat)
                    .save(strLocation)
            )
    else:
        strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if (strMode == "overwrite"):
            # Re-create the table and insert the data in the table
            strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Check if it is partitioned
            if (strPartitionByColumn == ""):
                print(f"{strCurrentDate}: Re-creating non-partitioned and inserting table '{strSchemaTableName}'.")

                (
                    dfTableData
                        .write
                        .mode(strMode)
                        .option("overwriteSchema", "true")
                        .format(strFormat)
                        .save(strLocation)
                )
            else:
                print(f"{strCurrentDate}: Re-creating partitioned and inserting table '{strSchemaTableName}' by '{strPartitionByColumn}'.")

                (
                    dfTableData
                        .write
                        .mode(strMode)
                        .option("overwriteSchema", "true")
                        .partitionBy(strPartitionByColumn)
                        .format(strFormat)
                        .save(strLocation)
                )
        else:
            # Insert the data in the table
            print(f"{strCurrentDate}: {strNumSpaces}  Inserting table '{strSchemaTableName}'.")

            (
                dfTableData
                    .write
                    .mode(strMode)
                    .format(strFormat)
                    .insertInto(strSchemaDotTableName)
            )

    strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    strDuration    = fnGetDurationAsString(fltStartTime)
    print(f"{strCurrentDate}: {strNumSpaces}Finished  - fnWriteToDeltaTable('{strSchemaTableName}', '{strMode}', '{strPartitionByColumn}', 'dataframe', '{intIndentationLevel}') in '{strDuration}'.")

    return True


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return the secret in the parameter separated by string provided
def fnGetPrintableSecretLong (strSecret, strSeparator = chr(29), intIndentationLevel = 0):
    # fltStartTime   = time.time()
    # strNumSpaces   = fnGetIndentationString(intIndentationLevel)
    # strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # print(f"{strCurrentDate}: {strNumSpaces}Executing - fnGetPrintableSecretLong('Secret', '{strSeparator}').")

    global boolDebug

    strPrintableSecretLong = strSecret

    if (boolDebug):
        strPrintableSecretLong = ""
        for char in strSecret:
            strPrintableSecretLong += strSeparator + char
    else:
        strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        strDuration    = fnGetDurationAsString(fltStartTime)
        print(f"{strCurrentDate}: {strNumSpaces}Warning   - fnGetPrintableSecretLong: Secrets were not changed since global boolDebug = 'False'!.")        

    # strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # strDuration    = fnGetDurationAsString(fltStartTime)
    # print(f"{strCurrentDate}: {strNumSpaces}Finished  - fnGetPrintableSecretLong('Secret', '{strSeparator}') in '{strDuration}'.")

    return strPrintableSecretLong


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return the secret in the parameter with a string provided after the first character in the secret
def fnGetPrintableSecretShort (strSecret, strSeparator = chr(29), intIndentationLevel = 0):
    # fltStartTime   = time.time()
    # strNumSpaces   = fnGetIndentationString(intIndentationLevel)
    # strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # print(f"{strCurrentDate}: {strNumSpaces}Executing - fnGetPrintableSecretShort('Secret', '{strSeparator}').")

    global boolDebug

    strPrintableSecretShort = strSecret

    if (boolDebug):
        strPrintableSecretShort = strSecret[:1] + strSeparator + strSecret[1:]
    else:
        strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        strDuration    = fnGetDurationAsString(fltStartTime)
        print(f"{strCurrentDate}: {strNumSpaces}Warning   - fnGetPrintableSecretShort: Secrets were not changed since global boolDebug = 'False'!.")        

    # strCurrentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # strDuration    = fnGetDurationAsString(fltStartTime)
    # print(f"{strCurrentDate}: {strNumSpaces}Finished  - fnGetPrintableSecretShort('Secret', '{strSeparator}') in '{strDuration}'.")

    return strPrintableSecretShort


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
