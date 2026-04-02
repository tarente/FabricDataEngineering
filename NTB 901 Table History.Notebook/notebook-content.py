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

# MARKDOWN ********************

# **Programmatically defining the default lakehouse of a notebook**
# 
# Source: https://fabric.guru/how-to-attach-a-default-lakehouse-to-a-notebook-in-fabric


# CELL ********************

# MAGIC %%configure -f
# MAGIC { 
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": '<Lakehouse name>',
# MAGIC         "id": '<Lakehouse Id>',
# MAGIC         "workspaceId": '<Workspace Id>'
# MAGIC     }
# MAGIC }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run "NTB 002 Table History"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define table's information
strTableName              = "<Table Name>"
strColumnName             = "<Column Name>"
strGroupByColumnName      = ""
strListGroupByColumnNames = ""
intNrVersions             = 5


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fnGetDeltaTableHistory version As Of
display (
    fnGetDeltaTableHistory (
        strTableName,
        strColumnName,
        strGroupByColumnName=strGroupByColumnName,
        boolversionAsOf=True,
        intNrVersions=intNrVersions
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fnGetDeltaTableHistory timestamp As Of
display (
    fnGetDeltaTableHistory (
        strTableName,
        strColumnName,
        strGroupByColumnName=strGroupByColumnName,
        boolversionAsOf=False,
        intNrVersions=intNrVersions
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fnGetDeltaTableHistoryDynamic version As Of
display (
    fnGetDeltaTableHistoryDynamic (
        strTableName,
        strColumnName,
        strListGroupByColumnNames=strListGroupByColumnNames,
        boolversionAsOf=True,
        intNrVersions=intNrVersions
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fnGetDeltaTableHistoryDynamic timestamp As Of
display (
    fnGetDeltaTableHistoryDynamic (
        strTableName,
        strColumnName,
        strListGroupByColumnNames=strListGroupByColumnNames,
        boolversionAsOf=False,
        intNrVersions=intNrVersions
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
