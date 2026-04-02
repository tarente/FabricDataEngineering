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

%run "NTB 011 get_lakehouse_tables_schema"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy_labs as labs
from sempy_labs import migration, directlake, admin, graph
from sempy_labs import lakehouse as lake
from sempy_labs import report as rep
from sempy_labs.tom import connect_semantic_model
from sempy_labs.report import ReportWrapper


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse       = "<Lakehouse Name containing the shortcuts>" # The Fabric lakehouse name or ID. Defaults to None which resolves to the lakehouse attached to the notebook.
workspace       = "<Workspace Name containing Lakehouse with the shortcuts>"               # The Fabric workspace name or ID used by the lakehouse. Defaults to None which resolves to the workspace of
                                              # the attached lakehouse or if no lakehouse attached, resolves to the workspace of the notebook.
lakehouseSchema = "<Schema enabled Lakehouse Name>"           # The Fabric lakehouse name or ID. Defaults to None which resolves to the lakehouse attached to the notebook.
workspaceSchema = "<Workspace Name containing the Schema enabled Lakehouse>"               # The Fabric workspace name or ID used by the lakehouse. Defaults to None which resolves to the workspace of
                                              # the attached lakehouse or if no lakehouse attached, resolves to the workspace of the notebook.
extended        = True                        # Obtains additional columns relevant to the size of each table.
count_rows      = True                        # Obtains a row count for each lakehouse table.
export          = False                       # Exports the resulting dataframe to a delta table in the lakehouse.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdCheckGuardRailsSchema01 = (
        get_lakehouse_tables_schema(
                lakehouse  = lakehouseSchema,
                workspace  = workspaceSchema,
                extended   = extended,
                count_rows = count_rows,
                export     = export
        )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdCheckGuardRailsSchema02 = pdCheckGuardRailsSchema01.copy()

pdCheckGuardRailsSchema02["Table Name"] = pdCheckGuardRailsSchema02["Schema Name"] + "." + pdCheckGuardRailsSchema01["Table Name"]
pdCheckGuardRailsSchema02["Schema Name"] = "dbo"

pdCheckGuardRailsSchema = (
    pdCheckGuardRailsSchema02
        [["Workspace Name",
          "Lakehouse Name",
          "Schema Name",
          "Table Name",
          "Format",
          "Type",
          "Files",
          "Row Groups",
          "Table Size",
          "Row Count",
          "SKU"]]
        .copy()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfCheckGuardRailsSchema = (
    spark
        .createDataFrame (pdCheckGuardRailsSchema)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdCheckGuardRails01 = (
    get_lakehouse_tables_schema(
        lakehouse  = lakehouse,
        workspace  = workspace,
        extended   = extended,
        count_rows = count_rows,
        export     = export
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdCheckGuardRails02 = pdCheckGuardRails01.copy()

pdCheckGuardRails02["Workspace Name"] = workspaceSchema
pdCheckGuardRails02["Lakehouse Name"] = lakehouseSchema

pdCheckGuardRails = (
    pdCheckGuardRails02
    [["Workspace Name",
      "Lakehouse Name",
      "Schema Name",
      "Table Name",
      "Format",
      "Type",
      "Files",
      "Row Groups",
      "Table Size",
      "Row Count",
      "SKU"]]
    .copy()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfCheckGuardRails = (
    spark
        .createDataFrame (pdCheckGuardRails)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdCheckGuardRailsSLL01 = (
    labs.lakehouse.get_lakehouse_tables(
        lakehouse  = lakehouse,
        workspace  = workspace,
        extended   = extended,
        count_rows = count_rows,
        export     = export
    )
)

pdCheckGuardRailsSLL01["Schema Name"] = "dbo"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdCheckGuardRailsSLL02 = pdCheckGuardRailsSLL01.copy()

pdCheckGuardRailsSLL02["Workspace Name"] = lakehouseSchema
pdCheckGuardRailsSLL02["Lakehouse Name"] = lakehouseSchema

pdCheckGuardRailsSLL = (
    pdCheckGuardRailsSLL02
    [["Workspace Name",
      "Lakehouse Name",
      "Schema Name",
      "Table Name",
      "Format",
      "Type",
      "Files",
      "Row Groups",
      "Table Size",
      "Row Count",
      "SKU"]]
    .copy()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfCheckGuardRailsSLL = (
    spark
        .createDataFrame (pdCheckGuardRailsSLL)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display (
    dfCheckGuardRailsSchema
        .orderBy("Table Name")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display (
    dfCheckGuardRails
        .orderBy("Table Name")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display (
    dfCheckGuardRailsSLL
        .orderBy("Table Name")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display (
    dfCheckGuardRailsSchema
        .subtract(dfCheckGuardRails)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display (
    dfCheckGuardRails
        .subtract(dfCheckGuardRailsSLL)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display (
    dfCheckGuardRailsSchema
        .exceptAll(dfCheckGuardRails)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display (
    dfCheckGuardRails
        .exceptAll(dfCheckGuardRailsSLL)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdCheckGuardRailsSchemaSorted = (
    pdCheckGuardRailsSchema
        .sort_values(by="Table Name")
        .sort_index()
        .reset_index(drop=True)
        .copy()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdCheckGuardRailsSorted = (
    pdCheckGuardRails
        .sort_values(by="Table Name")
        .sort_index()
        .reset_index(drop=True)
        .copy()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdCheckGuardRailsSLLSorted = (
    pdCheckGuardRailsSLL
        .sort_values(by="Table Name")
        .sort_index()
        .reset_index(drop=True)
        .copy()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display (
    pdCheckGuardRailsSchemaSorted
        .equals(pdCheckGuardRailsSorted)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display (
    pdCheckGuardRailsSorted
        .equals(pdCheckGuardRailsSLLSorted)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pandas.testing import assert_frame_equal


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert_frame_equal(pdCheckGuardRailsSchemaSorted, pdCheckGuardRailsSorted)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert_frame_equal(pdCheckGuardRailsSorted, pdCheckGuardRailsSLLSorted)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
