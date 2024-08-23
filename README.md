I created some Notebooks to be able to get the delta table history located in Fabric's Lakehouse.

Import the 3 notebooks described bellow in a same folder in a Fabric Workspace and change of the configuration of the notebook **NTB 901 Table History**.

Read the rest of this README and Enjoy.

---
The notebook **NTB 000 Utils**:
 * Import common libraries.
 * Set some Spark configuration.
 * Defines some commonly used PySpark functions.

To be used in other notebooks in this repository.

---
The notebook **NTB 002 Table History** defines the following PySpark functions:
 * **fnGetDeltaTableHistory** (strTableName, strColumnName, strGroupByColumnName="", boolversionAsOf=True, intNrVersions=100, intIndentationLevel = 0): dfTableHist
   * **strTableName**: the name of the table to get the delta history.
   * **strColumnName**: a single column name to group by and get the number of rows for column in each delta history for the table.
   * **strGroupByColumnName** (optional): an additional column name to group by and get the number of rows for column in each delta history for the table.
   * **boolversionAsOf** (optional): if True then the delta history will be based on the 'version As Of'. Otherwise, it will use the delta history will be based on the 'timestamp As Of'.
   * **intNrVersions** (optional): number of delta history to retrieve. If there are less versions in the delta history, only the available number is retrieved.
   * **intIndentationLevel** (optional): defines the indentation level of the information printed by the function.
 * **fnGetDeltaTableHistoryDynamic** (strTableName, strColumnName, strListGroupByColumnNames="", boolversionAsOf=True, intNrVersions=100, intIndentationLevel = 0): dfTableHist
   * **strTableName**: the name of the table to get the delta history.
   * **strColumnName**: a single column name to group by and get the number of rows for column in each delta history for the table.
   * **strListGroupByColumnNames** (optional): an additional **list of** column name to group by and get the number of rows for column in each delta history for the table.
   * **boolversionAsOf** (optional): if True then the delta history will be based on the 'version As Of'. Otherwise, it will use the delta history will be based on the 'timestamp As Of'.
   * **intNrVersions** (optional): number of delta history to retrieve. If there are less versions in the delta history, only the available number is retrived.
   * **intIndentationLevel** (optional): defines the indentation level of the information printed by the function.

The main difference between the functions **fnGetDeltaTableHistory** and **fnGetDeltaTableHistoryDynamic**, is that the former only accepts one additional optional column, while the later accepts and optional list of columns to aggregate the data.

---
The notebook **NTB 901 Table History** allows to dynamically attach the notebook to a Fabric Lakehouse (as described in **Programmatically defining the default lakehouse of a notebook**, see  https://fabric.guru/how-to-attach-a-default-lakehouse-to-a-notebook-in-fabric).

To use this notebook, you need to set the following values:
 * Second cell - Lakehouse configuration:
   * **\<Lakehouse name\>**: the name of the Lakehouse where the table to get the history is located.
   * **\<Lakehouse Id\>**: the Id of the Lakehouse where the table to get the history is located.
   * **\<Workspace Id\>**: the Id of the Workspace where the Lakehouse is located.
 * Fourth cell - Table configuration:
   * **\<Table Name\>**: the name of the table to get the delta history.
   * **\<Column Name\>**: a single column name to group by and get the number of rows for column in each delta history for the table.
   * You can also change the value of the following variables _strGroupByColumnName_, _strListGroupByColumnNames_ and _intNrVersions_.

---
**Limitations**:
 * If you want to get the delta history of tables in a Fabric Warehouse, them create a shortcut to it in a Fabric Lakehouse and it will behave as a Lakehouse table.
 * The PySpark functions defined in **NTB 002 Table History** do not work with Lakehouses that have the Lakehouse schemas enabled (https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas).
