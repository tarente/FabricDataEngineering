I created some Notebooks to be able to get the delta table history located in Fabric's Lakehouse.

Import the 3 notebooks described bellow in a same folder in a Fabric Workspace and change of the configuration of the notebook **NTB 901 Table History**.

Read the rest of this README and Enjoy.

---
The notebook **NTB 000 Utils**:
 * Import common libraries.
 * Set some Spark configuration.
 * Defines some commonly used PySpark functions.
 * Two functions to print (or try to print) secrets. I.e., when they are printed as [REDACTED]:
  * **fnGetPrintableSecretLong** (strSecret, strSeparator = chr(29), intIndentationLevel = 0): strPossiblyPrintableSecret
    * **strSecret**: secret to be modified to be printable. Normally, stored in Azure Key Vault and obtained by using **notebookutils.credentials.getSecret**.
    * **strSeparator** (optional): the string to be used to separate each character in the strSecret.
    * **intIndentationLevel** (optional): defines the indentation level of the information printed by the function.
  * **fnGetPrintableSecretShort** (strSecret, strSeparator = chr(29), intIndentationLevel = 0): strPossiblyPrintableSecret
    * **strSecret**: secret to be modified to be printable. Normally, stored in Azure Key Vault and obtained by using **notebookutils.credentials.getSecret**.
    * **strSeparator** (optional): the string to be used to separate each character in the strSecret.
    * **intIndentationLevel** (optional): defines the indentation level of the information printed by the function.
  * The main difference between the functions **fnGetPrintableSecretLong** and **fnGetPrintableSecretShort**, is that the former retunrs a bigger string representation of the **strSecret**, since it will happend the **strSeparator** after each character in **strSecret**. While the latter, will only had a **strSeparator** between the first and second character of the **strSecret**.

To be used in other notebooks in this repository.

**Limitations**:
 * The secret needs to have at least two characters to be printable.
 * To prevent security breaches the **fnGetPrintableSecretLong** and **fnGetPrintableSecretShort** will only return a _printable_ secret if **global boolDebug = True**. Otherwise a warning message will be displayed instructing to set **global boolDebug = True**.

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

**Limitations**:
 * If you want to get the delta history of tables in a Fabric Warehouse, them create a shortcut to it in a Fabric Lakehouse and it will behave as a Lakehouse table.
 * The PySpark functions defined in **NTB 002 Table History** do not work with Lakehouses that have the Lakehouse schemas enabled (https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas).

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


**Limitations**:
 * If you want to get the delta history of tables in a Fabric Warehouse, them create a shortcut to it in a Fabric Lakehouse and it will behave as a Lakehouse table.
 * The PySpark functions defined in **NTB 002 Table History** do not work with Lakehouses that have the Lakehouse schemas enabled (https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas).

---
The notebook **NTB 911 Print Key Vault Secret** shows examples of displaying three secrets stored in an Azure Key Vault.
This notebook has inspired after watching this https://www.youtube.com/watch?v=XI94dJyxHwU (Keep your secrets SAFE in Microsoft Fabric and Azure Key Vault) and is getting the following values from a user defined Azure Key Vault:
 * **strAKVURL**: <replace by your Azure Key Vault URL>.
 * **key-YouTubeURL**: https://www.youtube.com/watch.
 * **key-YouTubeParameters**: ?v=
 * **key-YouTubeVideoID** XI94dJyxHwU

You can see from the notebook execution that the functions **fnGetPrintableSecretLong** and **fnGetPrintableSecretShort** have a different behavior when used to print a secret or a concatenation of secrets, but I will let you look at the results and take your own conclusions.

**REMEMBER: the above functions should be used for debugging purposes! That is why you need to ensure the to set **global boolDebug = True**.**
