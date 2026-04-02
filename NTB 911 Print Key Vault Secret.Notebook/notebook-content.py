# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run "NTB 000 Utils"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Variables
strAKVURL                 = '«replace by your Azure Key Vault URL»'
strYouTubeURLValue        = 'key-YouTubeURL'
strYouTubeParametersValue = 'key-YouTubeParameters'
strYouTubeVideoIDValue    = 'key-YouTubeVideoID'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Get AVK Values
strYouTubeURL        = notebookutils.credentials.getSecret(strAKVURL, strYouTubeURLValue)
strYouTubeParameters = notebookutils.credentials.getSecret(strAKVURL, strYouTubeParametersValue)
strYouTubeVideoID    = notebookutils.credentials.getSecret(strAKVURL, strYouTubeVideoIDValue)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Print various versions of the 3 secrets
def fnPrintYouTubeFullURL (strYouTubeURL, strYouTubeParameters, strYouTubeVideoID, strSeparator = chr(29)):
    global boolDebug

    # Set global boolDebug
    boolDebug = True

    # Define strYouTubeFullURL has the concantenation of the 3 secrets
    strYouTubeFullURL = f"{strYouTubeURL}{strYouTubeParameters}{strYouTubeVideoID}"
    intYouTubeFullURL = len(strYouTubeFullURL)

    # Define strYouTubeFullURLLong01 as the Printable Long version of the concantenation of the 3 secrets
    strYouTubeFullURLLong01  = fnGetPrintableSecretLong(strYouTubeFullURL, strSeparator)
    intYouTubeFullURLLong01  = len(strYouTubeFullURLLong01)

    # Define strYouTubeFullURLLong02 as the concatenation of the Printable Long version of each secrets
    strYouTubeURLLong        = fnGetPrintableSecretLong(strYouTubeURL, strSeparator)
    strYouTubeParametersLong = fnGetPrintableSecretLong(strYouTubeParameters, strSeparator)
    strYouTubeVideoIDLong    = fnGetPrintableSecretLong(strYouTubeVideoID, strSeparator)
    strYouTubeFullURLLong02  = f"{strYouTubeURLLong}{strYouTubeParametersLong}{strYouTubeVideoIDLong}"
    intYouTubeFullURLLong02  = len(strYouTubeFullURLLong02)

    # Define strYouTubeFullURLShort01 as the Printable Long version of the concantenation of the 3 secrets
    strYouTubeFullURLShort01 = fnGetPrintableSecretShort(strYouTubeFullURL, strSeparator)
    intYouTubeFullURLShort01 = len(strYouTubeFullURLShort01)

    # Define strYouTubeFullURLShort02 as the concatenation of the Printable Long version of each secrets
    strYouTubeURLShort        = fnGetPrintableSecretShort(strYouTubeURL, strSeparator)
    strYouTubeParametersShort = fnGetPrintableSecretShort(strYouTubeParameters, strSeparator)
    strYouTubeVideoIDShort    = fnGetPrintableSecretShort(strYouTubeVideoID, strSeparator)
    strYouTubeFullURLShort02  = f"{strYouTubeURLShort}{strYouTubeParametersShort}{strYouTubeVideoIDShort}"
    intYouTubeFullURLShort02  = len(strYouTubeFullURLShort02)

    # Print all versions
    print(f"[REDACTED] = '{strYouTubeFullURL}', length = {intYouTubeFullURL}.")
    print(f"[Long01]   = '{strYouTubeFullURLLong01}', length = {intYouTubeFullURLLong01}.")
    print(f"[Long02]   = '{strYouTubeFullURLLong02}', length = {intYouTubeFullURLLong02}.")
    print(f"[Short01]  = '{strYouTubeFullURLShort01}', length = {intYouTubeFullURLShort01}.")
    print(f"[Short02]  = '{strYouTubeFullURLShort02}', length = {intYouTubeFullURLShort02}.")

    # Reset global boolDebug
    boolDebug = False

    return


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Call fnPrintYouTubeFullURL with pipe as separator
strSeparator = '|'
fnPrintYouTubeFullURL (strYouTubeURL, strYouTubeParameters, strYouTubeVideoID, strSeparator)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Call fnPrintYouTubeFullURL with 'non-printable' character as separator
strSeparator = chr(29)
fnPrintYouTubeFullURL (strYouTubeURL, strYouTubeParameters, strYouTubeVideoID, strSeparator)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
