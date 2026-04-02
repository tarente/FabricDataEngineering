# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

! pip install semantic-link-labs --q


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sempy_labs import admin


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

capacity = None #"<Capacity Name or Capacity ID>"
tenant_setting = None #"<Tenant Setting>" e.g.: "RTHOperationalAgentsTenantSwitch"
dry_run = True


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pd_capacity_settings_overrides_returned = (
    admin.delete_all_capacity_tenant_setting_overrides(
        capacity = capacity,
        tenant_setting = tenant_setting,
        dry_run = dry_run
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display (
    pd_capacity_settings_overrides_returned
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
