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

%pip install semantic-link-labs -q


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timedelta
from sempy_labs import admin


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# The cell below 👇🏻 is hidden, but you can unhide it to see the code.

# CELL ********************

func_in_semantic_link_labs = True

try:
    from sempy_labs.admin import list_activity_events_multiple_days
    print("Function 'list_activity_events_multiple_days' is already incorporated in Semantic Link Labs.")

except Exception as exc:
    func_in_semantic_link_labs = False
    print(f"Defining the missing functions!")

    from sempy_labs import admin
    import sempy_labs._icons as icons

    from sempy_labs._helper_functions import (
        _create_dataframe,
    )
    import time
    from datetime import datetime, timedelta
    import pandas as pd
    from typing import Optional
    import importlib

    # Define the functions if they do not exist in Semantic Link Labs.
    def _resolve_function(func_name: str, namespace):
        """
        Resolve a function name string into an actual callable/function object.

        Rules:
        - Only search inside the provided namespace (usually globals()).
        - Support dotted paths like "admin.list_events".
        - Do NOT import modules.
        - Fail immediately if any part of the path does not exist.
        """

        # Split the function name into parts (e.g., "admin.list_events" → ["admin", "list_events"])
        parts = func_name.split(".")

        # ---------------------------------------------------------
        # Case 1: simple function name (no dots)
        # ---------------------------------------------------------
        if len(parts) == 1:
            name = parts[0]

            # The function must exist directly in the namespace
            if name in namespace:
                return namespace[name]
            else:
                # Fail if not found
                raise ValueError(f"{icons.red_dot} Function '{func_name}' not found in namespace!")
        else:
            # ---------------------------------------------------------
            # Case 2: dotted path (object.attribute.attribute...)
            # ---------------------------------------------------------

            # The first element must exist in the namespace (e.g., "admin")
            module_name = parts[0]
            name = parts[1]
            if module_name in namespace:
                return getattr(namespace[module_name], name)
            else:
                # Fail if not found
                raise ValueError(f"{icons.red_dot} '{module_name}' not found in namespace!")


    def _count_rows(obj):
        """
        Count the number of logical records in an arbitrary structure.
        Ensures consistent results between dicts and pandas DataFrames.

        Error handling:
        - Gracefully handles unexpected types
        - Protects against malformed dicts/lists
        - Never crashes on non-iterable or mixed-type structures
        """

        try:
            # DataFrame → number of rows
            if isinstance(obj, pd.DataFrame):
                return len(obj)

            # Series → number of elements
            if isinstance(obj, pd.Series):
                return len(obj)

            # List of dicts → treat each element as a row
            if isinstance(obj, list):
                if all(isinstance(x, dict) for x in obj):
                    return len(obj)
                # Mixed list → fallback to 1 row
                return 1

            # Dict with lists → assume lists represent columns
            if isinstance(obj, dict):
                list_lengths = []
                for v in obj.values():
                    if isinstance(v, list):
                        try:
                            list_lengths.append(len(v))
                        except Exception:
                            # Non-countable list element → ignore
                            pass

                if list_lengths:
                    return max(list_lengths)  # pandas-like behavior

                return 1  # single logical record

            # Anything else → treat as 1 record
            return 1

        except Exception as e:
            # Optional: enable this for debugging
            # print(f"count_rows error: {e} (type={type(obj)})")
            return 1

    def _deep_merge(a, b):
        """
        Recursively merge two objects of arbitrary structure.

        Merge rules:
        - pandas.DataFrame + pandas.DataFrame:
            Concatenate rows (axis=0), index ignored.
        - pandas.Series + pandas.Series:
            Concatenate values (axis=0), index ignored.
        - dict + dict:
            Recursively merge keys. If a key exists in both, merge their values.
        - list + list:
            Concatenate lists.
        - Any other combination:
            Primitive overwrite → b replaces a.

        Parameters
        ----------
        a : any
            First object.
        b : any
            Second object.

        Returns
        -------
        any
            The merged result following the rules above.
        """

        # DataFrame + DataFrame → row-wise concat
        if isinstance(a, pd.DataFrame) and isinstance(b, pd.DataFrame):
            return pd.concat([a, b], ignore_index=True)

        # Series + Series → concat
        if isinstance(a, pd.Series) and isinstance(b, pd.Series):
            return pd.concat([a, b], ignore_index=True)

        # dict + dict → recursive merge
        if isinstance(a, dict) and isinstance(b, dict):
            merged = {}
            keys = set(a.keys()) | set(b.keys())
            for k in keys:
                if k in a and k in b:
                    merged[k] = _deep_merge(a[k], b[k])
                elif k in a:
                    merged[k] = a[k]
                else:
                    merged[k] = b[k]
            return merged

        # list + list → concatenate
        if isinstance(a, list) and isinstance(b, list):
            return a + b

        # Anything else → b wins
        return b

    def execute_in_timeslots(func_name, parameters_list, max_per_slot, slot_seconds, namespace):
        """
        Execute a function repeatedly with rate limiting, merging results and
        logging progress.

        - Resolves the target function once from the given namespace.
        - Processes each parameters as a separate call with arguments in parameters_list.
        - Enforces a sliding‑window limit: max_per_slot calls per slot_seconds.
        - Sleeps or resets the window when limits are reached.
        - Merges results using deep_merge and counts rows via count_rows.
        - On error, logs the failure and returns partial results immediately.

        Returns the merged result of calling the function func_name with the parameters in the parameters_list.
        """

        results = None          # Accumulated DataFrame OR merged dict
        results_dict = []       # Store raw dict results
        total_rows = 0          # Total number of rows returned across all calls

        # Initialize sliding‑window rate limiting
        window_start = time.time()
        calls_in_window = 1

        # Resolve the function only once for efficiency
        func = _resolve_function(func_name, namespace)

        # Iterate over each parameters
        for idx, params in enumerate(parameters_list, start=1):

            elapsed = time.time() - window_start

            print("\n====================================================")
            print(f"Iteration {idx}")

            if elapsed >= slot_seconds:
                print(f"{icons.in_progress} Window expired → resetting window.")
                window_start = time.time()
                elapsed = time.time() - window_start
                calls_in_window = 1

            elif calls_in_window > max_per_slot:
                sleep_time = slot_seconds - elapsed
                print(f"{icons.in_progress} Rate limit reached → sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
                window_start = time.time()
                elapsed = time.time() - window_start
                calls_in_window = 1
                print("{icons.in_progress} Window reset after sleep.")

            print(f"Elapsed in current window: {elapsed:.2f}s")
            print(f"Calls in current window: {calls_in_window}/{max_per_slot}")

            try:
                call_str = f"{func_name}(" + ", ".join([f"{k}={repr(v)}" for k, v in params.items()]) + ")"
                print(f"Executing now: {call_str}")

                # Execute the resolved function
                result = func(**params)

                rows_idx = 0
                results = _deep_merge(results, result)
                rows_idx = _count_rows(result)

                total_rows += rows_idx

                print(f"{icons.green_dot} Returned '{rows_idx}' rows: {call_str}.")
                print(f"{icons.green_dot} Partial results '{total_rows}'.")

                calls_in_window += 1

            except Exception as e:
                print(f"{icons.red_dot} ERROR during execution of {call_str}: '{e}'!")
                print(f"{icons.yellow_dot} Returning partial results: '{total_rows}'.")
                return results

        print(f"{icons.green_dot} Total results: '{total_rows}'.")

        return results

    def list_activity_events_multiple_days(
        start_day: str,
        num_days: int,
        inc_days: int = 1,
        activity_filter: Optional[str] = None,
        user_id_filter: Optional[str] = None,
        return_dataframe: bool = True,
    ) -> pd.DataFrame | dict:
        """
        Shows a list of audit activity events for a tenant.

        This is a wrapper function for the following API: `Admin - Get Activity Events <https://learn.microsoft.com/rest/api/power-bi/admin/get-activity-events>`_.

        Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

        Parameters
        ----------
        start_time : str
            Start date and time of the window for audit event results. Example: "2024-09-25T07:55:00".
        end_time : str
            End date and time of the window for audit event results. Example: "2024-09-25T08:55:00".
        activity_filter : str, default=None
            Filter value for activities. Example: 'viewreport'.
        user_id_filter : str, default=None
            Email address of the user.
        return_dataframe : bool, default=True
            If True the response is a pandas.DataFrame. If False returns a dict. Default True

        Returns
        -------
        pandas.DataFrame | dict
            A pandas dataframe or dict showing a list of audit activity events for a tenant.
        """

        func_name = "admin.list_activity_events"
        parameters_list = []
        max_per_slot = 200
        slot_seconds = 60
        namespace = globals()
        
        # Normalize start_day
        start_date = datetime.strptime(start_day, "%Y-%m-%d").date()

        # Build the list of tasks
        for i in range(num_days):
            day = start_date + timedelta(days = i * inc_days)

            start_iso = day.strftime("%Y-%m-%dT00:00:00.000Z")
            end_iso   = day.strftime("%Y-%m-%dT23:59:59.999Z")

            parameters_list.append(
                {
                    "start_time": start_iso,
                    "end_time": end_iso,
                    "activity_filter": activity_filter,
                    "user_id_filter": user_id_filter,
                    "return_dataframe": return_dataframe,
                }
            )

        results = (
            execute_in_timeslots(func_name, parameters_list, max_per_slot, slot_seconds, namespace)
        )

        return results



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timedelta


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_day = (datetime.utcnow().date() - timedelta(days=27)).strftime("%Y-%m-%d")
num_days  = 3 # 28
inc_days = 7 # 1
activity_filter = None
user_id_filter = None
return_dataframe = True # False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if func_in_semantic_link_labs:
    results = (
        admin.list_activity_events_multiple_days(
            start_day = start_day,
            num_days = num_days,
            inc_days = inc_days,
            activity_filter = activity_filter,
            user_id_filter = user_id_filter,
            return_dataframe = return_dataframe
        )
    )
else:
    results = (
        list_activity_events_multiple_days(
            start_day = start_day,
            num_days = num_days,
            inc_days = inc_days,
            activity_filter = activity_filter,
            user_id_filter = user_id_filter,
            return_dataframe = return_dataframe
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if return_dataframe:
    from pyspark.sql.functions import *
    from pyspark.sql.window import Window

    df_results = (
        spark
            .createDataFrame(results)
    )

    display(
        df_results
            .withColumn("Creation Date", to_date(col("Creation Time")))
            .groupBy("Creation Date")
            .agg(
                min(col("Creation Time")).alias("min Creation Time"),
                max(col("Creation Time")).alias("max Creation Time"),
                count("*").alias("Num Rows")
            )
            .orderBy("Creation Date")
            .withColumn(
                "Acc Num Rows",
                sum("Num Rows").over(Window.rowsBetween(Window.unboundedPreceding, Window.currentRow))
            )
            .withColumn(
                "Total Num Rows",
                sum("Num Rows").over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
            )
    )
else:
    print(results)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
