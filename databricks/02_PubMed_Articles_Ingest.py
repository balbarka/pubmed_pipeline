# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PubMed Articles Ingest
# MAGIC
# MAGIC **Objective**: This notebook will use both developer arguments, `pubmed.raw_metadata`, and `pubmed.raw_search_hist` to query PMC for new articles related to our key word search topcis of interest. Upon a successful run, `pubmed.raw_metadata` and `pubmed.raw_search_hist` will be updated with search and download metadata and articles will be downloaded to `pubmed.raw_articles`.
# MAGIC
# MAGIC
# MAGIC This notebook can be used interactively or as a script that can be used in a job. This notebook has a single section that runs a single method that only downloads files and updates metadata. As a convenience of execution, the method accepts pubmed asset classes.

# COMMAND ----------

# DBTITLE 1,Widget Configuration
dbutils.widgets.dropdown(name="FILE_TYPE", defaultValue="xml", choices=["xml", "text"])
FILE_TYPE = dbutils.widgets.get("FILE_TYPE")
dbutils.widgets.text(name="KW_SEARCH", defaultValue="breast cancer")
KW_SEARCH = dbutils.widgets.get("KW_SEARCH")
dbutils.widgets.dropdown(name="INSPECT_METADATA_HIST", defaultValue="true", choices=["true", "false"])
#INSPECT_METADATA_HIST = dbutils.widgets.get("INSPECT_METADATA_HIST")
dbutils.widgets.dropdown(name="INSPECT_SEARCH_HIST", defaultValue="true", choices=["true", "false"])
#INSPECT_SEARCH_HIST = dbutils.widgets.get("INSPECT_SEARCH_HIST")

# COMMAND ----------

# MAGIC %run ./_resources/pubmed_pipeline_config $RESET_ALL_DATA=false $DISPLAY_CONFIGS=true

# COMMAND ----------

# MAGIC %run ./_resources/pubmed_central_utils

# COMMAND ----------

# Example for updating all existing KW_SEARCH
metadata_updates = get_needed_pmids_df(search_hist=pubmed.raw_search_hist,
                                       metadata=pubmed.raw_metadata,
                                       articles=pubmed.raw_articles,
                                       min_dte = "2024/06/01")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Inspect `metadata_update` (OPTIONAL)
# MAGIC
# MAGIC Sometimes it's easier to inspect with just SQL, by using a createOrReplaceTempView.
# MAGIC
# MAGIC The output of the metadata_update is all of the files that were just downloaded with the following additional fields:
# MAGIC
# MAGIC | Field Name | Description |
# MAGIC | ---------- | ----------- |
# MAGIC | `Status`   | This is the status of the given article which can be assigned one of the following states: </br>- `PENDING` : The article is known to exist, but not downloaded </br>- `DOWNLOADED`: The article is downloaded with an assigned Volume path </br>- `ERROR`: There was an Error in the download process and the download should be corrected manually </br> - `RETRACTED` : The article has been retracted by PMC. |
# MAGIC | `volume_path` | This is the path that the raw file has been extracted to | 

# COMMAND ----------

metadata_updates.createOrReplaceTempView("metadata_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM metadata_updates;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Inspect `pubmed.raw_metadata`

# COMMAND ----------

if dbutils.widgets.get("INSPECT_METADATA_HIST") == 'true':
    dat = spark.sql(f"SELECT * FROM {pubmed.raw_metadata.name}").filter('Status = "DOWNLOADED"')
    display(dat)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Inspect `pubmed.raw_search_hist`
# MAGIC
# MAGIC To avoid running unnecessary searches on PMC, we will persist our search history and update as we search over more and more date ranges. Since there isn't a known business use case where having a gap in search history is beneficial, raw_search_hist will only search over ranges with min max dates (no gaps in search). There is a helper function, `get_search_hist_args` that will expand a KW_SEARCH range requested by a user to avoid gaps.

# COMMAND ----------

if dbutils.widgets.get("INSPECT_METADATA_HIST") == 'true':
    dat = spark.sql(f"SELECT * FROM {pubmed.raw_search_hist.name}")
    display(dat)
