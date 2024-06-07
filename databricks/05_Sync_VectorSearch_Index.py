# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch==0.22
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown(name="FILE_TYPE", defaultValue="xml", choices=["xml", "text"])
FILE_TYPE = dbutils.widgets.get("FILE_TYPE")
dbutils.widgets.dropdown(name="INSPECT_CONTENT", defaultValue="true", choices=["true", "false"])

# COMMAND ----------

# MAGIC %run ./_resources/pubmed_pipeline_config $RESET_ALL_DATA=false $DISPLAY_CONFIGS=true

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We are going to now create a VectorSearch index. This will actually be done in a couple minor steps:
# MAGIC  - Create if not exists a vector search endpoint
# MAGIC (Optional) Create and configure an endpoint to serve the embedding model
# MAGIC Create a vector search index
# MAGIC Update a vector search index
# MAGIC Query a Vector Search endpoint
# MAGIC Example notebooks
# MAGIC Show less

# COMMAND ----------

# TODO: After working instantiaion and sync, move config CONSTANTS into pubmed config class
VECTORSEARCH_ENDPOINT_NAME="pubmed"
VECTORSEARCH_INDEX_NAME=pubmed.processed_articles_content.name + "_vs_index"

# COMMAND ----------

# Utility functions for working with VectorSearch Client
# TODO: move to a run notebook script after verify works for processed_articles_content

from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

import time
def wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name):
  for i in range(180):
    try:
      endpoint = vsc.get_endpoint(vs_endpoint_name)
    except Exception as e:
      if "REQUEST_LIMIT_EXCEEDED" in str(e):
        print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. Please manually check your endpoint status")
        return
      else:
        raise e
    status = endpoint.get("endpoint_status", endpoint.get("status"))["state"].upper()
    if "ONLINE" in status:
      return endpoint
    elif "PROVISIONING" in status or i <6:
      if i % 20 == 0: 
        print(f"Waiting for endpoint to be ready, this can take a few min... {endpoint}")
      time.sleep(10)
    else:
      raise Exception(f'''Error with the endpoint {vs_endpoint_name}. - this shouldn't happen: {endpoint}.\n Please delete it and re-run the previous cell: vsc.delete_endpoint("{vs_endpoint_name}")''')
  raise Exception(f"Timeout, your endpoint isn't ready yet: {vsc.get_endpoint(vs_endpoint_name)}")

def index_exists(vsc, endpoint_name, index_full_name):
    try:
        dict_vsindex = vsc.get_index(endpoint_name, index_full_name).describe()
        return dict_vsindex.get('status').get('ready', False)
    except Exception as e:
        if 'RESOURCE_DOES_NOT_EXIST' not in str(e):
            print(f'Unexpected error describing the index. This could be a permission issue.')
            raise e
    return False

def wait_for_index_to_be_ready(vsc, vs_endpoint_name, index_name):
  for i in range(180):
    idx = vsc.get_index(vs_endpoint_name, index_name).describe()
    index_status = idx.get('status', idx.get('index_status', {}))
    status = index_status.get('detailed_state', index_status.get('status', 'UNKNOWN')).upper()
    url = index_status.get('index_url', index_status.get('url', 'UNKNOWN'))
    if "ONLINE" in status:
      return
    if "UNKNOWN" in status:
      print(f"Can't get the status - will assume index is ready {idx} - url: {url}")
      return
    elif "PROVISIONING" in status:
      if i % 40 == 0: print(f"Waiting for index to be ready, this can take a few min... {index_status} - pipeline url:{url}")
      time.sleep(10)
    else:
        raise Exception(f'''Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and re-run the previous cell: vsc.delete_index("{index_name}, {vs_endpoint_name}") \nIndex details: {idx}''')
  raise Exception(f"Timeout, your index isn't ready yet: {vsc.get_index(index_name, vs_endpoint_name)}")


# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()

# Create VectorSearch endpoint if does not exist
if VECTORSEARCH_ENDPOINT_NAME not in [ep['name'] for ep in vsc.list_endpoints()['endpoints']]:
    vsc.create_endpoint(name=VECTORSEARCH_ENDPOINT_NAME,
                        endpoint_type="STANDARD")

wait_for_vs_endpoint_to_be_ready(vsc, VECTORSEARCH_ENDPOINT_NAME)

# COMMAND ----------

# This section adds an index to the endpoint

if not index_exists(vsc, VECTORSEARCH_ENDPOINT_NAME, VECTORSEARCH_INDEX_NAME.replace('`','')):
  print(f"Creating index {VECTORSEARCH_INDEX_NAME.replace('`','')} on endpoint {VECTORSEARCH_ENDPOINT_NAME}...")
  vsc.create_delta_sync_index(
    endpoint_name=VECTORSEARCH_ENDPOINT_NAME,
    index_name=VECTORSEARCH_INDEX_NAME.replace('`',''),
    source_table_name=pubmed.processed_articles_content.name.replace('`', ''),
    pipeline_type="TRIGGERED",
    primary_key="id",
    embedding_source_column='content',                         #The column containing our text
    embedding_model_endpoint_name='databricks-bge-large-en')   #The embedding endpoint used to create the embeddings
else:
  #Trigger a sync to update our vs content with the new data saved in the table
  vsc.get_index(VECTORSEARCH_ENDPOINT_NAME, VECTORSEARCH_INDEX_NAME.replace('`','')).sync()

#Let's wait for the index to be ready and all our embeddings to be created and indexed
wait_for_index_to_be_ready(vsc, VECTORSEARCH_ENDPOINT_NAME, VECTORSEARCH_INDEX_NAME.replace('`',''))
print(f"index {VECTORSEARCH_INDEX_NAME.replace('`','')} on table {pubmed.processed_articles_content.name.replace('`', '')} is ready")

# COMMAND ----------

# Run a similarity search on our VectorSearch Index served in our VectorSearch endpoint

# COMMAND ----------

QUERY = "What impact does inflammatory response with respect to cancer?"

index = vsc.get_index(endpoint_name=VECTORSEARCH_ENDPOINT_NAME, index_name=VECTORSEARCH_INDEX_NAME.replace('`',''))
rslt = index.similarity_search(num_results=1, columns=["content"], query_text=QUERY)
print(rslt['result']['data_array'][0][0])

# COMMAND ----------


