from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import os
import json
# from datetime import datetime

# Connect to the Elasticsearch instance
CERT_FINGERPRINT = "4e83f56cebeff8f8996481ecfed3ae1281566cfadb8e4988229dfe3b46d2532c"
ELASTIC_PASSWORD = "vYGRQcwMwtr1ruvL7YMg"
INDEX_NAME = "face_records"
INPUT_DIR = "inputs"
OUTPUT_DIR = "output"

es = Elasticsearch(
    "http://localhost:9200",
    # ssl_assert_fingerprint=CERT_FINGERPRINT,
    # basic_auth=("elastic", ELASTIC_PASSWORD)
)
client_info = es.info()
print("Connected to Elasticsearch")
# print(client_info.body)

documents = []

def query_metadata_by_date(index_name, start_date, end_date, timestamp_field="timestamp"):
    """
    Query ElasticSearch for metadata entries within a defined time period.
    
    :param index_name: The name of the Elasticsearch index to query.
    :param start_date: The start date in "YYYY-MM-DD" format.
    :param end_date: The end date in "YYYY-MM-DD" format.
    :param timestamp_field: The field in your documents that stores the timestamp.
    :return: A list of metadata entries (documents).
    """
    
    # Define the search query with a date range filter
    query = {
        "query": {
            "range": {
                timestamp_field: {
                    "gte": start_date,
                    "lte": end_date,
                    "format": "yyyy-MM-dd"
                }
            }
        }
    }
    
    # Execute the search query
    response = es.search(index=index_name, body=query, size=1000)  # size=1000 limits the results to 1000 entries
    
    # Extract the metadata (hits) from the response
    metadata_entries = [hit['_source'] for hit in response['hits']['hits']]
    
    return metadata_entries

def add_documents():
    # collect input data
    for filename in os.listdir(INPUT_DIR):
        file_path = os.path.join(INPUT_DIR, filename)
        with open(file_path, 'r') as file:
            doc = json.load(file)
            print("=================================================")
            documents.append({"_index": INDEX_NAME, "_source": { "doc": doc, "date": filename }})

    bulk(es, documents)

if __name__ == "__main__":

    # create an index and add documents to elasticsearch
    # create index of elasticsearch
    # es.indices.create(index=INDEX_NAME)
    # add_documents()
    # print("added documents!")

    query_body = {'query': { 'match': { 'doc.search_users.SearchedFace.FaceId': "7e7887e0-62fb-4326-b8db-360e310fc52e" } } }
    result = es.search(index=INDEX_NAME, body=query_body)
    print(result)

    # delete index of elasticsearch
    # es.indices.delete(index=INDEX_NAME)
    # es.delete(index=INDEX_NAME, id="_-s4lZIBFmtJn3B0Fejo")
    # es.delete_by_query(index=INDEX_NAME, body={"query": {"match_all": {}}})
    # print("deleted!")
