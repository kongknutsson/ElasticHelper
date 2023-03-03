import pandas as pd 
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk
from os import getenv
from dotenv import load_dotenv

load_dotenv()
ELASTIC_PASSWORD = getenv("ELASTIC_PASSWORD")
ELASTIC_USERNAME = getenv("ELASTIC_USERNAME")
ELASTIC_CERT = getenv("ELASTIC_CERT")


class ElasticHelper:
    """This class is used to help with the bulk insertion of data into ElasticSearch.
    """

    def __init__(self) -> None:
        self.es = Elasticsearch(
            "https://localhost:9200",
            ca_certs=ELASTIC_CERT,
            basic_auth=("elastic", ELASTIC_PASSWORD),
            verify_certs=False
        )

    def _doc_generator(self, index_name: str, df: pd.DataFrame):
        """ This is a generator function that yields a dictionary of the index name, document id, and the document itself.

        Args:
            index_name (str): The index to insert the document into.
            df (pd.DataFrame): The dataframe to iterate over.

        Yields:
            dict: A dictionary of the index name, document id, and the document itself.
        """
        df_iter = df.iterrows()
        for index, document in df_iter:
            yield {
                "_index": index_name,
                "_id" : f"{document['SNo']}",
                "_source": document.to_dict()
            }

    def bulk_insert(self, index_name: str, df: pd.DataFrame):
        """This function is used to bulk insert a dataframe into ElasticSearch.

        Args:
            index_name (str): The index to insert the document into.
            df (pd.DataFrame): The dataframe to iterate over.
        """
        responses = parallel_bulk(self.es, self._doc_generator(index_name, df))
        for response in responses:
            if response[1]["index"]["status"] != 201:
                print(response)
                
    def create_index(self, index_name: str, mappings: dict) -> None:
        """This function is used to create an index in ElasticSearch.

        Args:
            index_name (str): The name of the index to create.
            mappings (dict): The mappings to use for the index.
        """
        request_body = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1
            },
            "mappings": mappings
        }
        self.es.indices.create(index=index_name, body=request_body)

    def delete_index(self, index_name: str) -> bool: 
        """This function is used to delete an index in ElasticSearch.

        Args:
            index_name (str): The name of the index to delete.

        Returns:
            bool: True if the index was deleted, False if the user did not confirm deletion.
        """
        answer = input(f"WARNING: Being asked to delete {index_name}, is this correct? (y/n) ")
        if answer.lower() == "n":
            print(f"Interrupted deletion of {index_name}.")
            return False 
        self.es.indices.delete(index=index_name)
        return True 
