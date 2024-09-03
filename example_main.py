import pandas as pd
from elastic_helper import ElasticHelper

class DataLoader:
    @staticmethod
    def load(path: str) -> pd.DataFrame:
        data = DataLoader._read_data(path)
        data = DataLoader._cast_data_types(data)
        return data

    @staticmethod
    def _read_data(path: str) -> pd.DataFrame:
        return pd.read_csv(path, encoding="utf-8")

    @staticmethod
    def _cast_data_types(df: pd.DataFrame) -> pd.DataFrame:
        # Perform changes to the dataframe if needed. 
        return df


if __name__ == "__main__":
    # Example usage for reading a csv file and inserting it into an Elasticsearch index.

    data_path = "example_data.csv"
    index_name = "example_index"

    data = DataLoader.load(data_path)

    es = ElasticHelper()

    if input("Recreate entire index? (y/n) " == "y"):
        mappings = {
            'properties': {
                'Id': {'type': 'long'},
                'Name': {'type': 'keyword'},
                'Age': {'type': 'long'},
                'Occupation': {'type': 'keyword'},
            }
        }
        es.delete_index(index_name)
        es.create_index(index_name, mappings)
    es.bulk_insert(index_name, data, "Id")