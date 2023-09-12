from elasticsearch import Elasticsearch, helpers
from pandas import DataFrame


def _index_data(records: {}, index: str):
    for record in records:
        yield {
            "_index": index,
            "_source": record
        }


class IndexHelper:

    def __init__(self, es: Elasticsearch):
        self.es = es

    def bulk_index(self, data: DataFrame, index: str):
        data_dict = data.to_dict('records')

        if self.es.indices.exists(index=index):
            self.es.indices.delete(index=index)
            print('Deleting index...')

        helpers.bulk(self.es, _index_data(data_dict, index))
