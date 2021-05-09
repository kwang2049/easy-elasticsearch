import os
from elasticsearch import Elasticsearch, helpers, NotFoundError
import csv
import time
import tqdm
import requests
import os
import time
import subprocess
from typing import List, Dict
import logging
logger = logging.getLogger(__name__)
logging.getLogger('elasticsearch').setLevel(logging.CRITICAL)  # muting logging from ES
logging.basicConfig(
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)


class ElasticSearchBM25(object):
    
    def check_service_running(self, port):
        try:
            out = requests.get(f'http://localhost:9200/')
            return out.json()['cluster_name'] == 'elasticsearch'
        except:
            return False
    
    def start_service(self, es_bin, port_http, port_tcp, max_waiting):
        assert 'elasticsearch' in os.listdir(es_bin), f'No elasticsearch found in {es_bin}'
        with open(os.devnull, 'w') as null_file:
            self.p = subprocess.Popen(os.path.join(es_bin, "elasticsearch"), shell=True, stdout=null_file, stderr=subprocess.STDOUT)
            logger.info('Server shell PID: {}'.format(self.p.pid))
        
        ntried = 0
        while not self.check_service_running(port_http) and ntried < max_waiting:
            time.sleep(1)
            ntried += 1
        assert self.check_service_running(port_http), 'Exceeded maximum time for waiting, cannot start elasticsearch service'

    def index_corpus(self, corpus, index_name):
        es_index = {
            "mappings": {
                "properties": {
                        "document": {
                            "type": "text"
                        },
                }
            }
        }
        self.es.indices.create(index=index_name, body=es_index, ignore=[400])
        ndocuments = len(corpus)
        dids, documents = list(corpus.keys()), list(corpus.values())
        chunk_size = 500
        pbar = tqdm.trange(0, ndocuments, chunk_size)
        for begin in pbar:
            did_chunk = dids[begin:begin+chunk_size]
            document_chunk = documents[begin:begin+chunk_size]
            bulk_data = [{
                "_index": index_name,
                "_id": did,
                "_source": {
                    "document": documnt,
                }
            } for did, documnt in zip(did_chunk, document_chunk)]
            helpers.bulk(self.es, bulk_data)
        logger.info(f'Indexing work done: {ndocuments} documents indexed') 

    def __init__(self, corpus: Dict[str, str], es_bin, index_name='one_trial', reindexing=True, port_http='9200', port_tcp='9300', max_waiting=60):
        if self.check_service_running(port_http):
            logger.info(f'Elasticsearch service found at localhost:{port_http}')
        else:
            logger.info(f'No running service found at localhost:{port_http}. Now start it')
            self.start_service(es_bin, port_http, port_tcp, max_waiting)
        
        es = Elasticsearch()
        self.es = es
        if es.indices.exists(index=index_name) and reindexing:
            logger.info(f'Index {index_name} found and it will be indexed again since reindexing=True')
            es.indices.delete(index=index_name)
        
        self.index_corpus(corpus, index_name)
        self.index_name = index_name
        logger.info('All set up.')
    
    def query(self, query: str, topk, return_scores=False) -> Dict[str, str]:
        result = self.es.search(index=self.index_name, size=min(topk, 10000), body={
            "query": 
            {
                "match": {
                    "document": query
                }
            }
        })
        hits = result['hits']['hits']
        documents_ranked = {hit['_id']: hit['_source']['document'] for hit in hits}
        if return_scores:
            scores_ranked = {hit['_id']: hit['_score'] for hit in hits}
            return documents_ranked, scores_ranked
        else:
            return documents_ranked
    
    def score(self, query: str, document_ids: List[int]) -> Dict[str, str]:
        for i in range(60):
            try:
                scores = {}
                for document_id in document_ids:
                    result = self.es.explain(index=self.index_name, id=document_id, body={
                        "query": 
                        {
                            "match": {
                                "document": query
                            }
                        }
                    })
                    scores[document_id] = result['explanation']['value']
                return scores
            except NotFoundError as e:
                if i == 59:
                    raise e
                logger.info(f'NotFoundError, now re-trying ({i+1}/60).')
                time.sleep(1)
                
    def delete_index(self):
        logger.info(f'{self.index_name} index exists? {self.es.indices.exists(index=self.index_name)}')
        logger.info(f'Delete {self.index_name}: {self.es.indices.delete(self.index_name)}')