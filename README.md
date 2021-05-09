# Easy Elasticsearch

This repository contains a high-level encapsulation for using [Elasticsearch](https://www.elastic.co/downloads/elasticsearch) with python in just a few lines.

## Installation
Via pip:
```bash
pip install easy-elasticsearch
```
Via git repo:
```bash
git clone https://github.com/kwang2049/easy-elasticsearch
pip install -e . 
```
To get the backend server program as the very last step, one also needs to download official Elasticsearch: (please find the [suitable version](https://www.elastic.co/downloads/elasticsearch) for your OS if not using Linux x86/64)
```
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.12.1-linux-x86_64.tar.gz
tar -xf elasticsearch-7.12.1-linux-x86_64.tar.gz
```

## Usage
Just first create an ElasticSearchBM25 object while indicating the text pool to be indexed and your local path to elasticsearch-xx.xx.xx/bin; then either call its ```rank``` or ```score``` function for retrieval or calculating BM25 scores.
```python
from easy_elasticsearch import ElasticSearchBM25

pool = {
    'id1': 'What is Python? Is it a programming language',
    'id2': 'Which Python version is the best?',
    'id3': 'Using easy-elasticsearch in Python is really convenient!'
}
bm25 = ElasticSearchBM25(pool, 'elasticsearch-7.12.1/bin')  # remember to use your local path of elasticsearh/bin

query = "What is Python?"
rank = bm25.query(query, topk=10)  # topk should be <= 10000
scores = bm25.score(query, document_ids=['id2', 'id3'])

print(query, rank, scores)
```
Another example for retrieving Quora questions can be found in [example/quora.py](https://github.com/kwang2049/easy-elasticsearch/blob/main/example/quora.py).
