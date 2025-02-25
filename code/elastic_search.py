#%%
from pymongo import MongoClient
import json
from elasticsearch import Elasticsearch
import urllib3

#%%
class index_ES():
    def __init__(self, db, mongodb_source_collection, ES_username, ES_password):
        
        # Connexion à la collection MongoDB
        self.client = MongoClient('mongodb://localhost:27018/')
        self.db = self.client[db]
        self.collection = self.db[mongodb_source_collection]
        try:
            document = self.collection.find_one()
            if document:
                print(f"Connection to MongoDB collection {mongodb_source_collection} successfully!")
            else:
                print("Connection to MongoDB collection is successful, but the collection is empty.")
        except Exception as e:
            print("Failed to connect to MongoDB collection:", e)
        
        # Connexion à ElasticSearch
        urllib3.disable_warnings()
        self.es = Elasticsearch(hosts="https://localhost:9200", 
                                basic_auth=(ES_username, ES_password), 
                                verify_certs=False)

        if self.es.ping():
            print("Connected to Elasticsearch successfully!")
        else:
            print("Failed to connect to Elasticsearch.")
            
        print(self.es.info())
        
    def export_collection_toES(self, new_index_name):
        # Creation d'un index dans ES
        if not self.es.indices.exists(index=new_index_name):
            self.es.indices.create(index=new_index_name)
            print(f"Index {new_index_name} created successfully.")
        else:
            print(f"Index {new_index_name} already exists.")
        
        # Collecte des documents dans MongoDB
        pipeline_export = [
                {
                    '$project': {
                    'doi': 1,
                    'title': 1,
                    'abstract': 1,
                    'authors':1,
                    'author_corresponding_institution':1,
                    'category':1
                    }
                }]
        results = self.collection.aggregate(pipeline_export)
        results_list = list(results)
        print(f"Number of documents retrieved from MongoDB {self.collection}: {len(results_list)}")
        
        # Transfert des documents dans l'index ES par batch de 10'000
        output_iterate = []
        i=0

        for document in results_list:

            output_iterate.append({"index": {"_index": new_index_name, "_id": document.get('doi')}})
            output_iterate.append({
                "title": document.get('title'),
                "abstract": document.get('abstract'),
                "doi": document.get('doi'),
                "authors": document.get('authors'),
                "author_corresponding_institution": document.get('author_corresponding_institution'),
                "category": document.get('category')
            })

            if i % 10000 == 0 and i != 0:  
                self.es.bulk(operations=output_iterate, index=new_index_name)
                    
                count_index = self.es.count(index=new_index_name)
                print(f"Number of documents in '{new_index_name}':", count_index["count"])
                    
                output_iterate = []
            i += 1

        # Transfert des derniers documents après la boucle
        if output_iterate:
            self.es.bulk(operations=output_iterate, index=new_index_name)  
        count_index = self.es.count(index=new_index_name)
        print(f"Number of documents in '{new_index_name}':", count_index["count"])

    def search_ES_index(self, es_index_name, query):
        # Exécution de la recherche
        response = self.es.search(index=es_index_name, body=query, size=10000)
        print(response)
        total_hits = response["hits"]["total"]["value"]
        doi_list = [hit["_source"].get("doi", "N/A") for hit in response["hits"]["hits"]]

        # Output results
        print(f"Number of results: {total_hits}")
        print(f"lenght of doi list: {len(doi_list)}")
        return doi_list
    
    def results_collection_mongoDB(self, new_collection_mongodb, doi_list):
        
        if new_collection_mongodb in self.db.list_collection_names():
            print(f"Collection {new_collection_mongodb} already exists.")
        else:
            output_collection = self.db[new_collection_mongodb]
            output_collection.create_index([('doi', 1)], unique=True)
            print(f"Collection {new_collection_mongodb} created successfully with index on 'doi'.")
                
        pipeline_query_output = [
            {'$match': {'doi': {'$in': doi_list}}},
            {'$merge': {'into': f'{new_collection_mongodb}',
                    'on': 'doi',
                    'whenMatched': 'merge','whenNotMatched': 'insert'}}
        ]
             
        try:
            self.collection.aggregate(pipeline_query_output)
            print('Search results integated successfully to a new MongoDB collection')
        except Exception as e:
            print(e)
        

