# %%
from pymongo import MongoClient
from bs4 import BeautifulSoup
import pandas as pd
import urllib.request
import urllib.parse
import chardet
import json
from requests import get
from urllib.error import URLError

#%%

class DataCollector ():
    
    def __init__(self, mongo_db_collection_name):
        self.base_url = 'https://opencitations.net'
        self.http_headers = {"authorization": "587ee329-c5d0-4512-8272-1742619d35e6"}
        self.mongodatabase= mongo_db_collection_name
        print('DataCollector initialized - DOI list is empty')
    
    def collect_cit(self, doi): # API Call OpenCitations pour collecter les citations
        url = f'{self.base_url}/index/api/v2/citations'
        sorting = 'sort=desc(timespan)'
        cit_url = f'{url}/doi:{doi}?{sorting}'
        print(f'URL OpenCit:{cit_url}')

        try: 
            request = urllib.request.Request(cit_url, headers=self.http_headers)
            opencit_response = urllib.request.urlopen(request, timeout=60).read()
            response_json = json.loads(opencit_response)
            print(f'Response: {response_json}')
            return response_json
        except Exception as e: 
            print(f'Error collecting publication date for DOI {doi}: {e}') 
            return ''
        
    def collect_pubdate(self, doi): # API Call OpenCitations pour collecter la date de publication
        url = f'{self.base_url}/meta/api/v1/metadata'
        pubdate_url = f'{url}/doi:{doi}'
        print(f'URL OpenCit:{pubdate_url}')

        try:
            request = urllib.request.Request(pubdate_url, headers=self.http_headers)
            opencit_response = urllib.request.urlopen(request, timeout=60).read()
            pubdate = json.loads(opencit_response)[0].get("pub_date")
            print(f'Publication Date: {pubdate}')
            return pubdate
        except Exception as e: 
            print(f'Error collecting citations for DOI {doi}: {e}') 
            return ''

    def collect_and_upload_CitData(self):
        # Connection à la collection MongoDB
            uri = "mongodb://localhost:27018/"
            client = MongoClient(uri)
            database = client["UASB03"]
            collection = database[self.mongodatabase]
            query = { # Requête pour les articles sans date de publication et sans citations
                "published": {"$ne": "NA"},
                "publiDate": {"$exists": False},
                "citations": {"$exists": False}
            }
            print(f'Number of results: {collection.count_documents(query)}')
            cursor = collection.find(query)

            for document in cursor:
                doi = document.get("published")
                print(doi)
                
                try: # Collecte de la date de publication
                    pubdate = self.collect_pubdate(doi) 
                except Exception as e: 
                    print(f'Error collecting pubdate for DOI {doi}: {e}') 
                    pubdate = ''
                    continue
                    
                try: # Collecte des citations
                    citations = self.collect_cit(doi) 
                except Exception as e: 
                    print(f'Error collecting cit for DOI {doi}: {e}') 
                    citations = ''
                    continue
                     
                if pubdate != '' and citations != '': 
                    result = collection.update_one(
                        {"published": doi},
                        {"$set": {"publiDate": pubdate, "citations": citations}}
                        )
                    print(result.matched_count)
                    print(result.modified_count)
                else:   # Pas d'upload si les 2 informations ne sont pas disponibles
                    print('No data available')

            client.close()
            print('Finished')