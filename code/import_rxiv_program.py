# %%
import json
from pymongo import MongoClient
import urllib.request


# Class to collect the preprints from BioRXiv
# %%
class rxiv_collector() :
    def __init__(self, mongo_db_name):
        self.uri = "mongodb://localhost:27018/" # Info de connection a la base de donnée
        self.client = MongoClient(self.uri)
        self.database = self.client["UASB03"]
        print('Instance created')
        self.collection_name = mongo_db_name

    def biorxiv_upload(self, cursor, server, abstract_url):
        
        print(f'URL {server}:{abstract_url}')
        biorxiv_response = urllib.request.urlopen(abstract_url).read()
        
        # Parse the JSON and collect the list of articles "collection"
        response_json = json.loads(biorxiv_response)
        collect_preprints = response_json.get("collection", [])
        
        try:
            collection = self.database[self.collection_name] # Upload dans la base
            result = collection.insert_many(collect_preprints)
            print(result.acknowledged)
        
        except Exception as e:
            print(f"Cursor error: {cursor}") # Impression du curseur pour pouvoir reprendre la collecte
            raise Exception(
                "The following error occurred: ", e)
        
    def biorxiv_collect(self, entry_dict):
        server = entry_dict.get('server')
        date_interval = entry_dict.get('date_interval')
        counter = entry_dict.get('counter')
        
        url=f'https://api.biorxiv.org/details/{server}' # Creation de l'URL pour questionner l'API
        abstract_url = f'{url}/{date_interval}/{counter}'
        print(f'URL {server}:{abstract_url}')
        biorxiv_response = urllib.request.urlopen(abstract_url).read()
        
        # Informations sur le nombre total d'articles obtenus avec la requete 
        response_json = json.loads(biorxiv_response)
        messages = response_json.get("messages", []) 
        count_new_papers = int(messages[0].get('count_new_papers', 0))
        total = int(messages[0].get('total', 0))
        print("Messages:", messages)
        print(f'Count of new papers: {count_new_papers}, Total: {total}')
        
        while counter <= (total): # Boucle pour collecter les articles par 100 (max autorisé par l'API)
            print("Cursor:", counter)
            self.biorxiv_upload(counter, server, abstract_url)
            counter=counter+100
        print("Finished")
        


