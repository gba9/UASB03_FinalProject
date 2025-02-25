#%%
from pymongo import MongoClient
import json

#%%
class newFields_mongo():
    def __init__(self, mongo_db_collection_name):
        self.client = MongoClient('mongodb://localhost:27018/')
        self.db = self.client['UASB03']
        self.collection= mongo_db_collection_name
        self.connect=self.db[self.collection]
        print(f'Ready to add fields to {self.collection}')
        
    def create_index_doi(self):
        collection= self.db[self.collection]
        pipeline_duplicates = [
                {
                    '$group': {
                        '_id': '$doi', 
                        'maxVersion': {
                            '$max': '$version'
                        }, 
                        'doc': {
                            '$first': '$$ROOT'
                        }
                    }
                }, {
                    '$replaceRoot': {
                        'newRoot': '$doc'
                    }
                },
                {
                    '$out': 'temp_collection'
                }
            ]
        
        try:
            collection.aggregate(pipeline_duplicates)
        except Exception as e:
            print(e)
        collection.drop()
        self.db['temp_collection'].rename(self.collection)

    # Création d'un index unique sur le champ 'doi'
        indexes = self.connect.index_information()
        if 'doi_1' not in indexes:
            self.connect.create_index([('doi', 1)], unique=True)
        
        print(f'Ready to add new fields to the collection {self.collection} - Unique index on doi created')
    
    def add_new_fields(self):
        collection= self.db[self.collection]
        pipelines = [
        [
                {
                    '$addFields': {
                        'titleLength': {
                            '$strLenCP': '$title'
                        },
                        'abstractLength': {
                            '$strLenCP': '$abstract'
                        },
                        'numberAuthors': { 
                            '$size': { '$split': ["$authors", ";"]}
                            }
                        }
                    }
                ,
                {
                    '$merge': {
                        'into': f'{self.collection}',
                        'on': '_id',
                        'whenMatched': 'merge',
                        'whenNotMatched': 'insert'
                    }
                }
            ],

        [
            {
                '$match': {
                    'published': {'$ne': 'NA'},
                    'publiDate': {'$ne': ''},
                    'date': {'$ne': ''}
                }
            },
            {
                '$addFields': {
                    'dateFormatted': {
                        '$cond': [
                            {'$regexMatch': {'input': '$date', 'regex': '^[0-9]{4}$'}},
                            {'$concat': ['$date', '-01-01']},
                            '$date'
                        ]
                    },
                    'publiDateFormatted': {
                        '$cond': [
                            {'$regexMatch': {'input': '$publiDate', 'regex': '^[0-9]{4}$'}},
                            {'$concat': ['$publiDate', '-01-01']},
                            '$publiDate'
                        ]
                    }
                }
            },
            {
                '$addFields': {
                    'dateConverted': {'$dateFromString': {'dateString': '$dateFormatted'}},
                    'publiDateConverted': {'$dateFromString': {'dateString': '$publiDateFormatted'}}
                }
            },
            {
                '$unset': ['date', 'publiData', 'dateConverted', 'publiDateConverted']
            },
            {
                '$merge': {
                    'into': f'{self.collection}',
                    'on': '_id',
                    'whenMatched': 'merge',
                    'whenNotMatched': 'insert'
                }
            }
        ],

        [
            {
                '$addFields': {
                    'publiStatus': {
                        '$cond': [
                            {'$eq': ['$published', 'NA']},  # If published == "NA"
                            'NP',  
                            {
                                '$cond': [
                                    {  # If both publiDateConverted & dateConverted are missing, but published is not "NA"
                                        '$and': [
                                            {'$eq': ['$publiDateConverted', None]},
                                            {'$ne': ['$published', 'NA']}
                                        ]
                                    },
                                    'publi_nodate',  
                                    {
                                        '$cond': [
                                            {'$lt': [ {'$dateDiff': {'startDate': '$dateConverted', 
                                                                    'endDate': '$publiDateConverted', 'unit': 'day'}}, 365]},
                                            'year1',
                                            {'$cond': [
                                                    {'$lt': [{'$dateDiff': {'startDate': '$dateConverted', 
                                                                            'endDate': '$publiDateConverted', 'unit': 'day'}}, 730]},
                                                    'year2',
                                                    'year2+'
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                }
            },
            {
                '$merge': {
                    'into': f'{self.collection}',
                    'on': '_id',
                    'whenMatched': 'merge',
                    'whenNotMatched': 'insert'
                }
            }
        ],

        [
            {
                '$match': {
                    'published': {'$ne': 'NA'},
                    'citations': {'$exists': True}
                }
            },
            {
                '$addFields': {
                    'citTotal': {'$size': '$citations'},
                    'cit1year': {
                        '$size': {
                            '$filter': {
                                'input': '$citations',
                                'as': 'citation',
                                'cond': {
                                    '$eq': [
                                        {
                                            '$arrayElemAt': [
                                                {'$split': ['$$citation.timespan', 'Y']},
                                                0
                                            ]
                                        },
                                        'P0'
                                    ]
                                }
                            }
                        }
                    },
                    'cit2years': {
                        '$size': {
                            '$filter': {
                                'input': '$citations',
                                'as': 'citation',
                                'cond': {
                                    '$gte': [
                                        {
                                            '$toInt': {
                                                '$arrayElemAt': [
                                                    {'$split': [
                                                        {
                                                            '$arrayElemAt': [
                                                                {'$split': ['$$citation.timespan', 'P']},
                                                                1
                                                            ]
                                                        },
                                                        'Y'
                                                    ]},
                                                    0
                                                ]
                                            }
                                        },
                                        2
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            {
                '$merge': {
                    'into': f'{self.collection}',
                    'on': '_id',
                    'whenMatched': 'merge',
                    'whenNotMatched': 'insert'
                }
            }
        ]
        ]
        
        for pipeline in pipelines:
            try:
                collection.aggregate(pipeline)
            except Exception as e:
                print(e)

        self.client.close()
        print(f'New fields added to the collection {self.collection}')
        
        
        
        
        
        
    
        