# %%

import os
import sys
import pyspark
import pandas as pd
from pymongo import MongoClient
import json

from pyspark import StorageLevel

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, udf, col, size, expr
from pyspark.sql.types import ArrayType, FloatType

from sparknlp.base import *
from sparknlp.annotator import *

from sparknlp.pretrained import PretrainedPipeline

from pyspark.ml import Pipeline

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.functions import array_to_vector

# %%
class BioBertEmbeddings():
    def __init__(self, mongodb_base, mongodb_collection):
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        print(f"Pyspark version {pyspark.__version__}")
        self.db=mongodb_base
        self.collection=mongodb_collection
        
        # create a spark session
        self.spark = SparkSession \
            .builder \
            .master("local") \
            .appName("MongoDB") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "8g") \
            .config("spark.memory.fraction", "0.6") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.executor.cores", "4") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.driver.extraJavaOptions", "-XX:ReservedCodeCacheSize=512m") \
            .config("spark.mongodb.read.connection.uri", f"mongodb://localhost:27018/{self.db}") \
            .config("spark.mongodb.write.connection.uri", f"mongodb://localhost:27018/{self.db}") \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1') \
            .getOrCreate()
    
    def WordEmbedding(self, embedding_model):
        df = self.spark.read \
            .format("mongodb") \
            .option("uri", "mongodb://localhost:27018/UASB03") \
            .option("database", "UASB03") \
            .option("collection", self.collection) \
            .load() \
            .select("doi", "title", "titleLength", "abstract", "abstractLength")
            
        df= df.select("doi", "title", "titleLength", "abstract", "abstractLength")\
            .withColumn("merged_text", concat("title", lit(" "), "abstract"))

        df.printSchema()
        print(f'Number of documents in the df: {df.count()}')
        print("Top of the dataframe:")
        df.show(10, truncate=100)
        
        # Assemblage du document
        documentAssembler = DocumentAssembler() \
                            .setCleanupMode("inplace") \
                            .setInputCol("merged_text") \
                            .setOutputCol("document")
        # Detection des phrases
        sentence = SentenceDetector() \
                        .setInputCols(["document"]) \
                        .setOutputCol("sentence")
        # Tokenisation
        tokenizer = Tokenizer() \
            .setInputCols(['document']) \
            .setOutputCol('token')
        # Normalisation
        normalizer = Normalizer() \
            .setInputCols(["token"]) \
            .setOutputCol("normalized") \
            .setCleanupPatterns(["""[^a-zA-Z0-9]"""]) 
        # Encodage avec un modèle BERT pré-entraîné sur des articles scientifiques
        embeddings = BertEmbeddings.pretrained(embedding_model, "en") \
            .setInputCols(["document", "normalized"]) \
            .setOutputCol("biobert_embeddings")
        # Encodage des phrases (moyenne des vecteurs de mots) 
        embeddingsSentence = SentenceEmbeddings() \
                        .setInputCols(["sentence", "biobert_embeddings"]) \
                        .setOutputCol("sentence_embeddings") \
                        .setPoolingStrategy("AVERAGE")
        # Récupération des vecteurs de phrases     
        finisher = EmbeddingsFinisher() \
                                .setInputCols("sentence_embeddings") \
                                .setOutputCols("output") \
                                .setOutputAsVector(True) \
                                .setCleanAnnotations(False)
        # Pipeline
        pipeline_biobert = Pipeline(stages=[
            documentAssembler,
            sentence,
            tokenizer,
            normalizer,
            embeddings,
            embeddingsSentence,
            finisher])   
        # Application du pipeline
        model_biobert = pipeline_biobert.fit(df)
        result_biobert = model_biobert.transform(df)
        result_biobert = result_biobert.withColumn("features", col("output")[0])
        print("Schema after embedding:")
        result_biobert.printSchema()
        vector_size = result_biobert.select("features").first()["features"].size
        print(f"Size of the vectors in the features column: {vector_size}")
        result_biobert.select("title", "features").show(10, 300)
        
        df.unpersist(blocking = True)
        result_biobert.persist(StorageLevel.MEMORY_AND_DISK)
        
        #Define a UDF to convert Vector to list of floats
        vector_to_array = udf(lambda vector: vector.toArray().tolist(), ArrayType(FloatType()))
        output_mongo = result_biobert.select("title", 'abstract', "doi", "features")\
                                    .withColumn("features", vector_to_array("features"))

        output_mongo.printSchema()

        # Write to MongoDB
        
        new_collection_name=f"temp_BioBert_{self.collection}"
        output_mongo.write.format("mongodb")\
               .mode("append")\
               .option("database", "UASB03")\
               .option("collection", new_collection_name)\
               .save()
        # Connect to Mongo
        client = MongoClient('mongodb://localhost:27018/')
        db = client[self.db]
        new_collection = db[new_collection_name]
        # Create index on doi in the new collection
        indexes= new_collection.index_information()
        if 'doi_1' not in indexes:
            new_collection.create_index([('doi', 1)], unique=True)
            
        pipeline_add_embeddings = [
            {'$addFields': {'BioBertEmbedding_str': '$features'}},
            {'$unset': ['_id', 'features']},
            {'$merge': {'into': self.collection,'on': 'doi',
                    'whenMatched': 'merge','whenNotMatched': 'insert'}
             }]
        
        try:
            new_collection.aggregate(pipeline_add_embeddings)
        except Exception as e:
            print(e)
            
        new_collection.drop()
        document = db[self.collection].find_one()
        print(document)
        print("Upload to MongoDB fnished")
    
    
