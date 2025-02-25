# %%
from pymongo import MongoClient
import json
import pandas as pd
import os
import sys
import pyspark
import datetime

from pyspark import StorageLevel

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, udf, col, size, expr, log, max as spark_max, when, month, to_date, desc
from pyspark.sql.types import ArrayType, FloatType

from pyspark.ml import Pipeline, PipelineModel

from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.functions import array_to_vector


# %%

pipeline_export = [
            {'$match': {"$or": [
                { "$and": [ 
                    { "published": { "$ne": 'NA'}},
                    { "publiStatus": { "$ne": "publi_nodate"}}
                    ]},
                { "published": 'NA'}
                    ]}
            }]

array_to_vector = udf(lambda array: Vectors.dense(array), VectorUDT())

# %%
def export_csv(mongodb_name, collection_name, output_path, output_name):
    client = MongoClient('mongodb://localhost:27018/')
    db = client[mongodb_name]
    collection = db[collection_name]
        
    results = collection.aggregate(pipeline_export)
    results_list = list(results)
    print(f"Number of documents retrieved in {collection_name}: {len(results_list)}")
        

    list_fields=['doi', 'server', 'version', 'category', 'publiStatus', 'date', 'author_corresponding_institution',
             'titleLength', 'abstractLength', 'numberAuthors', 'cit1year']
        
    parsed_results = []
    for result in results_list:
        result_iter = {}
        for field in list_fields:
            # Ajout de chaque champs dans un dictionnaire
            result_iter[field] = result.get(field, None)
        parsed_results.append(result_iter)
        
    df = pd.DataFrame(parsed_results, columns=list_fields)
    # Change type de version and renommage de la colonne BioBert
    df['version'] = df['version'].astype('int64', errors='ignore')
    df.rename(columns={'BioBertEmbedding_str': 'BioBert'}, inplace=True)
        
    print(df.info())
    # Export the dataframe to a .csv file
    df.to_csv(f'{output_path}/{output_name}.csv', index=False)

# %%

class pretreatment_spark():
    
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
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0') \
            .getOrCreate()
        # Lecture contenu collection MongoDB
        df = self.spark.read \
                .format("mongodb") \
                .option("uri", f"mongodb://localhost:27018/{self.db}") \
                .option("database", "UASB03") \
                .option("collection", mongodb_collection) \
                .option("aggregationPipeline", str(pipeline_export)) \
                .load()

        df = df.select('doi', 'server', 'category', 'publiStatus', 'date', #'version', 'author_corresponding_institution',
                    'titleLength', 'abstractLength', 'numberAuthors', 'cit1year', 
                    'BioBertEmbedding_str')

        print(df.count())

        #Transformations des variables
        df = df.withColumn("BioBert_vec", array_to_vector(col("BioBertEmbedding_str")))\
                        .drop('BioBertEmbedding_str')\
                        .withColumn("numberAuthorsLog", log(col("numberAuthors"))) \
                        .withColumn("month_upload", month(to_date(col("date"))))

        # Recodage catégories
        df = df.withColumn("category_grouped",
            when(col("category").isin(["neuroscience", "neurology", "psychiatry and clinical psychology", "animal behavior and cognition"]), "Neuro")
            .when(col("category").isin(["infectious diseases", "hiv aids"]), "InfectDis")
            .when(col("category").isin(["genomics", "genetics", "genetic and genomic medicine"]), "GenetGenom")
            .when(col("category").isin(["ecology", "zoology"]), "EcoZoo")
            .when(col("category").isin(["evolutionary biology", "paleontology"]), "PaleoEvol")
            .when(col("category").isin(["cell biology", "molecular biology", "biochemistry"]), "MolCellBio")
            .when(col("category").isin(["cancer biology", "oncology"]), "CancerOnco")
            .when(col("category").isin(["immunology", "allergy and immunology"]), "Immuno")
            .when(col("category").isin(["epidemiology", "public and global health", "health policy", "health economics",
                                        "occupational and environmental health", "health systems and quality improvement",
                                        "medical education", "scientific communication and education", "medical ethics"]),
                "PublicHealthEpidemioEthics")
            .when(col("category").isin(["bioinformatics", "systems biology", "health informatics"]), "Bioinfo")
            .when(col("category").isin(["systems biology", "biophysics"]), "BioPhySystemsBio")
            .when(col("category").isin(["plant biology"]), "PlantBio")
            .when(col("category").isin(["physiology", "endocrinology", "nutrition"]), "PhysioEndocrino")
            .when(col("category").isin(["pharmacology and toxicology", "pharmacology and therapeutics", "toxicology", "clinical trials"]),
                "PharmacoTherapeutics")
            .when(col("category").isin(["nephrology", "gastroenterology", "hematology", "dermatology", "ophthalmology",
                                        "otolaryngology", "urology", "pediatrics", "geriatric medicine", "addiction medicine",
                                        "dentistry and oral medicine", "rehabilitation medicine and physical therapy", "sports medicine",
                                        "obstetrics and gynecology", "sexual and reproductive health", "cardiovascular medicine",
                                        "respiratory medicine", "intensive care and critical care medicine", "emergency medicine",
                                        "pain medicine", "palliative medicine", "primary care research", "nursing", "surgery",
                                        "transplantation", "orthopedics", "radiology and imaging", "anesthesia", "rheumatology", "pathology"]),
                "OtherMedecineSpecialties")
            .when(col("category").isin(["synthetic biology", "bioengineering"]), "SyntheticBioengineering")
            .when(col("category").isin(["microbiology"]), "MicroBio")
            .when(col("category").isin(["developmental biology"]), "DevelopmentBio")
            .otherwise(col("category")))
        
        
        df_output = df.select('server', 'category_grouped', 'month_upload', 'publiStatus',
                                  'titleLength', 'abstractLength', 'numberAuthorsLog', 'cit1year',
                                  'BioBert_vec').cache()
        
        df.unpersist()
        df_output.printSchema()
        df_output.show(10)
        ## Group and check content of DB
        df_output.groupby("publiStatus").count().orderBy(desc('count')).show()
        df_output.groupby("server").count().orderBy(desc('count')).show()
        df_output.groupby("month_upload").count().orderBy(desc('count')).show()
        df_output.groupby("category_grouped").count().orderBy(desc('count')).show()
        self.df_output=df_output
        
    
    def classification(self, output_path, output_name):
        df_classif=self.df_output.withColumn("label", when(col("publiStatus").isin(["year1"]), 1)
                                .otherwise(0)) \
                                .drop('cit1year')\
                                .cache()

        indexer_classif = StringIndexer(inputCols=["server", "category_grouped", ],
                                outputCols=["server_index", "category_grouped_index"])
        
        encoder_classif = OneHotEncoder(inputCols=["server_index", 'month_upload', "category_grouped_index"],
                                outputCols=["server_encoded", 'month_upload_encoded', "category_grouped_encoded"])
        
        # Assemble numerical features to be scaled into a single vector
        assembler_subset = VectorAssembler( inputCols=['titleLength', 'abstractLength', 'numberAuthorsLog'],
                                    outputCol="subsetFeatures")

        # scaler for assembled numerical features
        scaler_subset = StandardScaler(inputCol='subsetFeatures', outputCol='scaled_subsetfeatures')\
                .setWithStd(True) \
                .setWithMean(True)
        
        assembler_all = VectorAssembler( inputCols=['server_encoded', 'month_upload_encoded', 
                                                    'category_grouped_encoded',
                                                    'scaled_subsetfeatures', 
                                                    'BioBert_vec'], 
                                        outputCol="features")

        pipeline_pretreat_classification=Pipeline(stages=[
            indexer_classif,
            encoder_classif,
            assembler_subset,
            scaler_subset,
            assembler_all])
        
        df_classif = pipeline_pretreat_classification.fit(df_classif).transform(df_classif)
        df_classif.printSchema()
        df_classif.show(10, truncate=100)
        print("Features vector size:", df_classif.select("features").head()[0].size)
        
        df_classif.write.mode("overwrite").parquet(f"{output_path}/{output_name}")
        return df_classif
                                             
      
    def regression(self, output_path, output_name):  
        df_reg=self.df_output.filter(col('cit1year').isNotNull())\
            .cache()
        
        indexer_reg = StringIndexer(inputCols=["server", "category_grouped", "publiStatus"],
                                outputCols=["server_index", "category_grouped_index", "publiStatus_index"])
        
        encoder_reg = OneHotEncoder(inputCols=["server_index", 'month_upload', "category_grouped_index", "publiStatus_index"],
                                outputCols=["server_encoded", 'month_upload_encoded', "category_grouped_encoded", "publiStatus_encoded"])
        
        # Assemble numerical features to be scaled into a single vector
        assembler_subset = VectorAssembler( inputCols=['titleLength', 'abstractLength', 'numberAuthorsLog'],
                                    outputCol="subsetFeatures")

        # scaler for assembled numerical features
        scaler_subset = StandardScaler(inputCol='subsetFeatures', outputCol='scaled_subsetfeatures')\
                .setWithStd(True) \
                .setWithMean(True)
        
        assembler_all = VectorAssembler( inputCols=['server_encoded', 'month_upload_encoded', 
                                                    'category_grouped_encoded', 'publiStatus_encoded',
                                                    'scaled_subsetfeatures', 
                                                    'BioBert_vec'], 
                                        outputCol="features")

                       
        pipeline_pretreat_regression=Pipeline(stages=[
            indexer_reg,
            encoder_reg,
            assembler_subset,
            scaler_subset,
            assembler_all])
    
        df_reg = pipeline_pretreat_regression.fit(df_reg).transform(df_reg)
        df_reg.printSchema()
        df_reg.show(10, truncate=100)
        print("Features vector size:", df_reg.select("features").head()[0].size)
        
        df_reg.write.mode("overwrite").parquet(f"{output_path}/{output_name}")
        return df_reg
        

    



#
# %%
