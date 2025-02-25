# Contexte
La plateforme BioRxiv est une archive de pré-publications dans le domaine des sciences
biologiques lancée en 2013 sur le modèle de ArXiv, dédiée aux sciences physiques,
mathématiques et informatiques. Elle permet aux chercheurs de déposer et diffuser leurs
manuscrits avant leur publication dans des journaux revus par les pairs.
Au cours des dernières années, et plus récemment suite à la pandémie du COVID-19, l’usage
de BioRxiv a considérablement augmenté, le nombre de pré-publications déposées sur BioRxiv
chaque mois ayant doublé entre 2018 et 2024 [1]. Malgré les préoccupations quant à la qualité
du contenu déposé sur ces plateformes [2], leur impact positif sur la publication et la diffusion
ultérieure des articles dans des revues scientifiques de qualité est maintenant attestée [3], [4],
[5].
Par ailleurs, si la prédiction du nombre de citations et de l’impact des articles publiés est une
question déjà largement adressé par des techniques de machine learning [6], [7] ([8] pour une
revue sur le sujet), l’usage de méthodes prédictives pour étudier l’avenir des pré-publications
dans le domaine biomédical n’a à notre connaissance pas été rapporté. En effet, c’est plutôt
l’impact de la pré-publication (ou non) sur le nombre de citations des articles ultérieurs qui a
été étudié [4] ou une étude approfondie du devenir des pré-publications BioRxiv [9].
Dans le cadre de ce projet, nous souhaitons donc nous intéresser aux facteurs susceptibles
d’impacter la publication de ces pré-publications dans un journal suite à leur dépôt sur la
plateforme, et leur futur succès en nombre de citations. Nous aimerions pouvoir prédire si une
pré-publication sera publiée dans un journal dans l’année suivant son dépôt sur la plateforme,
et son nombre de citations lors de la première année de publication dans un journal. Chaque
pré-publication sera considérée comme une entité indépendante et nous nous baserons
uniquement sur ses caractéristiques intrinsèques. Par caractéristiques intrinsèques, nous
entendons l'absence de données concernant le réseau académique des auteurs ainsi que leur
précédent succès en termes de nombre de publications, et de citations, ou la réputation du
journal dans lequel est publié l’article (impact factor).

# Objectifs du projet
Dans le cadre du Certificat d’Analyse de données massives, les objectifs du projet sont
multiples :
- Collecte d’une grande quantité de données à partir de plusieurs sources dans une base
de données distribuée (NFE204).
- Usage d’un moteur de recherche plein-texte pour sélectionner un sous-ensemble
thématique dans le corpus (NFE204).
- Pré-traitement et analyse exploratoire du jeu de données (STA211).
- Encodage des textes avec un modèle adapté (RCP216).
- Construction, test et validation d’un modèle d’apprentissage supervisé pour prédire la
publication d’un article dans l’année suivant le dépôt d’une pré-publication (prédicteur
binaire, problème de classification) (STA211, RCP216).
- Construction, test et validation d’un modèle d’apprentissage supervisé pour prédire le
nombre de citations des articles publiés, durant leur première année de publication
(problème de régression) (STA211, RCP216).

# Description des étapes du projet
Le schéma suivant reprend les étapes principales du projet et la table 1 détaille les outils
employés et les liens vers les parties correspondantes dans le présent rapport, ainsi que vers les
extraits de code présentés en annexe. Les parties I à IV de ce rapport sont dédiées à la
description des étapes du projet, tandis que la partie V abordera la problématique du passage à
l'échelle.

<img width="371" alt="image" src="https://github.com/user-attachments/assets/7f86c5b9-c9bd-49a8-94cf-ed4397536c8a" />


### 1 Collecte et stockage des données des pré-publications via l’API BioRxiv
Python,PyMongo,MongoDB 
Script: import_rxiv_program.py

### 2 Collecte des informations de citations avec l’API OpenCitations. 
Ajout de ces nouvelles variables aux documents (‘citations’ et ‘publiDate’) |Python, PyMongo, MongoDB 
Script: add_citations.py

### 3 Calcul de nouvelles variables pour chaque document dans MongoDB
Pipelines d’agrégations MongoDB, PyMongo
Script: aggregations_mongodb.py

### 4 Création de collections thématiques par recherche plein-texte
Elastic Search
Script: elastic_search.py

### 5 Export et analyse exploratoire des données (variables numériques et catégorielles)
Scripts: R export_collection.py, exploratory_data_analysis.R

### 6 Encodage des titres et résumés des pré- publications avec un modèle Bert
Spark- MongoDB connector, SparkNLP
Script: bioBert_embeddings.py

### 7 Pré-traitement et transformation des variables pour l’apprentissage supervisé
Spark-MongoDB connector
Script: export_collection.py

### 8 Test et ajustement de différents modèles de classification binaire pour déterminer si une pré-publication sera publiée dans un journal dans la première année de son dépôt sur la plateforme d’archive.
Script: Spark MLib 8_binaryClassif.ipynb

### 9 Comparaison des performances des modèles de classification binaire.
Script: Spark MLib 9_binaryClassif_ModelComparison.ipynb

### 10 Test et ajustement de différents modèles de régression pour déterminer le nombre de citations dans la première année de publication, pour le sous-ensemble des pré-publications publiée.
Script: Spark MLib 10_regression.ipynb

### 11 Comparaison des performances des modèles de régression.
Script: Spark MLib 10_regression.ipynb

# Scalabilité
Les serveurs de BioRxiv comprennent aujourd'hui près de 500’000 pré-publications, ce qui
constitue un ensemble massif de données, qui ne cesse de croitre. Malgré la réalisation de ce
projet sur une unique machine localement, nous avons intégré des outils et méthodes permettant
de réaliser les traitements décris en prenant en compte les problématiques liées aux
caractéristiques des données massives :
- Volume : Stockage sur une plateforme distribuée.
- Variété : Stockage sous forme de documents JSON puis DataFrames Spark, et
traitements spécifiques des variables de types variés (numérique, catégoriel, textuel,
vectoriel, etc…).
- Vélocité : emploi de méthodes permettant une parallélisation des calculs (Agrégations
MongoDB, traitement distribué dans Spark).

Les outils choisis sont présentés dans la figure 2, et peuvent permettre un passage à l'échelle par
distribution afin de déployer le stockage et traitement sur un cluster de plusieurs machines.
En particulier, les explications liées au partitionnement et à la réplication des données 
stockées, l’usage d'algorithmes permettant la parallélisation des calculs, et l’intégration des
différents outils de stockage et de traitement des données, seront abordés dans la dernière partie de ce
rapport.

<img width="181" alt="image" src="https://github.com/user-attachments/assets/efa0b9a3-b62c-47fb-8e48-bbea8ccd5d2a" />

# Références
[1]	“bioRxiv Reporting.” Accessed: Jan. 06, 2025. [Online]. Available: https://api.biorxiv.org/reports/

[2]	C. F. D. Carneiro et al., “Comparing quality of reporting between preprints and peer-reviewed articles in the biomedical literature,” Res. Integr. Peer Rev., vol. 5, no. 1, p. 16, Dec. 2020, doi: 10.1186/s41073-020-00101-3.

[3]	D. Y. Fu and J. J. Hughey, “Releasing a preprint is associated with more attention and citations for the peer-reviewed article,” eLife, vol. 8, p. e52646, Dec. 2019, doi: 10.7554/eLife.52646.

[4]	N. Fraser, F. Momeni, P. Mayr, and I. Peters, “The relationship between bioRxiv preprints, citations and altmetrics,” Quant. Sci. Stud., vol. 1, no. 2, pp. 618–638, Jun. 2020, doi: 10.1162/qss_a_00043.

[5]	H. Liu, G. Hu, and Y. Li, “The enhanced research impact of self-archiving platforms: Evidence from bioRxiv,” J. Assoc. Inf. Sci. Technol., vol. 75, no. 8, pp. 883–897, 2024, doi: 10.1002/asi.24932.

[6]	L. D. Fu and C. Aliferis, “Models for Predicting and Explaining Citation Count of Biomedical Articles,” AMIA. Annu. Symp. Proc., vol. 2008, pp. 222–226, 2008.

[7]	M. Wang, S. Jiao, J. Zhang, X. Zhang, and N. Zhu, “Identification High Influential Articles by Considering the Topic Characteristics of Articles,” IEEE Access, vol. 8, pp. 107887–107899, 2020, doi: 10.1109/ACCESS.2020.3001190.

[8]	K. Kousha and M. Thelwall, “Factors associating with or predicting more cited or higher quality journal articles: An Annual Review of Information Science and Technology (ARIST) paper,” J. Assoc. Inf. Sci. Technol., vol. 75, no. 3, pp. 215–244, 2024, doi: 10.1002/asi.24810.

[9]	R. J. Abdill and R. Blekhman, “Tracking the popularity and outcomes of all bioRxiv preprints,” eLife, vol. 8, p. e45133, Apr. 2019, doi: 10.7554/eLife.45133.



