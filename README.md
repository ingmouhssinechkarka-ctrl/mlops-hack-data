# Projet MLOps : hack_data avec Spark ML + MLflow

## La question
Une entreprise a été piratée. `data/hack_data.csv` contient 334 attaques. Les experts pensent
qu'il y a eu **2 ou 3 hackers**. Indice : chaque hacker a fait à peu près **le même nombre
d'attaques**. Il n'y a pas de colonne "réponse", donc on fait du **clustering** (on regroupe
les attaques qui se ressemblent).

## Le pipeline
```
data/hack_data.csv ─► 1. Ingestion ─► 2. Traitement ─► 3. 3 modèles x (k=2, k=3) ─► MLflow
                      (Spark lit       (doublons,        KMeans, BisectingKMeans,     (params,
                       le CSV)          features,         GaussianMixture              métriques,
                                        mise à l'échelle)                              modèles)
```

| Fichier | Rôle |
|---|---|
| `pipeline/ingestion.py` | Étape 1 : lit le CSV avec Spark et un schéma typé. |
| `pipeline/preprocessing.py` | Étape 2 : enlève doublons / lignes vides, `VectorAssembler` puis `StandardScaler`. La colonne `Location` est ignorée (les hackers utilisent des VPN). |
| `pipeline/train.py` | Étape 3 : 3 modèles de clustering, chacun testé avec k=2 et k=3 → 6 runs MLflow. |
| `run_pipeline.py` | Lance tout le pipeline. |
| `lancer.bat` | Raccourci Windows : installe et lance tout en double-cliquant. |

**Ce que MLflow enregistre pour chaque run** : nombre de lignes, hyperparamètres (k, seed...),
métriques `silhouette` (qualité des groupes) et `balance_min_max` (1.00 = groupes de même
taille), taille des groupes (`cluster_sizes`) et le modèle entraîné.

## Résultat attendu
```
run                      silhouette  équilibre  tailles des groupes
kmeans_k2                    0.8176       1.00  [167, 167]
kmeans_k3                    0.7552       0.50  [84, 167, 83]
bisecting_kmeans_k2          0.8176       1.00  [167, 167]
bisecting_kmeans_k3          0.7552       0.50  [84, 83, 167]
gaussian_mixture_k2          0.8176       1.00  [167, 167]
gaussian_mixture_k3          0.7608       0.47  [79, 88, 167]
```
Avec k=2 les deux groupes ont exactement la même taille (167/167). Avec k=3 non.
**Conclusion : il y a eu 2 hackers.**

## Prérequis (Windows)
- **Python 3.10 à 3.12** conseillé : `python --version`
- **Java 17** (ou 21) : `java -version` doit répondre. Sinon installe
  [Temurin 17](https://adoptium.net/) et coche "Set JAVA_HOME".

## Lancer
Ouvre une invite de commandes (cmd) dans ce dossier, puis :
```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

python run_pipeline.py
mlflow ui
```
Ouvre ensuite http://127.0.0.1:5000 → expérience **hack-data** → coche les runs → **Compare**.
(Ou double-clique sur `lancer.bat` qui fait tout ça.)

## Pipeline GitHub Actions
`.github/workflows/ml-pipeline.yml` se lance à chaque push sur `main`, à chaque pull request, et
à la main (onglet **Actions** → **ML pipeline** → **Run workflow**). Sur Ubuntu avec Python 3.12
et Java 21, il :
1. lance les tests (`tests/`), qui vérifient notamment que k=2 donne bien 167/167 ;
2. exécute `run_pipeline.py` (les 6 runs) ;
3. affiche les résultats dans le résumé du run ;
4. publie `mlflow.db`, `mlruns/` et `run.log` en artifact, gardés 14 jours.

Pour voir ces runs dans MLflow : télécharge l'artifact, décompresse-le dans un dossier, puis lance
`mlflow ui --backend-store-uri sqlite:///mlflow.db` depuis ce dossier.

Tests en local : `pip install pytest`, puis `python -m pytest tests`. Sous Windows, le test de
`gaussian_mixture` est ignoré (voir ci-dessous).

## Si ça bloque
- Crash de Java avec un fichier `hs_err_pid*.log` sur `gaussian_mixture` : sous Windows, le LAPACK
  natif de Spark peut faire planter la JVM. Le pipeline GitHub (Linux) n'est pas concerné.
- `java` introuvable / `JAVA_HOME is not set` → installe Java 17 (voir plus haut), puis ferme et
  rouvre le terminal.
- Message `modèle non sauvegardé` ou erreur `winutils` / `HADOOP_HOME` : sous Windows, Spark a
  besoin de `winutils.exe` pour écrire des fichiers. Les métriques sont quand même dans MLflow.
  Pour enlever le message : `python run_pipeline.py --no-model`.
- `Python worker failed to connect back` → avant de lancer : `set PYSPARK_PYTHON=python`.
