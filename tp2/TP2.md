# TP 2 · Couche Silver avec Spark et Apache Iceberg

**Master 2 Data Engineering · Technologies Data Modernes**
**Étudiant(s) :** [Noms du groupe]
**Date :** [Date de la séance]

---

## 1. Objectif

Ce TP construit la couche Silver de la plateforme ShopNova avec un format de table ouvert, Apache Iceberg. On déploie un lakehouse local (Spark, catalogue REST Iceberg, MinIO), on écrit les commandes nettoyées dans une table partitionnée, puis on teste les mises à jour (MERGE INTO), le time travel, le rollback, l'évolution du schéma et du partitionnement, et la maintenance de la table.

## 2. Mise en place

Les données Bronze du TP 1 ont été téléchargées avec `download_bronze.py`, puis le lakehouse a été lancé avec Docker Compose (services `spark-iceberg`, `iceberg-rest` et `minio-tp2`).

Deux adaptations ont été nécessaires par rapport au cahier :
- Les images `minio/minio` et `minio/mc` ne sont plus téléchargeables. Nous avons réutilisé l'image `minio/minio` déjà présente sur la machine (`pull_policy: never`).
- Le service `mc` qui crée le bucket n'a pas pu être utilisé. Le bucket `warehouse` a été créé avec un petit script Python (`create_bucket.py`).

Les conteneurs Hadoop qui occupaient le port 9000 ont été arrêtés pendant le TP.

**Capture 1 : `docker compose ps` avec les trois services démarrés**

![Capture 1](captures/01_compose_ps.png)

**Capture 2 : console MinIO, bucket `warehouse` avant la création de la table**

![Capture 2](captures/02_warehouse_vide.png)

## 3. Étape 2 : table Silver

La table `demo.silver.orders` est partitionnée par jour sur `order_ts` (`days(order_ts)`) et compressée en zstd. Le nettoyage appliqué aux données Bronze est le suivant : suppression des doublons sur `order_id`, filtre sur `amount > 0`, typage de `order_ts` et `amount`, normalisation de `city` (espaces et majuscules) et de `status` (majuscules), et suppression de la colonne technique `_batch_id`.

**Capture 3 : sortie de la cellule 2 (nombre de commandes par statut)**

![Capture 3](captures/03_statuts.png)

**Capture 4 : tables de métadonnées `.snapshots` et `.files`**

![Capture 4](captures/04_metadonnees.png)

**Capture 5 : console MinIO, dossiers `data/` et `metadata/` de la table**

![Capture 5](captures/05_minio_table.png)

### Questions

**1. Combien de dossiers `order_ts_day=…` dans MinIO ? Pourquoi ce nombre ?**

Il y a [N] dossiers. Les commandes générées au TP 1 ont des dates réparties sur 90 jours, donc on obtient un dossier par jour qui contient au moins une commande (environ 90 ou 91). Avec 200 000 lignes réparties au hasard, chaque jour reçoit environ 2 200 commandes, donc aucun jour n'est vide.

**2. Contenu du dernier fichier `*.metadata.json`**

Le fichier contient le schéma de la table avec l'identifiant de chaque colonne (`order_id`, `customer_id`, etc.), la spécification de partition (transformation `day` appliquée à `order_ts`, champ nommé `order_ts_day`), la liste des snapshots et l'identifiant du snapshot courant (`current-snapshot-id` = [ID]). On y trouve aussi les propriétés de la table, dont la compression zstd.

**Capture 6 : contenu du fichier metadata (schéma, partitionnement, snapshot courant)**

![Capture 6](captures/06_metadata_json.png)

**3. Le partitionnement est-il exploité alors que la colonne de partition n'est pas citée ?**

Oui. Iceberg utilise le partitionnement caché (hidden partitioning) : il connaît la transformation `days(order_ts)` et traduit automatiquement le filtre sur `order_ts` en un filtre sur les partitions concernées. Les fichiers des autres jours sont ignorés sans les lire (élagage de partitions). L'utilisateur n'a pas besoin de connaître la colonne de partition ni de filtrer dessus, contrairement au partitionnement Hive du TP 1 où `ingestion_date` était une colonne à citer.

**Capture 7 : plan d'exécution `EXPLAIN` avec le filtre sur `order_ts`**

![Capture 7](captures/07_explain.png)

## 4. Étape 3 : MERGE INTO

Un lot de mises à jour a été simulé : 1 000 commandes `PAID` passent à `SHIPPED` et 500 nouvelles commandes sont ajoutées. Le MERGE a d'abord été exécuté en copy-on-write (mode par défaut), puis en merge-on-read.

**Capture 8 : résumé des snapshots après le MERGE copy-on-write**

![Capture 8](captures/08_merge_cow.png)

**Capture 9 : résumé des snapshots et types de fichiers après le MERGE merge-on-read**

![Capture 9](captures/09_merge_mor.png)

### Comparaison copy-on-write / merge-on-read

| Mesure | Copy-on-write | Merge-on-read |
|---|---|---|
| Fichiers de données ajoutés | [X] | [X] |
| Fichiers de données supprimés (réécrits) | [X] | [X] |
| Fichiers de suppression ajoutés | 0 | [X] |
| Durée du MERGE (s) | [X] | [X] |

### Questions

**1. Combien de fichiers de données ont été réécrits pour modifier 1 000 lignes ?**

En copy-on-write, [X] fichiers ont été réécrits. Les 1 000 lignes modifiées sont réparties dans de nombreuses partitions, donc presque chaque fichier touché doit être réécrit en entier pour changer quelques lignes. En merge-on-read, [X] fichiers de données ont été réécrits : Iceberg écrit seulement un petit fichier de suppression et les nouvelles versions des lignes.

**2. Quel type de fichier apparaît en merge-on-read ? Impact sur les lectures ?**

Des fichiers de suppression de position (valeur 1 dans la colonne `content`, contre 0 pour les fichiers de données). Ils indiquent quelles lignes d'un fichier de données sont supprimées. Les écritures sont plus rapides, mais les lectures sont plus lentes, car le moteur doit appliquer ces suppressions à chaque requête. Une compaction (`rewrite_data_files`) les intègre aux fichiers de données.

**3. Le MERGE est-il idempotent si on le relance avec la même vue ?**

Oui, avec la même vue. Après le premier passage, les commandes concernées sont déjà `SHIPPED`, donc la condition `t.status <> s.status` est fausse et la clause `UPDATE` ne s'applique plus. Les 500 nouvelles commandes existent maintenant dans la table, elles correspondent donc à la clause `MATCHED` et ne sont pas insérées une deuxième fois. Sans la condition `t.status <> s.status`, chaque relance réécrirait les mêmes lignes pour rien (nouveaux fichiers, nouveau snapshot, `_ingested_at` modifié). Le nombre de lignes avant et après la relance est resté identique ([N] lignes).

## 5. Étape 4 : time travel, rollback, évolution

**Capture 10 : time travel, répartition des statuts avant le MERGE (`VERSION AS OF`) et actuelle**

![Capture 10](captures/10_time_travel.png)

**Capture 11 : DELETE puis rollback, nombre de lignes à chaque étape**

![Capture 11](captures/11_rollback.png)

**Capture 12 : évolution du schéma (`DESCRIBE TABLE` après les trois `ALTER`)**

![Capture 12](captures/12_schema.png)

**Capture 13 : partitions après le passage de jour à mois**

![Capture 13](captures/13_partitions_mois.png)

### Questions

**1. Après le RENAME, comment Iceberg lit-il les anciens fichiers qui contiennent « city » ?**

Iceberg identifie les colonnes par un identifiant unique, pas par leur nom. Cet identifiant est stocké dans les métadonnées et dans les fichiers Parquet. Renommer une colonne ne change que son nom dans les métadonnées : l'identifiant reste le même, donc Iceberg associe la colonne « city » des anciens fichiers à `delivery_city`. Aucun fichier n'est réécrit.

**2. Où sont écrites les nouvelles données après le changement de partitionnement ? Les anciennes ont-elles été déplacées ?**

Les nouvelles données sont écrites selon la nouvelle spécification, par mois (`order_ts_month`). Les anciennes données ne sont pas déplacées ni réécrites : elles gardent leur partitionnement par jour. La table contient donc plusieurs spécifications de partition à la fois, et Iceberg sait planifier les requêtes sur les deux.

**3. Pourquoi DECIMAL(10,2) vers DECIMAL(12,2) est-il autorisé, mais pas l'inverse ?**

Élargir la précision est sans risque : toutes les valeurs existantes tiennent dans le nouveau type, avec la même échelle, donc il n'y a pas besoin de réécrire les fichiers. Réduire la précision pourrait faire perdre des valeurs trop grandes pour le nouveau type, c'est pourquoi Iceberg le refuse (voir le test dans le notebook).

## 6. Étape 5 : maintenance et export

La compaction a regroupé les petits fichiers, `expire_snapshots` a supprimé les anciens snapshots en gardant les 5 derniers, et `remove_orphan_files` a nettoyé les fichiers qui n'appartiennent plus à aucun snapshot. Par défaut, cette dernière procédure ne supprime que les fichiers de plus de 3 jours, donc elle n'a probablement rien supprimé ici ([X] fichiers supprimés).

**Capture 14 : résultats de la compaction et de `expire_snapshots`**

![Capture 14](captures/14_maintenance.png)

**Capture 15 : export Parquet (nombre de lignes exportées)**

![Capture 15](captures/15_export.png)

### Évolution du nombre de fichiers et de snapshots

| Étape | Fichiers | Snapshots | Lignes |
|---|---|---|---|
| Après écriture Silver | [X] | [X] | [X] |
| Après MERGE copy-on-write | [X] | [X] | [X] |
| Après MERGE merge-on-read | [X] | [X] | [X] |
| Après re-MERGE identique | [X] | [X] | [X] |
| Après DELETE + rollback | [X] | [X] | [X] |
| Après évolution du schéma | [X] | [X] | [X] |
| Après changement de partitionnement | [X] | [X] | [X] |
| Après compaction | [X] | [X] | [X] |
| Après expire_snapshots | [X] | [X] | [X] |

(Ces valeurs sont données par le récapitulatif en fin de notebook.)

## 7. Arborescence de la table

```
warehouse/silver/orders/
├── data/                                  <- fichiers Parquet (les données)
│   ├── order_ts_day=2026-07-15/           <- un dossier par jour (ancien partitionnement)
│   │   └── 00000-...-00001.parquet
│   ├── order_ts_month=2026-10/            <- nouveau partitionnement, après le changement
│   │   └── ...parquet
│   └── ...
└── metadata/                              <- la couche de métadonnées Iceberg
    ├── 00000-....metadata.json            <- version 0 de la table (schéma, partitions, snapshots)
    ├── 00001-....metadata.json            <- chaque changement crée un nouveau fichier
    ├── ...                                <- le catalogue pointe vers le plus récent
    ├── snap-....avro                      <- liste de manifestes d'un snapshot
    └── ....-m0.avro                       <- manifeste : liste de fichiers de données + statistiques
```

Lecture d'une requête : le catalogue donne le dernier `metadata.json`, qui désigne le snapshot courant, dont la liste de manifestes mène aux manifestes, qui listent les fichiers de données à lire (avec leurs statistiques pour éliminer les fichiers inutiles).

**Capture 16 : console MinIO, contenu du dossier `metadata/`**

![Capture 16](captures/16_minio_metadata.png)

## 8. Projet

[À compléter : tables Silver du sujet du groupe en Iceberg, avec la clé de chaque table, le partitionnement choisi et justifié, et les règles de nettoyage documentées.]

## 9. Conclusion

Ce TP a montré ce qu'apporte un format de table ouvert par rapport à un simple dossier de fichiers Parquet : des mises à jour fiables avec MERGE INTO, un historique complet (time travel et rollback), une évolution du schéma et du partitionnement sans réécriture des données, et des outils de maintenance. Le choix entre copy-on-write et merge-on-read dépend du compromis entre vitesse d'écriture et vitesse de lecture. La table Silver exportée en Parquet servira de source au TP 3.
