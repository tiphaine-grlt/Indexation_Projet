# Indexation_Projet

## Démarrer l’infrastructure (Kafka, Logstash, Elasticsearch, Kibana)

Dans le dossier du projet :

```bash
docker compose up -d
```

Créer le topic Kafka (une seule fois)
```bash
docker exec -it $(docker ps -qf name=kafka) \
kafka-topics --bootstrap-server kafka:29092 \
--create --topic meteo_api \
--partitions 1 --replication-factor 1
```

## Installer les dépendances Python

```bash
pip install requests kafka-python
```
