# Indexation_Projet — Pipeline Kafka → Logstash → Elasticsearch → Kibana (API Open-Meteo)

## Prérequis
- Docker + Docker Compose installés
- Python 3 installé
- (optionnel) pip / venv

## Fichiers attendus dans le dossier
- `docker-compose.yml`
- `logstash.conf`
- `collector_openmeteo_to_kafka.py`

---

## Démarrer l’infrastructure (Kafka, Logstash, Elasticsearch, Kibana)

Dans le dossier du projet :
docker compose up -d

Créer le topic Kafka (une seule fois)
docker exec -it $(docker ps -qf name=kafka) \
kafka-topics --bootstrap-server kafka:29092 \
--create --topic meteo_api --partitions 1 --replication-factor 1


## Mapping ElasticSearch :

PUT _index_template/meteo_template
{
  "index_patterns": ["meteo-*"],
  "template": {
    "settings": {
      "index.max_ngram_diff": 20,
      "analysis": {
        "tokenizer": {
          "ngram_tokenizer": {
            "type": "ngram",
            "min_gram": 2,
            "max_gram": 10,
            "token_chars": ["letter", "digit"]
          }
        },
        "analyzer": {
          "ngram_analyzer": {
            "type": "custom",
            "tokenizer": "ngram_tokenizer",
            "filter": ["lowercase"]
          }
        }
      }
    },
    "mappings": {
      "properties": {
        "@timestamp": { "type": "date" },
        "meteo_hour": { "type": "date" },
        
        "source": { "type": "keyword" },

        "city": {
          "type": "text",
          "fields": {
            "keyword": { "type": "keyword" },
            "ngram": {
              "type": "text",
              "analyzer": "ngram_analyzer",
              "search_analyzer": "standard"
            }
          }
        },

        "search_text": {
          "type": "text",
          "fields": {
            "ngram": {
              "type": "text",
              "analyzer": "ngram_analyzer",
              "search_analyzer": "standard"
            }
          }
        },

        "location": { "type": "geo_point" },

        "temperature_2m": { "type": "float" },
        "precipitation": { "type": "float" },
        "wind_speed_10m": { "type": "float" }
      }
    }
  }
}
