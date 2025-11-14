# Ingestion de CSV massifs vers Kafka avec Spring Boot

Ce module illustre comment lire un fichier CSV de ~100 000 lignes, le valider ligne par ligne et pousser les enregistrements valides vers un topic Kafka à l'aide de Spring Boot 3 + Spring for Apache Kafka.

## Principe

1. **Lecture streaming** — `CsvStreamProducer` s'appuie sur `Commons CSV` et `Files.newBufferedReader` pour itérer en flux sur le fichier sans le charger en mémoire.
2. **Validation** — chaque ligne est vérifiée (champs obligatoires, format de date ISO, e-mail valide, points de fidélité positifs). Les lignes rejetées sont comptées et loguées.
3. **Publication Kafka** — les lignes valides sont converties en `CustomerRecord` et publiées via `KafkaTemplate` sur le topic défini par `app.kafka.customer-topic`.

## Démarrer le producteur

```bash
cd kafka-csv-stream
mvn spring-boot:run \
  -Dspring-boot.run.arguments="--spring.kafka.bootstrap-servers=localhost:9092 --app.kafka.customer-topic=customers.csv.ingested" \
  -Dcsv.file="/chemin/vers/clients-100k.csv"
```

- `csv.file` pointe vers votre fichier CSV (avec en-têtes `id,first_name,last_name,email,signup_date,loyalty_points`).
- Lancez un Kafka local (par ex. `confluentinc/cp-kafka`) avant d'exécuter l'application.

## Générer un gros fichier de test

```bash
python3 scripts/generate_customers.py --rows 100000 --output /tmp/clients-100k.csv
```

_(Le script n'est pas fourni mais l'interface suggérée montre comment brancher votre propre générateur.)_

## Structure principale

- `KafkaCsvApplication` : lance Spring Boot et démarre l'ingestion via un `CommandLineRunner`.
- `CsvStreamProducer` : lit, valide et publie les enregistrements.
- `KafkaProducerConfig` : configure `KafkaTemplate` (JSON) pointant sur le cluster spécifié.

Vous pouvez remplacer `KafkaTemplate` par `ReactiveKafkaProducerTemplate` si vous souhaitez une approche réactive/back-pressure.
