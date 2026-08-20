# Operations Runbook

## Inspecting Kafka
```bash
chmod +x kafka-scripts/kafka-inspect.sh

# List Consumer Lag
./kafka-scripts/kafka-inspect.sh describe-group email-validation-workers
# View messages
./kafka-scripts/kafka-inspect.sh consume-topic email-validation
```

## Draining DLQ
When messages end up in `email-validation-dlq`, you can view the failed events directly. Once fixed:
- You'll typically replay them to the regular loop by using a script to read `email-validation-dlq` and write back to `email-validation`.

## Replaying Offsets
If you want to re-process historical messages from the beginning:
1. Turn off your consumers (e.g. stop `php artisan`).
2. Run offset reset:
```bash
./kafka-scripts/kafka-inspect.sh reset-offset email-validation-workers email-validation
```
3. Turn consumers back on. They will start consuming from offset 0.

## Complete Data Reset
To completely wipe Kafka Topics, DB content and logs, shut down the cluster and clean up volumes:
```bash
docker compose down -v
docker compose up -d
```
