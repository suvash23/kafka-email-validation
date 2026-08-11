#!/usr/bin/env bash
# =============================================================================
# Kafka Inspection Scripts
# These are helper scripts that wrap kafka CLI tools running inside Docker.
# Run from your host machine (not inside the container).
# =============================================================================

set -euo pipefail

KAFKA_CONTAINER="kafka"

# Text formatting
BOLD="\033[1m"
GREEN="\033[32m"
YELLOW="\033[33m"
CYAN="\033[36m"
RESET="\033[0m"

print_header() {
    echo -e "\n${BOLD}${CYAN}==============================${RESET}"
    echo -e "${BOLD}${CYAN} $1${RESET}"
    echo -e "${BOLD}${CYAN}==============================${RESET}\n"
}

# ---------------------------------------------------------------------------
# List all topics
# ---------------------------------------------------------------------------
list_topics() {
    print_header "📋 All Kafka Topics"
    docker exec "$KAFKA_CONTAINER" \
        kafka-topics --bootstrap-server kafka:9092 --list
}

# ---------------------------------------------------------------------------
# Describe a topic (shows partitions, replicas, leader, ISR)
# Usage: ./kafka-inspect.sh describe-topic email-validation
# ---------------------------------------------------------------------------
describe_topic() {
    local topic="${1:-email-validation}"
    print_header "🔍 Topic Description: $topic"

    # KAFKA CONCEPT: Topic Description Output Explained:
    #   Topic:            topic name
    #   PartitionCount:   how many partitions the topic has
    #   ReplicationFactor: how many brokers hold a copy of each partition
    #   Partition:        partition number (0-based)
    #   Leader:           broker ID that currently handles reads/writes
    #   Replicas:         all brokers that have a copy of this partition
    #   Isr:              In-Sync Replicas — followers fully caught up to leader

    docker exec "$KAFKA_CONTAINER" \
        kafka-topics \
            --bootstrap-server kafka:9092 \
            --describe \
            --topic "$topic"
}

# ---------------------------------------------------------------------------
# List consumer groups
# ---------------------------------------------------------------------------
list_consumer_groups() {
    print_header "👥 Consumer Groups"
    docker exec "$KAFKA_CONTAINER" \
        kafka-consumer-groups \
            --bootstrap-server kafka:9092 \
            --list
}

# ---------------------------------------------------------------------------
# Describe a consumer group (shows partition assignment + lag)
# Usage: ./kafka-inspect.sh describe-group email-validation-workers
#
# KAFKA CONCEPT: Consumer Group Lag
#   LAG = (Latest Offset in partition) - (Consumer's committed offset)
#   LAG = 0 → Consumer is caught up, no pending messages
#   LAG > 0 → Consumer is behind, messages waiting to be processed
# ---------------------------------------------------------------------------
describe_consumer_group() {
    local group="${1:-email-validation-workers}"
    print_header "🔍 Consumer Group: $group"
    docker exec "$KAFKA_CONTAINER" \
        kafka-consumer-groups \
            --bootstrap-server kafka:9092 \
            --describe \
            --group "$group"
}

# ---------------------------------------------------------------------------
# Read messages from a topic (from the beginning)
# Usage: ./kafka-inspect.sh consume-topic email-validation
# ---------------------------------------------------------------------------
consume_topic() {
    local topic="${1:-email-validation}"
    print_header "📨 Reading messages from: $topic (Ctrl+C to stop)"
    
    # --from-beginning: start from offset 0 (re-read all stored messages)
    # --property print.key=true: also show the message key (partition routing key)
    # --property print.offset=true: show the offset of each message
    docker exec -it "$KAFKA_CONTAINER" \
        kafka-console-consumer \
            --bootstrap-server kafka:9092 \
            --topic "$topic" \
            --from-beginning \
            --property print.key=true \
            --property print.offset=true \
            --property print.partition=true \
            --property key.separator=" | KEY: " \
            --formatter kafka.tools.DefaultMessageFormatter
}

# ---------------------------------------------------------------------------
# Produce a test message (for manual testing without the Laravel app)
# Usage: ./kafka-inspect.sh produce-test email-validation
# ---------------------------------------------------------------------------
produce_test() {
    local topic="${1:-email-validation}"
    print_header "✉️  Producing test message to: $topic"
    echo '{"event_id":"test-001","event_type":"email.validation.requested","email":"test@example.com","requested_at":"2024-01-01T00:00:00Z"}' | \
        docker exec -i "$KAFKA_CONTAINER" \
            kafka-console-producer \
                --bootstrap-server kafka:9092 \
                --topic "$topic" \
                --property "parse.key=true" \
                --property "key.separator=:"
    echo -e "\n${GREEN}✓ Message sent${RESET}"
}

# ---------------------------------------------------------------------------
# Show broker metadata
# ---------------------------------------------------------------------------
broker_info() {
    print_header "🖥️  Broker Info"
    docker exec "$KAFKA_CONTAINER" \
        kafka-broker-api-versions \
            --bootstrap-server kafka:9092 \
            2>&1 | head -5
    echo ""
    docker exec "$KAFKA_CONTAINER" \
        kafka-metadata-quorum \
            --bootstrap-server kafka:9092 \
            describe --status
}

# ---------------------------------------------------------------------------
# Reset consumer group offset (useful for re-processing)
# Usage: ./kafka-inspect.sh reset-offset email-validation-workers email-validation
# ---------------------------------------------------------------------------
reset_offset() {
    local group="${1:-email-validation-workers}"
    local topic="${2:-email-validation}"
    print_header "⏮️  Reset offset for group: $group on topic: $topic"
    echo -e "${YELLOW}WARNING: This will cause re-processing of all messages!${RESET}"
    read -p "Are you sure? (yes/no): " confirm
    if [[ "$confirm" == "yes" ]]; then
        docker exec "$KAFKA_CONTAINER" \
            kafka-consumer-groups \
                --bootstrap-server kafka:9092 \
                --group "$group" \
                --topic "$topic" \
                --reset-offsets \
                --to-earliest \
                --execute
        echo -e "${GREEN}✓ Offset reset complete${RESET}"
    else
        echo "Aborted."
    fi
}

# ---------------------------------------------------------------------------
# Main dispatcher
# ---------------------------------------------------------------------------
case "${1:-help}" in
    list-topics)        list_topics ;;
    describe-topic)     describe_topic "${2:-email-validation}" ;;
    list-groups)        list_consumer_groups ;;
    describe-group)     describe_consumer_group "${2:-email-validation-workers}" ;;
    consume-topic)      consume_topic "${2:-email-validation}" ;;
    produce-test)       produce_test "${2:-email-validation}" ;;
    broker-info)        broker_info ;;
    reset-offset)       reset_offset "${2:-email-validation-workers}" "${3:-email-validation}" ;;
    help|*)
        echo -e "\n${BOLD}Usage: ./kafka-inspect.sh <command> [args]${RESET}\n"
        echo "  list-topics                          List all topics"
        echo "  describe-topic <topic>               Describe topic (partitions, replicas)"
        echo "  list-groups                          List consumer groups"
        echo "  describe-group <group>               Show group lag + partition assignment"
        echo "  consume-topic <topic>                Read all messages from a topic"
        echo "  produce-test <topic>                 Send a test JSON message"
        echo "  broker-info                          Show broker metadata"
        echo "  reset-offset <group> <topic>         Reset consumer to earliest offset"
        echo ""
        ;;
esac
