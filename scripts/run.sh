#!/bin/bash

# Проверяем, передано ли имя entrypoint
if [ -z "$1" ]; then
    echo "Ошибка: Укажите имя entrypoint. Пример: ./run.sh my-script"
    exit 1
fi

ENTRYPOINT="$1"
# Создаем имя лога (например: run-flink-table-upsert-job_20260603_122700.log)
LOG_NAME="${ENTRYPOINT}_$(date +%Y%m%d_%H%M%S).log"

echo "Запуск entrypoint: $ENTRYPOINT"
echo "Лог пишется в файл: $LOG_NAME"

# Запуск команды с передачей всех остальных аргументов (если они есть)
uv run "$ENTRYPOINT" "${@:2}" > "$LOG_NAME" 2>&1
