# S3 Usage Collector

Скрипт для сбора и агрегации S3 usage-статистики.

Все параметры передаются **через аргументы командной строки** в формате `KEY=VALUE`.

---

## Конфигурация

Конфигурация находится в файле `s3_usage_collector/config.py` и отвечает за:

- определение корневых директорий проекта;
- пути для результатов и бэкапов;
- чтение S3-параметров из окружения (если требуется).

### Переменные конфигурации

- `RESULTS_DIR`
  
  Директория для записи итогового агрегированного файла `summarized_data.json`.
  Формируется относительно `RES_ROOT_DIR`.

- `USAGE_SUMMARY_FILE`
  
  Полный путь к итоговому файлу с агрегированной статистикой.
  По умолчанию файл называется `summarized_data.json` и перезаписывается при каждом запуске.

- `USAGE_BACKUP_DIR`
  
  Директория для хранения бэкапов итогового файла.
  При каждом запуске создаётся копия вида:
  `summarized_data_YYYY-MM-DD_HH-MM-SS.json`.

- `STATS_CHUNKS_DIR`
  
  Директория для сохранения сырых S3 usage-чанков в формате JSON.
  Используется только при включённом параметре `S3_SAVE_STATS_CHUNKS=TRUE`.
  Формируется относительно `ROOT_DIR`.

Используемые директории:

- `results/` — директория для итогового файла `summarized_data.json` (располагается относительно `RES_ROOT_DIR`)
- `results/stats/chunks/` — сырые usage-чанки в формате JSON (пишутся относительно `ROOT_DIR`, опционально)
- `backups/` — бэкапы итогового файла `summarized_data.json` с датой и временем запуска (располагается относительно `ROOT_DIR`)

---

## Параметры запуска

Все параметры передаются через командную строку в формате `KEY=VALUE`.

### Обязательные параметры

- `PUBLIC_S3_KEY`  
  Access key для S3.

- `SECRET_S3_KEY`  
  Secret key для S3.

- `S3_SERVERNOHTTPS`  
  URL S3-шлюза без дополнительных параметров.

  Примеры:
  - `https://s3.example.com`
  - `http://10.0.0.1:8080`

---

### Опциональные параметры

- `S3_USAGE_PERIOD`  
  Длина usage-периода в секундах.  
  По умолчанию: `3600`.

- `S3_REMOVE_STATS_ITEMS`  
  Удалять usage-объекты после обработки.  
  Значения: `TRUE` / `FALSE`.  
  По умолчанию: `FALSE`.

- `S3_SAVE_STATS_CHUNKS`  
  Сохранять ли сырые usage-чанки в `results/stats/chunks/`.  
  Значения: `TRUE` / `FALSE`.  
  По умолчанию: `FALSE`.

Преобразование значений `TRUE` / `FALSE` в `bool` выполняется внутри `UsageCollector`.


## Использование параметров в коде

```python
access_key = params.get('PUBLIC_S3_KEY')
secret_key = params.get('SECRET_S3_KEY')
host = params.get('S3_SERVERNOHTTPS')

s3_usage_period_seconds = params.get('S3_USAGE_PERIOD', 3600)
remove_items = params.get('S3_REMOVE_STATS_ITEMS', False)
save_chunks = params.get('S3_SAVE_STATS_CHUNKS', False)
```

---

## Установка зависимостей

Рекомендуется использовать виртуальное окружение:

```bash
python -m venv .venv
source .venv/bin/activate
```

Установка зависимостей:

```bash
pip install -r requirements.txt
```

Минимальный набор зависимостей:

```text
curl_cffi==0.9.0b2
loguru
python-dotenv
```

---

## Запуск

Пример минимального запуска скрипта:
```bash
python main.py \
  PUBLIC_S3_KEY=$PUBLIC_S3_KEY \
  S3_SERVERNOHTTPS=$S3_SERVERNOHTTPS \
  SECRET_S3_KEY=$SECRET_S3_KEY

```
Пример запуска скрипта:

```bash
python main.py \
  PUBLIC_S3_KEY=$PUBLIC_S3_KEY \
  S3_SERVERNOHTTPS=$S3_SERVERNOHTTPS \
  SECRET_S3_KEY=$SECRET_S3_KEY \
  S3_USAGE_PERIOD=3600 \
  RESULTS_DIR=$RESULTS_DIR \ 
  STATS_CHUNKS_DIR=$STATS_CHUNKS_DIR \ 
  USAGE_BACKUP_DIR=$USAGE_BACKUP_DIR \ 
  USAGE_SUMMARY_FILE=$USAGE_SUMMARY_FILE \
  S3_REMOVE_STATS_ITEMS=TRUE \
  S3_SAVE_STATS_CHUNKS=FALSE \
  
```

---

## Результаты работы

* `results/summarized_data.json`
  Итоговый агрегированный файл. Перезаписывается при каждом запуске.

* `results/usage_backups/`
  Бэкапы итогового файла за каждый запуск: `summarized_data_YYYY-MM-DD_HH-MM-SS.json`

* `results/stats/chunks/`
  Сырые usage-чанки (если включён `S3_SAVE_STATS_CHUNKS=TRUE`).
