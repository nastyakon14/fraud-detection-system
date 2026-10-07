# Real-Time Fraud Detection System

Сервис для скоринга фродовых транзакций в режиме потока через Kafka.

Проект подготовлен в учебных целях (MLOps, МТС ШАД). Датасеты взяты из соревнования:
[Teta ML 1 2025](https://www.kaggle.com/competitions/teta-ml-1-2025).

## Архитектура

### Компоненты

1. `interface` (Streamlit, порт `8501`)
   - принимает CSV-файл с транзакциями;
   - генерирует `transaction_id` для каждой строки;
   - отправляет каждую транзакцию в Kafka-топик `transactions`.

2. `fraud_detector`
   - читает сообщения из `transactions`;
   - выполняет препроцессинг признаков;
   - считает `score` и `fraud_flag` моделью CatBoost (`fraud_detector/models/my_catboost.cbm`);
   - отправляет результат в Kafka-топик `scores`.

3. `score_writer`
   - читает топик `scores` (`transaction_id`, `score`, `fraud_flag`);
   - записывает каждую транзакцию в PostgreSQL, таблицу `transaction_scores`.

4. `postgres`
   - хранит витрину скоров в той же сети, что и остальные сервисы.

5. Kafka-инфраструктура
   - `zookeeper`;
   - `kafka`;
   - `kafka-setup` (создает топики `transactions` и `scores`);
   - `kafka-ui` (порт `8080`) для проверки сообщений.

В Streamlit по кнопке «Посмотреть результаты» показываются 10 последних транзакций с `fraud_flag = 1` и гистограмма скоров последних 100 транзакций.

## Структура проекта

```text
.
├── docker-compose.yaml
├── README.md
├── interface/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py
│   └── .streamlit/config.toml
├── fraud_detector/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app/app.py
│   ├── src/preprocessing.py
│   ├── src/scorer.py
│   ├── models/my_catboost.cbm
│   └── train_data/train.csv
├── score_writer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
└── postgres/
    └── init.sql
```

## Требования

- Docker 20.10+
- Docker Compose v2
- Свободные порты: `8501`, `8080`, `9095`, `2181`, `5432`

## Подготовка train.csv перед запуском

`fraud_detector` использует `train.csv` для расчета статистик препроцессинга на старте контейнера.

1. Скачайте `train.csv` из соревнования:
   [Teta ML 1 2025](https://www.kaggle.com/competitions/teta-ml-1-2025/data)
2. Создайте локальную папку `fraud_detector/train_data/` (если ее нет).
3. Положите файл строго по пути:
   `fraud_detector/train_data/train.csv`

Итоговая структура должна быть такой:

```text
fraud_detector/
  train_data/
    train.csv
```

## Запуск

```bash
git clone https://github.com/nastyakon14/fraud-detection-system.git
cd fraud-detection-system
docker compose up --build
```

После запуска:
- Streamlit UI: `http://localhost:8501`
- Kafka UI: `http://localhost:8080`

Первый запуск `fraud_detector` занимает несколько минут: сервис один раз обрабатывает весь `train.csv`. Дождитесь в логах строки `Starting Kafka ML scoring service` и `Train data processed`.

Логи:

```bash
docker compose logs fraud_detector
docker compose logs score_writer
docker compose logs interface
```

## Как проверить работоспособность

### 1. Подготовьте входной файл

Нужен CSV со строками транзакций в формате `test.csv` соревнования, не файл сабмита с колонками `index,prediction`.

Ожидаемые колонки:

```text
transaction_time, merch, cat_id, amount, name_1, name_2, gender, street,
one_city, us_state, post_code, lat, lon, population_city, jobs,
merchant_lat, merchant_lon
```

Для первой проверки достаточно 50–100 строк.

### 2. Отправьте файл через интерфейс

1. Откройте `http://localhost:8501`.
2. Загрузите CSV.
3. Нажмите кнопку отправки этого файла.

### 3. Проверьте сообщения в Kafka UI

Откройте `http://localhost:8080`.

В топике `transactions` у каждого сообщения есть идентификатор и поля транзакции:

```json
{
  "transaction_id": "uuid",
  "data": {
    "transaction_time": "2023-01-01 12:30:00",
    "amount": 150.5
  }
}
```

В топике `scores` у каждого сообщения есть скор модели и флаг фрода:

```json
{
  "transaction_id": "uuid",
  "score": 0.995,
  "fraud_flag": 1
}
```

Сервис отработал корректно, если:

- число сообщений в `scores` совпадает с числом отправленных строк;
- `score` лежит в диапазоне от 0 до 1;
- `fraud_flag` равен 0 или 1;
- в `docker compose logs fraud_detector` нет строк `Error processing message`, а есть `Prediction complete`;
- в `docker compose logs score_writer` есть строки `Saved transaction`.

### 4. Посмотрите результаты в интерфейсе

На странице `http://localhost:8501` нажмите «Посмотреть результаты».

- Таблица показывает до 10 последних транзакций с `fraud_flag = 1`. Если таких строк нет, интерфейс пишет, что фродовых транзакций пока нет.
- Под таблицей строится гистограмма скоров последних 100 транзакций. Если в базе меньше 100 строк, гистограмма строится по всем имеющимся.
