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
   - отправляет результат в Kafka-топик `scoring`.

3. Kafka-инфраструктура
   - `zookeeper`;
   - `kafka`;
   - `kafka-setup` (создает топики `transactions` и `scoring`);
   - `kafka-ui` (порт `8080`) для проверки сообщений.

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
└── fraud_detector/
    ├── Dockerfile
    ├── requirements.txt
    ├── app/app.py
    ├── src/preprocessing.py
    ├── src/scorer.py
    ├── models/my_catboost.cbm
    └── train_data/train.csv
```

## Требования

- Docker 20.10+
- Docker Compose v2
- Свободные порты: `8501`, `8080`, `9095`, `2181`

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

Логи:

```bash
docker compose logs fraud_detector
docker compose logs interface
```
