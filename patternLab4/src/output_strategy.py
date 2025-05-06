from abc import ABC, abstractmethod
from typing import List, Dict, Any
from kafka import KafkaProducer
import redis
import json
import sys


class OutputStrategy(ABC):

    @abstractmethod
    def output_data(self, data: List[Dict[str, Any]]) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class ConsoleOutputStrategy(OutputStrategy):

    def __init__(self, **kwargs):
        super().__init__()

    def output_data(self, data: List[Dict[str, Any]]) -> None:
        print("\n=== Виведення даних у консоль ===\n")
        for i, item in enumerate(data):
            print(f"Запис #{i + 1}:")
            for key, value in item.items():
                print(f"  {key}: {value}")
            print("-" * 50)

    def close(self) -> None:
        pass


class KafkaOutputStrategy(OutputStrategy):

    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "fire_incidents", **kwargs):

        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Підключено до Kafka серверів: {bootstrap_servers}")
        except Exception as e:
            print(f"Помилка підключення до Kafka: {e}")
            sys.exit(1)

    def output_data(self, data: List[Dict[str, Any]]) -> None:
        print(f"\n=== Відправлення даних у Kafka топік '{self.topic}' ===\n")

        for i, item in enumerate(data):
            try:
                future = self.producer.send(self.topic, item)
                future.get(timeout=10)

                if i % 100 == 0:
                    print(f"Відправлено {i + 1} записів у Kafka")

            except Exception as e:
                print(f"Помилка відправки запису в Kafka: {e}")

        print(f"Всього відправлено {len(data)} записів у Kafka")

    def close(self) -> None:
        try:
            self.producer.flush()
            self.producer.close()
            print("З'єднання з Kafka закрито")
        except Exception as e:
            print(f"Помилка при закритті з'єднання з Kafka: {e}")


class RedisOutputStrategy(OutputStrategy):

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0,
                 key_prefix: str = "fire_incident:", **kwargs):

        super().__init__()
        self.host = host
        self.port = port
        self.db = db
        self.key_prefix = key_prefix

        try:
            self.redis_client = redis.Redis(host=host, port=port, db=db)
            print(f"Підключено до Redis серверу: {host}:{port}, БД: {db}")
        except Exception as e:
            print(f"Помилка підключення до Redis: {e}")
            sys.exit(1)

    def output_data(self, data: List[Dict[str, Any]]) -> None:

        print(f"\n=== Збереження даних у Redis (хост: {self.host}:{self.port}, БД: {self.db}) ===\n")

        with self.redis_client.pipeline() as pipe:
            for i, item in enumerate(data):

                incident_id = item.get('INCIDENT_DISPATCH_ID', i)
                key = f"{self.key_prefix}{incident_id}"

                try:
                    pipe.set(key, json.dumps(item))

                    if (i + 1) % 100 == 0:
                        pipe.execute()
                        print(f"Збережено {i + 1} записів у Redis")

                except Exception as e:
                    print(f"Помилка збереження запису в Redis: {e}")

            pipe.execute()

        print(f"Всього збережено {len(data)} записів у Redis")

    def close(self) -> None:
        try:
            self.redis_client.close()
            print("З'єднання з Redis закрито")
        except Exception as e:
            print(f"Помилка при закритті з'єднання з Redis: {e}")


class OutputStrategyFactory:

    @staticmethod
    def create_strategy(strategy_type: str, config: Dict[str, Any]) -> OutputStrategy:
        if strategy_type == "console":
            return ConsoleOutputStrategy(**config)
        elif strategy_type == "kafka":
            return KafkaOutputStrategy(**config)
        elif strategy_type == "redis":
            return RedisOutputStrategy(**config)
        else:
            raise ValueError(f"Невідомий тип стратегії виводу: {strategy_type}")