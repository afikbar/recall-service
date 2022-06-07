from dataclasses import dataclass
import json
from typing import Any, Dict, List, Tuple
import psycopg2
from kafka import KafkaProducer


_DEFAULT_HOST = 'localhost'
_DEFAULT_PORT = 5432
_DEFAULT_DBNAME = 'postgres'
_DEFAULT_USER = 'postgres'
_DEFAULT_PASSWORD = 'postgres'


class PsqlDal:
    def __init__(self,
                 host=_DEFAULT_HOST,
                 port=_DEFAULT_PORT,
                 dbname=_DEFAULT_DBNAME,
                 user=_DEFAULT_USER,
                 password=_DEFAULT_PASSWORD):
        self.conn = psycopg2.connect(user=user,
                                     password=password,
                                     host=host,
                                     port=port,
                                     database=dbname)

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query(self, sql: str) -> List[Tuple]:
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()

        return data

    def get_predicitions_and_actuals(self, version_id: int, segment_id: int = None) -> List[Tuple[int, int]]:
        sql = f"SELECT prediction_value, actual_value FROM Predictions P INNER JOIN Actual A ON P.record_id = A.record_id WHERE version_id = {version_id}"
        if segment_id is not None:
            sql += f" AND segment_id = {segment_id}"
        
        data = self.query(sql)

        return data


_DEFAULT_KAFKA_HOST = 'localhost'
_DEFAULT_KAFKA_PORT = 9092


class KafkaDal:
    def __init__(self,
                 host=_DEFAULT_KAFKA_HOST,
                 port=_DEFAULT_KAFKA_PORT):
        self.producer = KafkaProducer(bootstrap_servers=[f"{host}:{port}"])

    def close(self):
        self.producer.flush()
        self.producer.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def publish_json(self, topic: str, payload: Dict[str, Any]):
        message = json.dumps(payload).encode('utf-8')
        return self.producer.send(topic, message)

    def publish_recall(self, version_id: int, segment_id: int, recall: float, timeout: int = 10) -> bool:
        payload = {
            'version_id': version_id,
            'segment_id': segment_id,
            'recall': recall
        }
        futrue = self.publish_json('recall', payload)
        futrue.get(timeout=timeout)
        return futrue.succeeded()
