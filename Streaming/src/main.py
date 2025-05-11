# print -> logging 변경 필요
from client import Api
from kafka import KafkaProducer
from json import dumps
from kafka.errors import KafkaError


BROKER_LIST = ["15.164.218.105:9091", "15.164.218.105:9092", "15.164.218.105:9093"]
TOPIC_NAME = "transction"

class DataGenerator:
    """
    Description:
        API Data to Kafka
    Params:
        - topic_name (str, Required) : Topic명
        - brokers (List[str], Required) : Broker 목록
        - retries (Optional[int]) : 재시도 횟수
    """
    def __init__(self, topic_name, brokers, retries = 3):
        self.topic_name = topic_name
        self.brokers = brokers
        self.retries = retries

    def publish_prices(self):
        producer = KafkaProducer(
            acks=0,
            bootstrap_servers=self.brokers,
            retries = self.retries,
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        try:
            tickers = Api.get_tickers()
            if tickers is None:
                raise Exception("Ticker 조회가 안되었습니다.")
            
            for ticker in tickers:
                data = Api.get_price(ticker)
                producer.send(self.topic_name, data)
                print(data)

            producer.flush()
        
        except KafkaError as e:
            print(f"Kafka error occurred: {e}")
        finally:
            producer.close() # kafka-python는 __enter__() / __exit__() 메서드를 구현하고 있지 않기 때문에 with 문을 사용할 수 없습니다. 따라서 close 선언 필요


if __name__ == "__main__":
    generator = DataGenerator(TOPIC_NAME, BROKER_LIST)
    generator.publish_prices()