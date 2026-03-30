from pydantic import Field, KafkaDsn
from pydantic_settings import BaseSettings


class KafkaSettings(BaseSettings, env_prefix='KAFKA_'):
    '''Настройки подключения к Kafka'''

    dsn: KafkaDsn = 'kafka://'  # Адрес подключения
    group_id: str = 'es_ok_users_group_id'  # Group ID для потребителя


class ElasticsearchSettings(BaseSettings, env_file='.env', extra='ignore'):
    '''Настройки Elasticsearch (заменяет Client Service)'''

    hosts: list[str] = Field(validation_alias='ELASTICSEARCH_HOSTS')  # Адреса ES
    index: str = 'ok_users'  # Имя индекса (Index - #{es_ok_users_index})
    batch_size: int = 1000  # Размер батча (Batch Size)
    max_bin_age: int = 30  # Максимальное время накопления (Max Bin Age)
    min_records: int = 1000  # Минимум записей для отправки
    max_records: int = 1000  # Максимум записей в батче
    retry_on_conflict: int = 10  # Количество повторных попыток (retry_on_conflict)


class Settings(BaseSettings, env_file='.env', extra='ignore'):
    '''Главные настройки приложения'''

    topic: str = 'ok_users'  # Топик Kafka (Topics - #{ok_users_topics})
    kafka: KafkaSettings = Field(KafkaSettings(), validation_alias='KAFKA')
    elasticsearch: ElasticsearchSettings = Field(ElasticsearchSettings())
    batch_processing_enabled: bool = Field(True, validation_alias='BATCH_PROCESSING_ENABLED')
    max_bins: int = Field(10, validation_alias='MAX_BINS')  # Maximum Number of Bins
    log_level: str = Field('INFO', validation_alias='LOG_LEVEL')


settings = Settings()
