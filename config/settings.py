from typing import Dict, List, Optional

from pydantic import BaseModel, Field, KafkaDsn
from pydantic_settings import BaseSettings


class KafkaSettings(BaseSettings, env_prefix='KAFKA_'):
    '''Настройки подключения к Kafka'''

    dsn: KafkaDsn = 'kafka://'
    group_id: str = 'es_ok_users_group_id'


class ElasticsearchSettings(BaseSettings, env_file='.env', extra='ignore'):
    '''Настройки Elasticsearch'''

    hosts: list[str] = Field(validation_alias='ELASTICSEARCH_HOSTS')
    index: str = 'ok_users'
    batch_size: int = 1000
    max_bin_age: int = 30
    min_records: int = 1000
    max_records: int = 1000
    retry_on_conflict: int = 10


class RecordProcessorSettings(BaseModel):
    '''Настройки обработки записей'''

    # Поля для форматирования дат
    fields_to_format: List[str] = ['last_online', 'registered_date']

    # Добавлять ли timestamp
    add_timestamp: bool = True

    # Поле для разбиения даты рождения (None - отключить)
    birthday_field: Optional[str] = 'birthday'

    # Поля для обрезания строк {поле: максимальная_длина}
    truncate_fields: Dict[str, int] = {'inspired_by': 256}

    # Поле для копирования хэша аватара (None - отключить)
    avatar_hash_field: Optional[str] = 'avatar_phash'

    # Целевое поле для хэша аватара
    avatar_hash_target: str = 'hash'

    # Схема данных (Pydantic модель)
    schema_class: str = 'config.schemas.OKUsers'


class MergerSettings(BaseModel):
    '''Настройки батчирования'''

    min_records: int = 1000
    max_records: int = 1000
    max_bin_age: int = 30
    max_bins: int = 10


class Settings(BaseSettings, env_file='.env', extra='ignore'):
    '''Главные настройки приложения'''

    topic: str = 'ok-users'
    kafka: KafkaSettings = Field(KafkaSettings(), validation_alias='KAFKA')
    elasticsearch: ElasticsearchSettings = Field(ElasticsearchSettings())

    # Настройки процессора записей
    record_processor: RecordProcessorSettings = Field(RecordProcessorSettings())

    # Настройки мерджера
    merger: MergerSettings = Field(MergerSettings())

    batch_processing_enabled: bool = Field(True, validation_alias='BATCH_PROCESSING_ENABLED')
    log_level: str = Field('INFO', validation_alias='LOG_LEVEL')


settings = Settings()
