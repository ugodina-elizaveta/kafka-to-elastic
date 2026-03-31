import logging
import time
from typing import Any, Dict, List, Optional

from config.schemas import OKUsers

from .date_processor import RecordEnricher

logger = logging.getLogger(__name__)


class RecordProcessor:
    '''Основной процессор записей (объединяет все трансформации)'''

    def __init__(
        self,
        fields_to_format: Optional[List[str]] = None,
        add_timestamp: bool = True,
        birthday_field: Optional[str] = 'birthday',
        truncate_fields: Optional[dict[str, int]] = None,
        avatar_hash_field: Optional[str] = None,
    ):
        """
        Инициализация процессора с настраиваемыми параметрами

        Args:
            fields_to_format: Список полей для форматирования дат
            add_timestamp: Добавлять ли поле ts
            birthday_field: Название поля для разбиения даты рождения
            truncate_fields: Словарь {поле: максимальная_длина} для обрезания строк
            avatar_hash_field: Название поля для копирования хэша аватара
        """
        self.enricher = RecordEnricher(
            fields_to_format=fields_to_format,
            add_timestamp=add_timestamp,
            birthday_field=birthday_field,
            truncate_fields=truncate_fields,
            avatar_hash_field=avatar_hash_field,
        )
        self.processed_count = 0
        self.error_count = 0
        self.start_time = time.time()
        logger.info("RecordProcessor initialized")

    def process_record(self, record: dict[str, Any]) -> Optional[dict[str, Any]]:
        '''
        Полная обработка одной записи
        Включает:
        1. Валидацию по схеме
        2. Добавление timestamp
        3. Форматирование дат
        4. Разбиение др
        5. Обрезание строк
        6. Копирование хэша аватара
        '''
        try:
            # Валидируем запись по схеме
            user = OKUsers(**record)
            record_dict = user.model_dump()
            logger.debug(f"Record validated successfully: uid={record_dict.get('uid')}")

            # Обогащаем запись (форматирование дат, добавление timestamp, обрезание, хэш)
            enriched = self.enricher.enrich_record(record_dict)
            logger.debug(f"Record enriched: uid={enriched.get('uid')}, ts={enriched.get('ts')}")

            # Разбиваем birthday (если настроено)
            processed = self.enricher.process_with_birthday_split(enriched)

            self.processed_count += 1
            if self.processed_count % 100 == 0:  # Логируем каждые 100 записей
                elapsed = time.time() - self.start_time
                logger.info(
                    f"Processed {self.processed_count} records, "
                    f"errors: {self.error_count}, "
                    f"rate: {self.processed_count/elapsed:.2f} rec/sec"
                )

            return processed

        except Exception as e:
            self.error_count += 1
            logger.error(f'Error processing record: {e}, record: {record}', exc_info=True)
            return None

    def process_batch(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        '''
        Обрабатывает батч записей
        '''
        batch_start = time.time()
        logger.info(f"Processing batch of {len(records)} records")

        processed_records = []
        for idx, record in enumerate(records):
            processed = self.process_record(record)
            if processed:
                processed_records.append(processed)

        batch_time = time.time() - batch_start
        logger.info(
            f"Batch processed: {len(processed_records)}/{len(records)} records, "
            f"time: {batch_time:.2f}s, "
            f"rate: {len(records)/batch_time:.2f} rec/sec"
        )

        return processed_records

    def get_stats(self) -> dict[str, Any]:
        '''Возвращает статистику обработки'''
        elapsed = time.time() - self.start_time
        return {
            'processed_count': self.processed_count,
            'error_count': self.error_count,
            'uptime_seconds': elapsed,
            'avg_rate': self.processed_count / elapsed if elapsed > 0 else 0,
        }
