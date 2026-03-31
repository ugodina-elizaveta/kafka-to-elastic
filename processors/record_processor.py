import logging
import time
from importlib import import_module
from typing import Any, Optional

from config.settings import settings

from .date_processor import RecordEnricher

logger = logging.getLogger(__name__)


class RecordProcessor:
    '''Основной процессор записей (объединяет все трансформации)'''

    def __init__(self):
        # Загружаем схему динамически
        self.schema_class = self._load_schema(settings.record_processor.schema_class)
        self.enricher = RecordEnricher(settings.record_processor)
        self.processed_count = 0
        self.error_count = 0
        self.start_time = time.time()
        logger.info('RecordProcessor initialized')

    def _load_schema(self, schema_path: str):
        '''Динамически загружает класс схемы'''
        try:
            module_path, class_name = schema_path.rsplit('.', 1)
            module = import_module(module_path)
            schema_class = getattr(module, class_name)
            logger.info(f'Loaded schema: {schema_path}')
            return schema_class
        except Exception as e:
            logger.error(f'Failed to load schema {schema_path}: {e}')
            raise

    def process_record(self, record: dict[str, Any]) -> Optional[dict[str, Any]]:
        '''Полная обработка одной записи'''
        try:
            # Валидируем запись по схеме
            user = self.schema_class(**record)
            record_dict = user.model_dump()
            logger.debug(f"Record validated successfully: uid={record_dict.get('uid')}")

            # Обогащаем запись
            enriched = self.enricher.enrich_record(record_dict)

            # Разбиваем birthday
            processed = self.enricher.process_with_birthday_split(enriched)

            self.processed_count += 1
            if self.processed_count % 100 == 0:
                elapsed = time.time() - self.start_time
                logger.info(
                    f'Processed {self.processed_count} records, '
                    f'errors: {self.error_count}, '
                    f'rate: {self.processed_count/elapsed:.2f} rec/sec'
                )

            return processed

        except Exception as e:
            self.error_count += 1
            logger.error(f'Error processing record: {e}', exc_info=True)
            return None

    def process_batch(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        '''Обрабатывает батч записей'''
        batch_start = time.time()
        logger.info(f'Processing batch of {len(records)} records')

        processed_records = []
        for record in records:
            processed = self.process_record(record)
            if processed:
                processed_records.append(processed)

        batch_time = time.time() - batch_start
        logger.info(f'Batch processed: {len(processed_records)}/{len(records)} records, ' f'time: {batch_time:.2f}s')

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
