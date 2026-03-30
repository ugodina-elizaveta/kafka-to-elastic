import logging
from typing import Any, Optional

from config.schemas import OKUsers

from .date_processor import RecordEnricher

logger = logging.getLogger(__name__)


class RecordProcessor:
    '''Основной процессор записей (объединяет все трансформации)'''

    def __init__(self):
        self.enricher = RecordEnricher()

    def process_record(self, record: dict[str, Any]) -> Optional[dict[str, Any]]:
        '''
        Полная обработка одной записи
        '''
        try:
            # Валидируем запись по схеме
            user = OKUsers(**record)
            record_dict = user.model_dump()

            # Добавляем timestamp и форматируем даты
            enriched = self.enricher.enrich_record(record_dict)

            # Разбиваем birthday
            processed = self.enricher.process_with_birthday_split(enriched)

            return processed

        except Exception as e:
            logger.error(f'Error processing record: {e}, record: {record}')
            return None

    def process_batch(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        '''Обрабатывает батч записей'''
        processed_records = []
        for record in records:
            processed = self.process_record(record)
            if processed:
                processed_records.append(processed)
        return processed_records
