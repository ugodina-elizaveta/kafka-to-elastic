import logging
import re
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)


class DateProcessor:
    '''Обработчик дат'''

    @staticmethod
    def parse_birthday(birthday: Optional[str]) -> dict[str, Optional[int]]:
        '''
        Разбивает дату рождения на год, месяц и день
        '''
        if not birthday:
            logger.debug('No birthday value to parse')
            return {'byear': None, 'bmonth': None, 'bday': None}

        # Проверяем формат YYYY-MM-DD или YYYY-M-D
        pattern = r'^\d{2,4}-\d{1,2}-\d{1,2}$'
        if not re.match(pattern, birthday):
            logger.warning(f'Birthday \'{birthday}\' does not match expected format YYYY-MM-DD')
            return {'byear': None, 'bmonth': None, 'bday': None}

        try:
            parts = birthday.split('-')
            if len(parts) == 3:
                year, month, day = parts
                logger.debug(f'Parsed birthday \'{birthday}\' into year={year}, month={month}, day={day}')
                return {'byear': int(year), 'bmonth': int(month), 'bday': int(day)}
        except (ValueError, AttributeError) as e:
            logger.error(f'Failed to parse birthday \'{birthday}\': {e}')

        return {'byear': None, 'bmonth': None, 'bday': None}

    @staticmethod
    def format_datetime(date_str: Optional[str]) -> Optional[str]:
        '''
        Преобразует дату из формата 'yyyy-MM-dd HH:mm:ss' в 'yyyy-MM-ddTHH:mm:ss'
        Пустые строки преобразуются в None
        '''
        if not date_str or date_str.strip() == '':
            logger.debug('Empty date string, returning None')
            return None

        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            formatted = dt.strftime('%Y-%m-%dT%H:%M:%S')
            logger.debug(f'Formatted date \'{date_str}\' -> \'{formatted}\'')
            return formatted
        except (ValueError, TypeError) as e:
            logger.warning(f'Failed to format date \'{date_str}\': {e}, returning None')
            return None

    @staticmethod
    def truncate_string(value: Optional[str], max_length: int = 256) -> Optional[str]:
        '''Обрезает строку до указанной длины'''
        if not value:
            return None

        if len(value) > max_length:
            truncated = value[:max_length]
            logger.debug(f'Truncated string from {len(value)} to {max_length} chars')
            return truncated

        return value


class RecordEnricher:
    '''Обогащение записей'''

    def __init__(self, config):
        """
        Args:
            config: RecordProcessorSettings объект с настройками
        """
        self.config = config
        self.date_processor = DateProcessor()

        logger.info(f'RecordEnricher initialized: format_fields={self.config.fields_to_format}')
        logger.info(f'RecordEnricher: add_timestamp={self.config.add_timestamp}')

        if self.config.birthday_field:
            logger.info(f'RecordEnricher: will split birthday field \'{self.config.birthday_field}\'')
        else:
            logger.info('RecordEnricher: birthday splitting disabled')

        if self.config.truncate_fields:
            logger.info(f'RecordEnricher: will truncate fields: {self.config.truncate_fields}')

        if self.config.avatar_hash_field:
            logger.info(
                f'RecordEnricher: will copy hash from \'{self.config.avatar_hash_field}\' '
                f'to \'{self.config.avatar_hash_target}\''
            )

    def enrich_record(self, record: dict[str, Any]) -> dict[str, Any]:
        '''Добавляет timestamp и форматирует даты'''
        enriched = record.copy()

        # 1. Форматирование полей с датами
        for field_name in self.config.fields_to_format:
            if field_name in enriched:
                original_value = enriched[field_name]
                if original_value == '':
                    logger.debug(f'Field \'{field_name}\' is empty string, setting to None')
                    enriched[field_name] = None
                elif original_value:
                    enriched[field_name] = self.date_processor.format_datetime(original_value)
                    logger.debug(
                        f'Formatted field \'{field_name}\': \'{original_value}\' -> \'{enriched[field_name]}\''
                    )

        # 2. Добавление timestamp
        if self.config.add_timestamp:
            enriched['ts'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
            logger.debug(f"Added timestamp: {enriched['ts']}")

        # 3. Обрезание строк
        for field_name, max_length in self.config.truncate_fields.items():
            if field_name in enriched and enriched[field_name]:
                original_value = enriched[field_name]
                enriched[field_name] = self.date_processor.truncate_string(original_value, max_length)

        # 4. Копирование хэша аватара
        if self.config.avatar_hash_field and self.config.avatar_hash_field in enriched:
            source_field = self.config.avatar_hash_field
            target_field = self.config.avatar_hash_target
            if source_field in enriched:
                enriched[target_field] = enriched[source_field]
                logger.debug(f'Copied hash from \'{source_field}\' to \'{target_field}\'')

        return enriched

    def process_with_birthday_split(self, record: dict[str, Any]) -> dict[str, Any]:
        '''Разбивает birthday на компоненты'''
        processed = record.copy()

        if not self.config.birthday_field:
            logger.debug('Birthday splitting disabled, skipping')
            return processed

        birthday = processed.get(self.config.birthday_field)

        if birthday:
            logger.debug(f'Splitting birthday field \'{self.config.birthday_field}\': {birthday}')
            birthday_parts = self.date_processor.parse_birthday(birthday)
            processed.update(birthday_parts)
            logger.debug(
                f"Added birthday parts: byear={birthday_parts['byear']}, "
                f"bmonth={birthday_parts['bmonth']}, bday={birthday_parts['bday']}"
            )
        else:
            logger.debug(f'Birthday field \'{self.config.birthday_field}\' is empty, setting parts to None')
            processed.update({'byear': None, 'bmonth': None, 'bday': None})

        return processed
