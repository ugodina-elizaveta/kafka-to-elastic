import logging
import re
from datetime import datetime
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


class DateProcessor:
    '''Обработчик дат'''

    @staticmethod
    def parse_birthday(birthday: Optional[str]) -> dict[str, Optional[int]]:
        '''
        Разбивает дату рождения на год, месяц и день
        '''
        if not birthday:
            logger.debug("No birthday value to parse")
            return {'byear': None, 'bmonth': None, 'bday': None}

        # Проверяем формат YYYY-MM-DD или YYYY-M-D
        pattern = r'^\d{2,4}-\d{1,2}-\d{1,2}$'
        if not re.match(pattern, birthday):
            logger.warning(f"Birthday '{birthday}' does not match expected format YYYY-MM-DD")
            return {'byear': None, 'bmonth': None, 'bday': None}

        try:
            parts = birthday.split('-')
            if len(parts) == 3:
                year, month, day = parts
                logger.debug(f"Parsed birthday '{birthday}' into year={year}, month={month}, day={day}")
                return {'byear': int(year), 'bmonth': int(month), 'bday': int(day)}
        except (ValueError, AttributeError) as e:
            logger.error(f"Failed to parse birthday '{birthday}': {e}")

        return {'byear': None, 'bmonth': None, 'bday': None}

    @staticmethod
    def format_datetime(date_str: Optional[str]) -> Optional[str]:
        '''
        Преобразует дату из формата 'yyyy-MM-dd HH:mm:ss' в 'yyyy-MM-ddTHH:mm:ss'
        '''
        # Проверяем на None, пустую строку или строку состоящую только из пробелов
        if not date_str or date_str.strip() == '':
            logger.debug('Empty date string, returning None')
            return None

        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            formatted = dt.strftime('%Y-%m-%dT%H:%M:%S')
            logger.debug(f"Formatted date '{date_str}' -> '{formatted}'")
            return formatted
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to format date '{date_str}': {e}, returning None instead")
            # Возвращаем None вместо некорректной даты, чтобы ES не падал
            return None

    @staticmethod
    def truncate_string(value: Optional[str], max_length: int = 256) -> Optional[str]:
        '''
        Обрезает строку до указанной длины
        Заменяет NiFi выражение: substring(/personal/inspired_by, 0, 256)
        '''
        if not value:
            return None

        if len(value) > max_length:
            truncated = value[:max_length]
            logger.debug(f"Truncated string from {len(value)} to {max_length} chars")
            return truncated

        return value


class RecordEnricher:
    '''Обогащение записей (заменяет два процессора UpdateRecord)'''

    def __init__(
        self,
        fields_to_format: Optional[List[str]] = None,
        add_timestamp: bool = True,
        birthday_field: Optional[str] = 'birthday',
        truncate_fields: Optional[dict[str, int]] = None,
        avatar_hash_field: Optional[str] = None,
    ):
        """
        Инициализация обогатителя записей с настраиваемыми параметрами

        Args:
            fields_to_format: Список полей для форматирования дат (по умолчанию: ['last_online', 'registered_date'])
            add_timestamp: Добавлять ли поле ts (текущее время)
            birthday_field: Название поля для разбиения даты рождения (None - не разбивать)
            truncate_fields: Словарь {поле: максимальная_длина} для обрезания строк
            avatar_hash_field: Название поля для копирования хэша аватара (None - не копировать)
        """
        self.date_processor = DateProcessor()

        # Настройка форматирования дат
        self.fields_to_format = fields_to_format or ['last_online', 'registered_date']
        logger.info(f"RecordEnricher initialized: format_fields={self.fields_to_format}")

        # Настройка добавления timestamp
        self.add_timestamp = add_timestamp
        logger.info(f"RecordEnricher: add_timestamp={self.add_timestamp}")

        # Настройка разбиения birthday
        self.birthday_field = birthday_field
        if self.birthday_field:
            logger.info(f"RecordEnricher: will split birthday field '{self.birthday_field}'")
        else:
            logger.info("RecordEnricher: birthday splitting disabled")

        # Настройка обрезания строк
        self.truncate_fields = truncate_fields or {}
        if self.truncate_fields:
            logger.info(f"RecordEnricher: will truncate fields: {self.truncate_fields}")

        # Настройка копирования хэша аватара
        self.avatar_hash_field = avatar_hash_field
        if self.avatar_hash_field:
            logger.info(f"RecordEnricher: will copy hash to field '{self.avatar_hash_field}'")

    def enrich_record(self, record: dict[str, Any]) -> dict[str, Any]:
        '''
        Добавляет timestamp и форматирует даты
        '''
        enriched = record.copy()

        # 1. Форматирование полей с датами (цикл по списку)
        for field_name in self.fields_to_format:
            if field_name in enriched:
                original_value = enriched[field_name]
                # Пустые строки превращаются в None
                if original_value == "":
                    logger.debug(f"Field '{field_name}' is empty string, setting to None")
                    enriched[field_name] = None
                elif original_value:
                    enriched[field_name] = self.date_processor.format_datetime(original_value)
                    logger.debug(f"Formatted field '{field_name}': '{original_value}' -> '{enriched[field_name]}'")
                else:
                    logger.debug(f"Field '{field_name}' is None, keeping as is")

        # 2. Добавление timestamp (текущее время)
        if self.add_timestamp:
            enriched['ts'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
            logger.debug(f"Added timestamp: {enriched['ts']}")

        # 3. Обрезание строк (например, /personal/inspired_by)
        for field_name, max_length in self.truncate_fields.items():
            if field_name in enriched and enriched[field_name]:
                original_value = enriched[field_name]
                enriched[field_name] = self.date_processor.truncate_string(original_value, max_length)
                logger.debug(
                    f"Truncated field '{field_name}': {len(original_value)}\
                          -> {len(enriched[field_name]) if enriched[field_name] else 0} chars"
                )

        # 4. Копирование хэша аватара (например, /avatar_phash -> /hash)
        if self.avatar_hash_field and self.avatar_hash_field in enriched:
            # Предполагаем, что исходное поле называется так же, или можно настроить
            source_field = self.avatar_hash_field
            target_field = 'hash'  # или можно сделать параметром
            if source_field in enriched:
                enriched[target_field] = enriched[source_field]
                logger.debug(f"Copied hash from '{source_field}' to '{target_field}': {enriched[target_field]}")

        return enriched

    def process_with_birthday_split(self, record: dict[str, Any]) -> dict[str, Any]:
        '''
        Разбивает birthday на компоненты (если настроен birthday_field)
        '''
        processed = record.copy()

        # Если разбиение не настроено, возвращаем запись без изменений
        if not self.birthday_field:
            logger.debug("Birthday splitting disabled, skipping")
            return processed

        # Получаем значение поля birthday
        birthday = processed.get(self.birthday_field)

        if birthday:
            logger.debug(f"Splitting birthday field '{self.birthday_field}': {birthday}")
            birthday_parts = self.date_processor.parse_birthday(birthday)
            processed.update(birthday_parts)
            logger.debug(
                f"Added birthday parts: byear={birthday_parts['byear']}, "
                f"bmonth={birthday_parts['bmonth']}, bday={birthday_parts['bday']}"
            )
        else:
            logger.debug(f"Birthday field '{self.birthday_field}' is empty or missing, setting parts to None")
            processed.update({'byear': None, 'bmonth': None, 'bday': None})

        return processed
