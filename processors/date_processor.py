import re
from datetime import datetime
from typing import Any, Optional


class DateProcessor:
    '''Обработчик дат (заменяет логику NiFi Expression Language)'''

    @staticmethod
    def parse_birthday(birthday: Optional[str]) -> dict[str, Optional[int]]:
        '''
        Разбивает дату рождения на год, месяц и день
        '''
        if not birthday:
            return {'byear': None, 'bmonth': None, 'bday': None}

        # Проверяем формат YYYY-MM-DD или YYYY-M-D
        pattern = r'^\d{2,4}-\d{1,2}-\d{1,2}$'
        if not re.match(pattern, birthday):
            return {'byear': None, 'bmonth': None, 'bday': None}

        try:
            parts = birthday.split('-')
            if len(parts) == 3:
                year, month, day = parts
                return {'byear': int(year), 'bmonth': int(month), 'bday': int(day)}
        except (ValueError, AttributeError):
            pass

        return {'byear': None, 'bmonth': None, 'bday': None}

    @staticmethod
    def format_datetime(date_str: Optional[str]) -> Optional[str]:
        '''
        Преобразует дату из формата 'yyyy-MM-dd HH:mm:ss' в 'yyyy-MM-ddTHH:mm:ss'
        '''
        if not date_str or date_str.strip() == '':
            return None

        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%Y-%m-%dT%H:%M:%S')
        except (ValueError, TypeError):
            return date_str


class RecordEnricher:
    '''Замена UpdateRecord'''

    def __init__(self):
        self.date_processor = DateProcessor()

    def enrich_record(self, record: dict[str, Any]) -> dict[str, Any]:
        '''
        Добавляет timestamp и форматирует даты
        '''
        enriched = record.copy()

        # Форматируем last_online
        if 'last_online' in enriched:
            enriched['last_online'] = self.date_processor.format_datetime(enriched['last_online'])

        # Форматируем registered_date
        if 'registered_date' in enriched:
            enriched['registered_date'] = self.date_processor.format_datetime(enriched['registered_date'])

        # Добавляем ts (текущее время)
        enriched['ts'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

        return enriched

    def process_with_birthday_split(self, record: dict[str, Any]) -> dict[str, Any]:
        '''
        Разбивает birthday на компоненты
        Заменяет второй UpdateRecord (разбиение даты рождения)
        '''
        processed = record.copy()
        birthday = processed.get('birthday')
        birthday_parts = self.date_processor.parse_birthday(birthday)
        processed.update(birthday_parts)
        return processed
