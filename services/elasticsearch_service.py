import logging
from typing import Any

from elasticsearch import Elasticsearch, helpers

from config.settings import settings

logger = logging.getLogger(__name__)


class ElasticsearchService:
    '''
    Сервис для работы с Elasticsearch
    '''

    def __init__(self):
        self.client = Elasticsearch(
            hosts=settings.elasticsearch.hosts, retry_on_timeout=True, max_retries=3, request_timeout=30
        )
        self.index = settings.elasticsearch.index
        self.retry_on_conflict = settings.elasticsearch.retry_on_conflict

        if not self.client.ping():
            logger.warning('Cannot connect to Elasticsearch')

    async def bulk_index(self, records: list[dict[str, Any]], id_field: str = 'uid') -> dict[str, Any]:
        '''
        Индексирует батч записей в Elasticsearch
        Заменяет PutElasticsearchRecord с операцией upsert
        '''
        if not records:
            return {'success': True, 'count': 0}

        actions = []
        for record in records:
            doc_id = record.get(id_field)
            if not doc_id:
                logger.warning(f'Record without {id_field}: {record}')
                continue

            # Формируем действие для bulk update с upsert
            action = {
                '_op_type': 'update',  # Index Operation = upsert
                '_index': self.index,  # Index
                '_id': str(doc_id),  # ID Record Path = /uid
                'doc': record,  # Данные для обновления
                'doc_as_upsert': True,  # Upsert = true
                'retry_on_conflict': self.retry_on_conflict,  # retry_on_conflict = 10
            }
            actions.append(action)

        try:
            success, failed = helpers.bulk(
                self.client, actions, raise_on_error=False, raise_on_exception=False, stats_only=False
            )

            if failed:
                logger.error(f'Failed to index {len(failed)} records')
                for fail in failed[:10]:
                    logger.error(f'Failed record: {fail}')

            return {'success': len(failed) == 0, 'count': success, 'failed': len(failed)}

        except Exception as e:
            logger.error(f'Error during bulk indexing: {e}')
            return {'success': False, 'count': 0, 'error': str(e)}

    async def health_check(self) -> bool:
        '''Проверяет здоровье соединения'''
        try:
            return self.client.ping()
        except Exception:
            return False
