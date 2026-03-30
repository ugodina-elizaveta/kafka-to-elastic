import logging
from typing import Any, Awaitable, Callable, Dict

from faststream import FastStream
from faststream.confluent import KafkaBroker
from faststream.security import SASLPlaintext

from config.settings import settings

logger = logging.getLogger(__name__)


class KafkaConsumerService:
    '''
    Сервис для потребления сообщений из Kafka
    '''

    def __init__(self, message_handler: Callable[[Dict[str, Any]], Awaitable[None]]):
        # Настройка брокера
        self.broker = KafkaBroker(
            bootstrap_servers=[f'{settings.kafka.dsn.host}:{settings.kafka.dsn.port}'],
            security=SASLPlaintext(
                username=settings.kafka.dsn.username,
                password=settings.kafka.dsn.password,
            ),
            acks='all',  # Подтверждение записи
            compression_type='zstd',  # Тип сжатия
            max_request_size=104_858_800,  # Максимальный размер запроса
        )

        self.app = FastStream(self.broker)
        self.message_handler = message_handler
        self._setup_handlers()

    def _setup_handlers(self):
        '''Настраивает обработчики сообщений'''

        @self.broker.subscriber(
            settings.topic,  # Топик для подписки
            group_id=settings.kafka.group_id,  # Group ID
            auto_offset_reset='earliest',  # Auto Offset Reset = earliest
            auto_commit_interval_ms=1000,  # Max Uncommitted Time = 1 sec
        )
        async def handle_message(message: Dict[str, Any]):
            try:
                await self.message_handler(message)
            except Exception as e:
                logger.error(f'Error handling message: {e}')
                raise

    async def start(self):
        '''Запускает консьюмер'''
        logger.info(f'Starting Kafka consumer for topics: {settings.topic}')
        await self.broker.start()

    async def stop(self):
        '''Останавливает консьюмер'''
        logger.info('Stopping Kafka consumer...')
        await self.broker.close()
