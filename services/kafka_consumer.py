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
            # auto_commit_interval_ms=1000,      # Max Uncommitted Time = 1 sec (если нужно)
        )
        async def handle_message(message: Dict[str, Any]):
            '''Обработчик сообщений из Kafka'''
            try:
                # Логируем полученное сообщение
                uid = message.get('uid') if isinstance(message, dict) else None
                logger.debug(f'Received message from Kafka: uid={uid}')

                await self.message_handler(message)
                logger.debug(f'Message processed successfully: uid={uid}')

            except Exception as e:
                logger.error(f'Error handling message: {e}', exc_info=True)
                raise

    async def start(self):
        '''Запускает консьюмер'''
        try:
            logger.info(f'Starting Kafka consumer for topics: {settings.topic}')
            logger.info(f'Kafka broker: {settings.kafka.dsn.host}:{settings.kafka.dsn.port}')
            logger.info(f'Group ID: {settings.kafka.group_id}')
            await self.broker.start()
        except Exception as e:
            logger.error(f'Failed to start Kafka consumer: {e}')
            raise

    async def stop(self):
        '''Останавливает консьюмер'''
        try:
            logger.info('Stopping Kafka consumer...')
            await self.broker.close()
        except Exception as e:
            logger.error(f'Error stopping Kafka consumer: {e}')
