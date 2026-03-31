import asyncio
import logging
import signal
import sys
import time
from typing import Any

from config.settings import settings
from processors.record_merger import RecordMerger
from processors.record_processor import RecordProcessor
from services.elasticsearch_service import ElasticsearchService
from services.kafka_consumer import KafkaConsumerService

logging.basicConfig(
    level=getattr(logging, settings.log_level), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaToElasticPipeline:
    '''
    Основной пайплайн обработки данных из Kafka в Elasticsearch
    Объединяет все компоненты в единый поток обработки
    '''

    def __init__(self):
        # Инициализация процессора с настраиваемыми параметрами
        # параметры нужно вынести в settings.py !!!
        self.record_processor = RecordProcessor(
            fields_to_format=['last_online', 'registered_date'],  # Список полей для форматирования дат
            add_timestamp=True,  # Добавлять ли ts
            birthday_field='birthday',  # Поле для разбиения (None - отключить)
            truncate_fields={'inspired_by': 256},  # Обрезание строк {поле: макс_длина}
            avatar_hash_field='avatar_phash',  # Поле для копирования хэша аватара
        )
        self.elastic_service = None
        self.record_merger = None
        self.kafka_service = None
        self.running = False
        self.flush_task = None
        self.stop_event = asyncio.Event()

        # Статистика
        self.start_time = None
        self.last_stats_time = None
        self.stats_task = None

    async def _init_elasticsearch_with_retry(self, max_retries: int = 10, delay: int = 5):
        '''Инициализация ES с повторными попытками'''
        for attempt in range(max_retries):
            try:
                logger.info(f'Attempting to connect to Elasticsearch (attempt {attempt + 1}/{max_retries})...')
                self.elastic_service = ElasticsearchService()

                if await self.elastic_service.health_check():
                    logger.info('Successfully connected to Elasticsearch')
                    return True
            except Exception as e:
                logger.warning(f'Failed to connect to Elasticsearch on attempt {attempt + 1}: {e}')

            if attempt < max_retries - 1:
                logger.info(f'Retrying in {delay} seconds...')
                await asyncio.sleep(delay)

        logger.error('Failed to connect to Elasticsearch after all retries')
        return False

    async def handle_message(self, message: dict[str, Any]):
        '''Обрабатывает сообщение из Kafka'''
        if not self.elastic_service or not self.record_merger:
            logger.error('Service not initialized')
            return

        try:
            # Обрабатываем массив или одиночную запись
            if isinstance(message, list):
                logger.info(f'Received batch of {len(message)} records from Kafka')
                for record in message:
                    await self._process_single_record(record)
            else:
                await self._process_single_record(message)

        except Exception as e:
            logger.error(f'Error processing message: {e}', exc_info=True)

    async def _process_single_record(self, record: dict[str, Any]):
        '''Обрабатывает одну запись'''
        try:
            processed = self.record_processor.process_record(record)

            if processed and self.record_merger:
                batch = await self.record_merger.add_record(processed)
                if batch:
                    logger.info(f'Batch ready from merger, size: {len(batch)}')
                    await self._send_to_elasticsearch(batch)
        except Exception as e:
            logger.error(f'Error processing single record: {e}', exc_info=True)

    async def _send_to_elasticsearch(self, batch: list[dict[str, Any]]):
        '''Отправляет батч в Elasticsearch'''
        if not batch or not self.elastic_service:
            return

        try:
            logger.info(f'Sending batch of {len(batch)} records to Elasticsearch')
            start_time = time.time()

            result = await self.elastic_service.bulk_index(batch)

            elapsed = time.time() - start_time
            if result['success']:
                logger.info(
                    f"Successfully indexed {result['count']} records in {elapsed:.2f}s "
                    f"(rate: {result['count']/elapsed:.2f} rec/sec)"
                )
                if result.get('failed', 0) > 0:
                    logger.warning(f"Failed to index {result['failed']} records")
            else:
                logger.error(f"Failed to index batch: {result.get('error', 'Unknown error')}")

        except Exception as e:
            logger.error(f'Error sending to Elasticsearch: {e}', exc_info=True)

    async def _flush_ready_batches(self):
        '''Периодическая отправка готовых батчей'''
        while self.running:
            try:
                await asyncio.sleep(1)  # Проверяем каждую секунду

                if self.record_merger:
                    ready_batches = await self.record_merger.get_ready_batches()
                    if ready_batches:
                        logger.debug(f'Found {len(ready_batches)} ready batches from merger')
                    for batch in ready_batches:
                        await self._send_to_elasticsearch(batch)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f'Error in flush task: {e}', exc_info=True)

    async def _log_stats_periodically(self):
        '''Периодическое логирование статистики'''
        while self.running:
            try:
                await asyncio.sleep(30)  # Логируем каждые 30 секунд

                if self.record_processor and self.record_merger:
                    processor_stats = self.record_processor.get_stats()
                    merger_stats = self.record_merger.get_stats()

                    logger.info("=== PIPELINE STATISTICS ===")
                    logger.info(
                        f"Processor: {processor_stats['processed_count']} processed, "
                        f"{processor_stats['error_count']} errors, "
                        f"rate: {processor_stats['avg_rate']:.2f} rec/sec"
                    )
                    logger.info(
                        f"Merger: {merger_stats['total_records_added']} added, "
                        f"{merger_stats['total_batches_created']} batches created"
                    )
                    logger.info(
                        f"Current buffer: {merger_stats['current_total_records']} records "
                        f"in {len(merger_stats['current_bin_sizes'])} bins"
                    )
                    logger.info(
                        f"Batches by reason: size={merger_stats['batches_by_reason']['size']}, "
                        f"timeout={merger_stats['batches_by_reason']['timeout']}"
                    )
                    logger.info("============================")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f'Error in stats logging: {e}', exc_info=True)

    async def start(self):
        '''Запускает пайплайн'''
        self.start_time = time.time()
        self.last_stats_time = self.start_time

        # Подключаемся к Elasticsearch
        if not await self._init_elasticsearch_with_retry():
            logger.error('Cannot connect to Elasticsearch. Exiting...')
            sys.exit(1)

        # Инициализируем мерджер
        self.record_merger = RecordMerger(
            min_records=settings.elasticsearch.min_records,
            max_records=settings.elasticsearch.max_records,
            max_bin_age=settings.elasticsearch.max_bin_age,
            max_bins=settings.max_bins,
        )

        self.running = True

        # Запускаем фоновые задачи
        self.flush_task = asyncio.create_task(self._flush_ready_batches())
        self.stats_task = asyncio.create_task(self._log_stats_periodically())

        # Запускаем Kafka консьюмер
        self.kafka_service = KafkaConsumerService(self.handle_message)

        logger.info("=" * 50)
        logger.info("Pipeline started successfully")
        logger.info("Configuration:")
        logger.info(f"  - Kafka topic: {settings.topic}")
        logger.info(f"  - Kafka group_id: {settings.kafka.group_id}")
        logger.info(f"  - Elasticsearch hosts: {settings.elasticsearch.hosts}")
        logger.info(f"  - Elasticsearch index: {settings.elasticsearch.index}")
        logger.info(f"  - Batch size: {settings.elasticsearch.max_records} records")
        logger.info(f"  - Max bin age: {settings.elasticsearch.max_bin_age} seconds")
        logger.info("=" * 50)

        try:
            await self.kafka_service.start()
            # Ждем сигнала остановки
            await self.stop_event.wait()
        except asyncio.CancelledError:
            logger.info('Shutting down...')
        except Exception as e:
            logger.error(f'Kafka consumer error: {e}', exc_info=True)
        finally:
            await self.stop()

    async def stop(self):
        '''Останавливает пайплайн'''
        self.stop_event.set()
        logger.info('Stopping pipeline...')
        self.running = False

        # Логируем финальную статистику
        if self.record_processor and self.record_merger:
            processor_stats = self.record_processor.get_stats()
            merger_stats = self.record_merger.get_stats()
            logger.info("=== FINAL STATISTICS ===")
            logger.info(f"Total processed: {processor_stats['processed_count']} records")
            logger.info(f"Total errors: {processor_stats['error_count']}")
            logger.info(f"Total batches: {merger_stats['total_batches_created']}")
            logger.info(f"Uptime: {processor_stats['uptime_seconds']:.2f} seconds")
            logger.info("========================")

        # Останавливаем фоновые задачи
        for task in [self.flush_task, self.stats_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Отправляем оставшиеся записи
        if self.record_merger:
            try:
                remaining_batches = await self.record_merger.get_ready_batches()
                if remaining_batches:
                    logger.info(f"Flushing {len(remaining_batches)} remaining batches")
                    for batch in remaining_batches:
                        await self._send_to_elasticsearch(batch)
            except Exception as e:
                logger.error(f'Error flushing remaining batches: {e}')

        # Останавливаем консьюмер
        if self.kafka_service:
            await self.kafka_service.stop()

        logger.info('Pipeline stopped')


async def main():
    '''Точка входа в приложение'''
    pipeline = KafkaToElasticPipeline()

    # Настройка graceful shutdown
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(pipeline.stop()))

    try:
        await pipeline.start()
    except KeyboardInterrupt:
        logger.info('Application stopped by user')
    except Exception as e:
        logger.error(f'Application error: {e}', exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
