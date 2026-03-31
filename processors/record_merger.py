import asyncio
import logging
import time
from collections import deque
from typing import Any, Optional

logger = logging.getLogger(__name__)


class RecordMerger:
    '''
    Объединяет записи в батчи
    Заменяет MergeRecord 2.5.0 с алгоритмом Bin-Packing
    '''

    def __init__(self, min_records: int = 1000, max_records: int = 1000, max_bin_age: int = 30, max_bins: int = 10):
        """
        Параметры соответствуют настройкам MergeRecord:
        - min_records: Minimum Number of Records = 1000
        - max_records: Maximum Number of Records = 1000
        - max_bin_age: Max Bin Age = 30 сек
        - max_bins: Maximum Number of Bins = 10
        """
        self.min_records = min_records
        self.max_records = max_records
        self.max_bin_age = max_bin_age
        self.max_bins = max_bins

        self.bins: list[deque] = []
        self.bin_timestamps: list[float] = []
        self._lock = asyncio.Lock()

        # Статистика
        self.total_records_added = 0
        self.total_batches_created = 0
        self.batches_by_size = {}
        self.batches_by_reason = {'size': 0, 'timeout': 0}

        self._init_bins()
        logger.info(
            f"RecordMerger initialized: min_records={min_records}, "
            f"max_records={max_records}, max_bin_age={max_bin_age}s, max_bins={max_bins}"
        )

    def _init_bins(self):
        '''Инициализирует бины'''
        self.bins = [deque() for _ in range(self.max_bins)]
        self.bin_timestamps = [time.time() for _ in range(self.max_bins)]
        logger.debug(f"Initialized {self.max_bins} bins")

    def _get_best_bin(self) -> int:
        '''Находит бин с наименьшим количеством записей (Bin-Packing Algorithm)'''
        best_bin = 0
        min_size = len(self.bins[0])

        for i, bin_queue in enumerate(self.bins):
            if len(bin_queue) < min_size:
                min_size = len(bin_queue)
                best_bin = i

        logger.debug(f"Selected bin {best_bin} with {min_size} records")
        return best_bin

    async def add_record(self, record: dict[str, Any]) -> Optional[list[dict[str, Any]]]:
        '''
        Добавляет запись в бин
        Возвращает батч, если бин заполнен до max_records
        '''
        async with self._lock:
            self.total_records_added += 1
            bin_idx = self._get_best_bin()
            self.bins[bin_idx].append(record)
            current_size = len(self.bins[bin_idx])

            logger.debug(f"Added record to bin {bin_idx}, size={current_size}")

            if current_size >= self.max_records:
                # Бин заполнен
                batch = list(self.bins[bin_idx])
                batch_size = len(batch)
                self.bins[bin_idx].clear()
                self.bin_timestamps[bin_idx] = time.time()

                self.total_batches_created += 1
                self.batches_by_size[batch_size] = self.batches_by_size.get(batch_size, 0) + 1
                self.batches_by_reason['size'] += 1

                logger.info(f"Created batch of {batch_size} records (max size reached) from bin {bin_idx}")
                return batch

            return None

    async def get_ready_batches(self) -> list[list[dict[str, Any]]]:
        '''
        Возвращает батчи, готовые к отправке по условиям:
        - достигнут min_records
        - или истек max_bin_age
        '''
        async with self._lock:
            batches = []
            current_time = time.time()

            for i, bin_queue in enumerate(self.bins):
                bin_size = len(bin_queue)
                if bin_size == 0:
                    continue

                bin_age = current_time - self.bin_timestamps[i]
                reason = None

                if bin_size >= self.min_records:
                    # Достигнут минимум записей
                    reason = f"min_records reached ({bin_size} >= {self.min_records})"
                    batches.append(list(bin_queue))
                    bin_queue.clear()
                    self.bin_timestamps[i] = current_time
                    self.batches_by_reason['size'] += 1

                elif bin_age >= self.max_bin_age:
                    # Истекло время ожидания
                    reason = f"timeout reached ({bin_age:.1f}s >= {self.max_bin_age}s)"
                    batches.append(list(bin_queue))
                    bin_queue.clear()
                    self.bin_timestamps[i] = current_time
                    self.batches_by_reason['timeout'] += 1

                if reason:
                    self.total_batches_created += 1
                    self.batches_by_size[bin_size] = self.batches_by_size.get(bin_size, 0) + 1
                    logger.info(f"Created batch of {bin_size} records from bin {i}: {reason}")

            return batches

    def get_stats(self) -> dict[str, Any]:
        '''Возвращает статистику мерджера'''
        current_sizes = [len(b) for b in self.bins]
        return {
            'total_records_added': self.total_records_added,
            'total_batches_created': self.total_batches_created,
            'batches_by_size': self.batches_by_size,
            'batches_by_reason': self.batches_by_reason,
            'current_bin_sizes': current_sizes,
            'current_total_records': sum(current_sizes),
        }
