import asyncio
import logging
import time
from collections import deque
from typing import Any, Optional

from config.settings import settings

logger = logging.getLogger(__name__)


class RecordMerger:
    '''Объединяет записи в батчи (заменяет MergeRecord)'''

    def __init__(self):
        self.min_records = settings.merger.min_records
        self.max_records = settings.merger.max_records
        self.max_bin_age = settings.merger.max_bin_age
        self.max_bins = settings.merger.max_bins

        self.bins: list[deque] = []
        self.bin_timestamps: list[float] = []
        self._lock = asyncio.Lock()

        self.total_records_added = 0
        self.total_batches_created = 0
        self.batches_by_size = {}
        self.batches_by_reason = {'size': 0, 'timeout': 0}

        self._init_bins()
        logger.info(
            f'RecordMerger initialized: min_records={self.min_records}, '
            f'max_records={self.max_records}, max_bin_age={self.max_bin_age}s, '
            f'max_bins={self.max_bins}'
        )

    def _init_bins(self):
        '''Инициализирует бины'''
        self.bins = [deque() for _ in range(self.max_bins)]
        self.bin_timestamps = [time.time() for _ in range(self.max_bins)]

    def _get_best_bin(self) -> int:
        '''Находит бин с наименьшим количеством записей (Bin-Packing Algorithm)'''
        best_bin = 0
        min_size = len(self.bins[0])

        for i, bin_queue in enumerate(self.bins):
            if len(bin_queue) < min_size:
                min_size = len(bin_queue)
                best_bin = i

        return best_bin

    async def add_record(self, record: dict[str, Any]) -> Optional[list[dict[str, Any]]]:
        '''Добавляет запись в бин, возвращает батч если бин заполнен'''
        async with self._lock:
            self.total_records_added += 1
            bin_idx = self._get_best_bin()
            self.bins[bin_idx].append(record)
            current_size = len(self.bins[bin_idx])

            if current_size >= self.max_records:
                batch = list(self.bins[bin_idx])
                self.bins[bin_idx].clear()
                self.bin_timestamps[bin_idx] = time.time()

                self.total_batches_created += 1
                self.batches_by_size[current_size] = self.batches_by_size.get(current_size, 0) + 1
                self.batches_by_reason['size'] += 1

                logger.info(f'Created batch of {current_size} records (max size reached)')
                return batch

            return None

    async def get_ready_batches(self) -> list[list[dict[str, Any]]]:
        '''Возвращает батчи, готовые к отправке'''
        async with self._lock:
            batches = []
            current_time = time.time()

            for i, bin_queue in enumerate(self.bins):
                bin_size = len(bin_queue)
                if bin_size == 0:
                    continue

                bin_age = current_time - self.bin_timestamps[i]

                if bin_size >= self.min_records:
                    batches.append(list(bin_queue))
                    bin_queue.clear()
                    self.bin_timestamps[i] = current_time
                    self.batches_by_reason['size'] += 1
                    self.total_batches_created += 1
                    self.batches_by_size[bin_size] = self.batches_by_size.get(bin_size, 0) + 1
                    logger.info(f'Created batch of {bin_size} records (min_records reached)')

                elif bin_age >= self.max_bin_age:
                    batches.append(list(bin_queue))
                    bin_queue.clear()
                    self.bin_timestamps[i] = current_time
                    self.batches_by_reason['timeout'] += 1
                    self.total_batches_created += 1
                    self.batches_by_size[bin_size] = self.batches_by_size.get(bin_size, 0) + 1
                    logger.info(f'Created batch of {bin_size} records (timeout reached)')

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
