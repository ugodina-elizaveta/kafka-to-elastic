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

        self._init_bins()

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
        '''
        Добавляет запись в бин
        Возвращает батч, если бин заполнен до max_records
        '''
        async with self._lock:
            bin_idx = self._get_best_bin()
            self.bins[bin_idx].append(record)

            if len(self.bins[bin_idx]) >= self.max_records:
                batch = list(self.bins[bin_idx])
                self.bins[bin_idx].clear()
                self.bin_timestamps[bin_idx] = time.time()
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
                if len(bin_queue) >= self.min_records:
                    # Достигнут минимум записей
                    batches.append(list(bin_queue))
                    bin_queue.clear()
                    self.bin_timestamps[i] = current_time
                elif len(bin_queue) > 0 and (current_time - self.bin_timestamps[i]) >= self.max_bin_age:
                    # Истекло время ожидания
                    batches.append(list(bin_queue))
                    bin_queue.clear()
                    self.bin_timestamps[i] = current_time

            return batches
