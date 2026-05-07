
import time
import logging


class Timer:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        self.elapsed_seconds = self.end_time - self.start_time
        logging.info(f"\n\nElapsed Time: {self.elapsed_seconds:.4f} seconds")