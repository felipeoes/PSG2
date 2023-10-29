from multiprocessing import Queue
from queue import Empty
from threading import Thread
from pathlib import Path
import time


class FilesRemover(Thread):
    """Background class to delete downloaded files, based on queue"""

    def __init__(self):
        Thread.__init__(self, daemon=True)
        self.queue: Queue[Path] = Queue()
        self.stop = False

    def add_file_to_queue(self, filepath: Path):
        self.queue.put(filepath)

    def empty_queue(self):
        while not self.queue.empty():
            filepath = self.queue.get(block=False, timeout=0.1)
            if filepath:
                filepath.unlink()
                break

    def run(self):
        while True:
            try:
                filepath = self.queue.get(block=True, timeout=1)
                if filepath:
                    filepath.unlink()

                if self.stop:
                    # get all files from queue, delete them and exit
                    self.empty_queue()
                    break

            # ignore timeout and empty exception
            except Empty:
                if self.stop:
                    self.empty_queue()
                    break
                pass

            except KeyboardInterrupt:
                if self.stop:
                    # get all files from queue, delete them and exit
                    self.empty_queue()
                break
            except Exception as e:
                print(f"Error while deleting file: {e}")
            time.sleep(1)
