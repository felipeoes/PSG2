import pdfkit
import json
import time

from bs4 import BeautifulSoup
from requester import Requester
from utils.selenium_helper import SeleniumHelper
from pathlib import Path

from g_drive_service import *
from utils.files_remover import FilesRemover
from utils.files_uploader import FilesUploader
from utils.checkpoint_saver import CheckpointSaver
from utils import format_filename

from threading import Lock

PARENT_FOLDER_ID = "1vRRmyecRE71qKmHlmSbW29G1cPDj3E2t"
FAILED_LINKS_JSON_PATH = Path("failed_links.json")


# decorator to retry function n times
def retry(n: int = 3):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for _ in range(n):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(e)
                    continue

            return None

        return wrapper

    return decorator


lock = Lock()


class BaseScraper:
    """Base class for all scrapers"""

    def __init__(
        self,
        url: str,
        files_remover: FilesRemover,
        files_uploader: FilesUploader,
        use_selenium: bool = False,
        **kwargs,
    ):
        self.url = url
        self.requester = Requester()
        self.driver = None
        self.html = None
        self.soup = None
        self.service = create_service()
        self.set_driver() if use_selenium else None
        self.files_remover = files_remover
        self.files_uploader = files_uploader
        self.checkpoint_saver = CheckpointSaver(
            Path("checkpoints") / Path(f"{self.__class__.__name__}.json")
        )

    def clone_driver(self):
        """Clone driver to use in other thread"""
        driver = SeleniumHelper(self.driver.chrome_options)  # type: ignore

        return driver

    def set_driver(self, options: dict = {}, **kwargs):
        if not self.driver:
            self.driver = SeleniumHelper(options)
        else:
            self.driver.set_options(options, **kwargs)

    def set_html(self):
        """Get html from url"""
        self.html = self.requester.make_request(self.url)

        return self.html

    def set_soup(self, html: str = None):
        """Get soup from html"""
        if not html:
            html = self.html

        self.soup = BeautifulSoup(html, "lxml")

        return self.soup

    @retry()
    def html_to_pdf(
        self, url: str = None, html_str: str = None, output_path: Path = None
    ):
        """Convert html to pdf"""
        if not url and not html_str:
            raise Exception("Either url or html_str must be provided")

        # check if dirs in output_path exists or create them
        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)

        if url:
            return pdfkit.from_url(
                url, output_path, options={"enable-local-file-access": ""}
            )

        return pdfkit.from_string(
            html_str, output_path, options={"enable-local-file-access": ""}
        )

    def upload_file(
        self, file_path: Path, folder_name: str, parent_folder_name: str = "", **kwargs
    ):
        file_info = {
            "file_path": file_path,
            "folder_name": folder_name,
            "parent_folder_name": parent_folder_name,
            **kwargs,
        }
        self.files_uploader.upload_file(**file_info)
        # self.files_uploader.add_file_to_queue(file_info)

    def format_filename(self, filename: str):
        return format_filename(filename)

    def change_to_new_tab(self, link: str):
        # open new tab with link
        self.driver.get_driver().execute_script(f"window.open('{link}');")

        # wait for new tab to open
        time.sleep(1)

        # change to new tab

        for handle in self.driver.get_driver().window_handles:
            self.driver.get_driver().switch_to.window(handle)
            if self.driver.get_driver().current_url == link:
                break

        # wait for page to load
        time.sleep(1)
        
        return self.driver.get_driver().current_window_handle

    def save_checkpoint(self, checkpoint: dict, filt: str):
        """Save checkpoint to file."""
        self.checkpoint_saver.save_checkpoint(checkpoint, filt)

    def load_checkpoint(self, filt: str):
        """Load checkpoint from file."""
        return self.checkpoint_saver.load_checkpoint(filt)

    def add_failed_link(self, url: str, filename: str):
        """Add failed link to failed_links.json file. It will be a list of dicts with url and filename keys"""
        # use lock
        with lock:
            if not FAILED_LINKS_JSON_PATH.exists():
                with open(FAILED_LINKS_JSON_PATH, "w") as f:
                    json.dump([], f, indent=4)

            with open(FAILED_LINKS_JSON_PATH, "r") as f:
                failed_links = json.load(f)

            failed_links.append({"url": url, "filename": filename})

            with open(FAILED_LINKS_JSON_PATH, "w") as f:
                json.dump(failed_links, f, indent=4)

    def run(self):
        """Run the scraper"""
        raise NotImplementedError
