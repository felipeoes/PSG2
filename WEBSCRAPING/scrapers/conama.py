import re
import time
from scraper import BaseScraper
from selenium.webdriver.common.by import By
from tqdm import tqdm
from pathlib import Path
from unidecode import unidecode

CONAMA_URL = "http://conama.mma.gov.br/atos-normativos-sistema"
# OUTPUT_DIR = r"C:\Users\felip\OneDrive\Email attachments\Documentos\SI - 8 SEM\TCC_SEMESTRAL\CODIGOS\WEBSCRAPING\data\conama\\"
OUTPUT_DIR = r"conama"


class ConamaScraper(BaseScraper):
    """Scraper for CONAMA (Conselho Nacional do Meio Ambiente) website. The website has a table with all the resolutions and a link to download the pdf file."""

    def __init__(self, url: str = CONAMA_URL):
        super().__init__(url, use_selenium=True)
        self.set_driver(
            options={
                "download.default_directory": OUTPUT_DIR,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "plugins.always_open_pdf_externally": True,
            }
        )

    def run(self, output_dir: str = OUTPUT_DIR):
        """Run the scraper: Download all pdf files from the website"""
        self.driver.get(self.url)
        time.sleep(1)
        self.html = self.driver.get_driver().page_source
        self.set_soup()

        pagination_ul = self.soup.find("ul", {"class": "pagination"})

        # get last li text before 'next li', which is the total number of pages
        total_pages = int(pagination_ul.find_all("li")[-2].text)

        # iterate over all pages and download the pdf files
        for page in tqdm(
            range(1, total_pages + 1), desc=f"Número de páginas: {total_pages}"
        ):
            try:
                # get the table with the resolutions and download the files
                table = self.soup.find("table", {"id": "tabela-atos-normativos"})
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) > 0:
                        # get the link to the file
                        link = cells[0].find("a")
                        url = link.get("href") if link else None
                        if url and ".download" in url:
                            filename = f"{cells[0].find('b').text}.pdf"
                            self.requester.download_file(url, filename, output_dir)

                            # upload to gdrive and remove from local
                            filepath = Path(output_dir) / unidecode(
                                str(filename).replace(" ", "_").replace("/", "-")
                            )
                            self.upload_file(
                                filepath, output_dir, mimetype="application/pdf"
                            )

                # click on the next page using selenium driver
                next_page_link = "//*[@id='content-section']/div/div/div[2]/div/fieldset/div/div[1]/div[3]/div[2]/ul/li[9]/a"
                self.driver.click(By.XPATH, next_page_link)  # type: ignore

                # wait for the page to load
                self.set_soup(self.driver.get_driver().page_source)
                time.sleep(0.5)

            except Exception as e:
                print(f"Error while processing page {page}: {e}")
                continue
