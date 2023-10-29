from scraper import BaseScraper
from tqdm import tqdm
from pathlib import Path
from bs4 import BeautifulSoup

ICMBIO_URL_INST_NORMATIVAS = "https://www.gov.br/icmbio/pt-br/acesso-a-informacao/legislacao/instrucoes-normativas"
ICMBIO_URL_PORTARIAS = (
    "https://www.gov.br/icmbio/pt-br/acesso-a-informacao/legislacao/portarias"
)
OUTPUT_DIR = r"icmbio"


class ICMBIOScraper(BaseScraper):
    """Scraper for CONAMA (Conselho Nacional do Meio Ambiente) website. The website has a table with all the resolutions and a link to download the pdf file."""

    def __init__(self, url: str = ICMBIO_URL_INST_NORMATIVAS, **kwargs):
        super().__init__(url, **kwargs)

    def download_pdf(self, pdf_link: BeautifulSoup):
        """Download pdf file from link"""
        pdf_name = pdf_link.text
        pdf_name = self.format_filename(pdf_name)
        pdf_name = pdf_name + ".pdf"
        pdf_url = pdf_link["href"]

        self.requester.download_file(pdf_url, pdf_name, output_dir=OUTPUT_DIR)

        return pdf_name

    def download_portaria_pdfs(self, portaria_link: str, filt_dir: str):
        html_str = self.requester.make_request(portaria_link)
        self.set_soup(html_str)

        # some portaria links are in a table, others are in a div

        # check if there is a table or div of links
        table = self.soup.find("table")
        if table:
            links = table.find_all("a")
        else:
            div = self.soup.find("div", id="parent-fieldname-text")
            links = div.find_all("a")

        # iterate through each portaria
        for pdf_link in tqdm(links):
            try:
                # download pdf and upload to Google Drive
                pdf_name = self.download_pdf(pdf_link)

                output_path = Path(OUTPUT_DIR) / Path(pdf_name)
                self.upload_file(
                    output_path,
                    OUTPUT_DIR,
                    parent_folder_name=filt_dir,
                    mimetype="application/pdf",
                )
            except Exception as e:
                print(f"Error downloading file: {e}")

    def download_portarias(self, filt_dir: str = "portarias"):
        html_str = self.requester.make_request(self.url)
        self.set_soup(html_str)

        # get links for portarias
        cards = self.soup.find_all("div", class_="card")
        links = [card.find("a") for card in cards]

        # iterate through each portaria
        for portaria_link in tqdm(links):
            self.download_portaria_pdfs(portaria_link.get("href"), filt_dir)

    def download_instrucoes_normativas(self, filt_dir: str = "instrucoes_normativas"):
        """Run the scraper for Instruções Normativas only"""
        html_str = self.requester.make_request(self.url)
        self.set_soup(html_str)

        # get table links
        table = self.soup.find("table")
        links = table.find_all("a")

        # get links
        for link in tqdm(links):
            try:
                # download pdf and upload to Google Drive
                pdf_name = self.download_pdf(link)
                output_path = Path(OUTPUT_DIR) / Path(pdf_name)

                self.upload_file(
                    output_path,
                    OUTPUT_DIR,
                    parent_folder_name=filt_dir,
                    mimetype="application/pdf",
                )
            except Exception as e:
                print(f"Error downloading file: {e}")

    def run(self):
        """Run the scraper: Download all pdf files from the website"""

        # run for Instruções Normativas first
        self.download_instrucoes_normativas()

        # run for Portarias
        self.url = ICMBIO_URL_PORTARIAS
        self.download_portarias()
