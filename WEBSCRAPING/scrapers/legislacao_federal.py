import time
import base64
import re
import concurrent.futures
from scraper import BaseScraper
from tqdm import tqdm
from pathlib import Path
from unidecode import unidecode
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from multiprocessing import cpu_count
from datetime import datetime


LEGISLACAO_FEDERAL_URL = "https://legislacao.presidencia.gov.br/"
SEARCH_URL = "https://legislacao.presidencia.gov.br/pesquisa/ajax/resultado_pesquisa_legislacao.php"
RESULTS_PER_PAGE = 10  # amount of results that the website shows per page
CONCURRENT_REQUESTS = 10  # amount of concurrent requests to be made
OUTPUT_DIR = r"legislacao_federal"

DATES = {
    "janeiro": "01",
    "fevereiro": "02",
    "março": "03",
    "marco": "03",  # some dates have 'marco' instead of 'março
    "abril": "04",
    "maio": "05",
    "junho": "06",
    "julho": "07",
    "agosto": "08",
    "setembro": "09",
    "outubro": "10",
    "novembro": "11",
    "dezembro": "12",
}


class LegislacaoFederalScraper(BaseScraper):
    """Scraper for Legislação Federal Brasileira website. The website has a form to search for laws and each result is represented by a div card, which contains a link to a html website containing the law specs."""

    def __init__(self, url: str = LEGISLACAO_FEDERAL_URL, **kwargs):
        super().__init__(url, use_selenium=True, **kwargs)
        self.main_window_handle = ""

    def download_law(self, card: BeautifulSoup, output_dir: str):
        ul = card.find("ul", class_="list-inline p-0 m-0")
        link = ul.find_all("a")[1]["href"]  # second link is 'texto integral'

        filename = unidecode(card.find("h4", class_="card-title").text.strip()).replace(
            " ", "_"
        )

        output_path = Path(output_dir) / f"{filename}.pdf"

        if "pesquisa.in.gov.br/" in link:  # already pdf, don't need to convert
            jornal = link.split("jornal=")[1].split("&")[0]
            pagina = link.split("pagina=")[1].split("&")[0]

            # date will be in filename: Ex: 'Decreto_Legislativo_no_74_de_28_de_junho_de_2023.pdf'. Need to get d+1 string: 29/06/2023
            date = filename.split("de_")
            day = int(date[1].split("_")[0]) + 1
            month = DATES[date[2].split("_")[0].lower()]
            year = date[3]

            date_str = f"{day}/{month}/{year}"

            real_link = f"https://pesquisa.in.gov.br/imprensa/servlet/INPDFViewer?jornal={jornal}&pagina={pagina}&data={date_str}&captchafield=firstAccess"

            self.requester.download_file(real_link, f"{filename}.pdf", output_dir)

        else:
            html_str = self.requester.make_request(link)
            converted_ok = self.html_to_pdf(html_str=html_str, output_path=output_path)
            # converted_ok = self.html_to_pdf(url=link, output_path=output_path)
            if not converted_ok:
                print(f"Failed to convert {link}")
                return

        # upload to google drive. thread safe
        self.upload_file_thread_safe(
            output_path, OUTPUT_DIR, mimetype="application/pdf"
        )

    def download_laws_parallel(
        self,
        cards: list[BeautifulSoup],
        output_dir: str,
        cards_executor: concurrent.futures.ThreadPoolExecutor,
    ):
        futures = []
        for card in cards:
            futures.append(cards_executor.submit(self.download_law, card, output_dir))

        # wait for all futures to finish
        for future in futures:
            future.result()

    def search_page(self, data: dict) -> list[BeautifulSoup]:
        # send request
        response_html_text = self.requester.make_request(
            SEARCH_URL, data=data, method="POST"
        )

        # set soup
        self.set_soup(response_html_text)

        # return all laws
        return self.soup.find_all("div", class_="card p-2 pr-3 pl-3 w-100")

    def parallel_run(self):
        """Run the scraper: Use concurrentt.futures to download all html files from the website concurrently, filter laws by active only"""
        # First request to URL using selenium to set cookies to requester
        self.driver.get(self.url)
        time.sleep(30)
        self.html = self.driver.get_driver().page_source
        self.set_soup()

        # close driver
        self.driver.close()

        # set cookies to requester
        cookies = self.driver.get_driver().get_cookies()
        for cookie_obj in cookies:
            cookie = self.requester.cookie_dict_to_cookie(cookie_obj)
            self.requester.set_cookie(cookie)

        # make first search request to get total number of pages
        content_filter: str = "NÃO CONSTA REVOGAÇÃO EXPRESSA|1;NÃO CONSTA REVOGAÇÃO EXPRESSA (VER CAMPO ALTERAÇÃO)|13;NÃO CONSTA REVOGAÇÃO EXPRESSA (VER CAMPO CORRELAÇÃO)|14;NÃO CONSTA REVOGAÇÃO EXPRESSA (VER CAMPO OBSERVAÇÃO)|15"
        situacao_ato = ",".join([x.split("|")[1] for x in content_filter.split(";")])

        page = 0
        offset = 0

        # set data to be sent in request
        data = {
            "pagina": page,
            "posicao": offset,
            "termo": "",
            "num_ato": "",
            "ano_ato": "",
            "dat_inicio": "",
            "dat_termino": "",
            "tipo_macro_ato": "",
            "tipo_ato": "",
            "situacao_ato": situacao_ato,
            "presidente_exercicio": "",
            "chefe_governo": "",
            "dsc_referenda_ministerial": "",
            "referenda_ministerial": "",
            "origem": "",
            "diario_extra": "",
            "data_resenha": "",
            "num_mes_resenha": "",
            "num_ano_resenha": "",
            "ordenacao": "maior_data",
            "conteudo_tipo_macro_ato": "",
            "conteudo_tipo_ato": "",
            "conteudo_situacao_ato": content_filter,
            "conteudo_presidente_exercicio": "",
            "conteudo_chefe_governo": "",
            "conteudo_referenda_ministerial": "",
            "conteudo_origem": "",
            "conteudo_diario_extra": "",
        }

        # send request
        response_html_text = self.requester.make_request(
            SEARCH_URL, data=data, method="POST"
        )

        # set soup
        self.set_soup(response_html_text)

        # calculate total number of pages
        while True:
            try:
                total_results = self.soup.find("h4", class_="pb-2 fw-bold").text
                break
            except AttributeError:
                print("Failed to find total_results. Trying again in 30 seconds")
                self.driver.set_driver()
                self.driver.get(self.url)
                time.sleep(30)
                self.html = self.driver.get_driver().page_source
                self.set_soup()

                # close driver
                self.driver.close()

                # set cookies to requester
                cookies = self.driver.get_driver().get_cookies()
                for cookie_obj in cookies:
                    cookie = self.requester.cookie_dict_to_cookie(cookie_obj)
                    self.requester.set_cookie(cookie)

                # send request
                response_html_text = self.requester.make_request(
                    SEARCH_URL, data=data, method="POST"
                )

                # set soup
                self.set_soup(response_html_text)

        total_results = int(total_results.split(" ")[0].replace(".", ""))
        total_pages = total_results // RESULTS_PER_PAGE
        if total_results % RESULTS_PER_PAGE != 0:
            total_pages += 1

        total_finished = 0

        # iterate over pages by CONCURRENT_REQUESTS amount of pages
        with concurrent.futures.ThreadPoolExecutor() as pages_executor:
            cards_executor = concurrent.futures.ThreadPoolExecutor()

            while total_finished < total_pages:
                responses = [
                    pages_executor.submit(self.search_page, data)
                    for page in range(
                        total_finished, total_finished + CONCURRENT_REQUESTS
                    )
                ]

                total_finished += CONCURRENT_REQUESTS

                # iterate over responses
                for response in tqdm(
                    concurrent.futures.as_completed(responses), total=len(responses)
                ):
                    cards = response.result()
                    # self.download_laws(cards, OUTPUT_DIR)
                    self.download_laws_parallel(cards, OUTPUT_DIR, cards_executor)

                # print total finished so far
                print(f"Total finished: {total_finished}")

        cards_executor.shutdown(wait=True)

    def set_filter(self, filter: str):
        filter_button = self.driver.get_driver().find_element(By.ID, "btn-tipo-ato")
        # if button is not expanded, click it
        if filter_button.get_attribute("aria-expanded") == "false":
            filter_button.click()

        time.sleep(2)

        # set filter
        dropdown = self.driver.get_driver().find_element(By.CLASS_NAME, "dropdown-menu")
        filters_checkboxes = dropdown.find_elements(By.CLASS_NAME, "form-check-sign")
        for checkbox in filters_checkboxes:
            if filter.lower() in checkbox.text.lower():
                checkbox.click()
                break

        time.sleep(2)

    def format_date(self, filename: str) -> str:
        """Format date from 'de_dd_de_MONTH_de_yyyy' to d+1 string. Ex: 'de_28_de_junho_de_2023' to  29/06/2023'"""
        regex = r"de_(\d+)_de_(\w+)_de_(\d+)"
        match = re.search(regex, filename)

        day = int(match.group(1)) + 1
        month = DATES[match.group(2).lower()]
        year = match.group(3)

        # check day constraints
        if day < 10:
            day = f"0{day}"
        elif day > 30:
            day = "29"
        elif month == "02" and day > 28:
            day = "27"

        return f"{day}/{month}/{year}"

    def download_pdf_selenium(self, link: str, output_path: Path):
        pdf = self.driver.get_driver().execute_cdp_cmd(
            "Page.printToPDF", {"printBackground": True}
        )

        converted_ok = pdf.get("data")
        if not converted_ok:
            print(f"Failed to convert {link}")
            return None

        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "wb") as f:
            f.write(base64.b64decode(pdf["data"]))

    def check_whitelabel_error(self):
        body = self.driver.get_driver().find_element(By.TAG_NAME, "body")
        if "Whitelabel Error Page" in body.text or "Sistema Inexistente" in body.text:
            return True

        return False

    def check_law_not_found(self):
        # check if 'não foram encontra' is in page. If so, close tab and return because there's no doc associated with this law
        return self.driver.check_if_element_exists(  # type: ignore
            By.XPATH, "//div[contains(text(), 'Não foram encontr')]"
        )

    def close_secondary_tabs(self):  # close tabs and change to main window handle tab
        for window in self.driver.get_driver().window_handles:
            if window != self.main_window_handle:
                self.driver.get_driver().switch_to.window(window)
                self.driver.get_driver().close()

    def download_law_selenium(
        self, card: BeautifulSoup, output_dir: str, filter_dir: str
    ):
        try:
            ul = card.find("ul", class_="list-inline p-0 m-0")
            a_element = ul.find_all("a")[1]  # second link is 'texto integral'
            link = a_element["href"]

            filename = self.format_filename(
                card.find("h4", class_="card-title").text.strip()
            )
            output_path = Path(output_dir) / f"{filename}.pdf"

            # check if `output_path` already exists and skip file download
            if output_path.exists():
                # upload to google drive.
                self.upload_file(
                    output_path,
                    output_dir,
                    parent_folder_name=filter_dir,
                    mimetype="application/pdf",
                )

                return card

            if (
                "pesquisa.in.gov.br/" in link
            ):  # already embedded pdf, don't need to convert
                jornal = link.split("jornal=")[1].split("&")[0]
                pagina = link.split("pagina=")[1].split("&")[0]

                # date will be in filename: Ex: 'Decreto_Legislativo_no_74_de_28_de_junho_de_2023.pdf'. Need to get d+1 string: 29/06/2023
                date_str = self.format_date(filename)

                real_link = f"https://pesquisa.in.gov.br/imprensa/servlet/INPDFViewer?jornal={jornal}&pagina={pagina}&data={date_str}&captchafield=firstAccess"

                self.requester.download_file(real_link, f"{filename}.pdf", output_dir)

            # if file already pdf, don't need to convert
            elif ".pdf" in link:
                print(f"File already pdf: {filename}.pdf")
                self.requester.download_file(link, f"{filename}.pdf", output_dir)

            elif (
                "legis.senado.leg.br" in link
            ):  # page is not the target html, need to click 2 inner links
                w_handle = self.change_to_new_tab(link)

                if self.check_whitelabel_error() or self.check_law_not_found():
                    # close tab and  change to first tab
                    self.driver.get_driver().close()
                    self.driver.get_driver().switch_to.window(self.main_window_handle)

                    return card

                table = self.driver.get_driver().find_element(By.CLASS_NAME, "table")
                table.find_element(By.CLASS_NAME, "linknorma").click()

                # wait for page to load
                time.sleep(1)

                # change to inner link and get pdf
                inner_table = self.driver.get_driver().find_element(
                    By.CLASS_NAME, "table"
                )
                inner_link = inner_table.find_elements(By.TAG_NAME, "a")

                if len(inner_link) > 0:
                    inner_link = inner_link[0].get_attribute("href")
                    self.change_to_new_tab(inner_link)

                    # wait for page to load
                    time.sleep(1)

                    self.download_pdf_selenium(inner_link, output_path)

                # close tabs and change to main window handle tab
                self.close_secondary_tabs()

                self.driver.get_driver().switch_to.window(self.main_window_handle)
                self.set_soup(self.driver.get_driver().page_source)

            else:
                self.change_to_new_tab(link)

                if self.check_whitelabel_error() or self.check_law_not_found():
                    # close tab and  change to first tab
                    self.driver.get_driver().close()
                    self.driver.get_driver().switch_to.window(self.main_window_handle)

                    return None

                self.download_pdf_selenium(link, output_path)

                # close tab and  change to first tab
                self.close_secondary_tabs()

                self.driver.get_driver().switch_to.window(self.main_window_handle)
                self.set_soup(self.driver.get_driver().page_source)

            # upload to google drive.
            self.upload_file(
                output_path,
                output_dir,
                parent_folder_name=filter_dir,
                mimetype="application/pdf",
            )

            return card
        except Exception as e:
            # add card to failed links, card may not have link, that's why the exception is caught here
            link = card.find("ul", class_="list-inline p-0 m-0").find_all("a")
            if len(link) > 1:
                link = link[1]["href"]
            else:
                link = ""

            filename = self.format_filename(
                card.find("h4", class_="card-title").text.strip()
            )
            self.add_failed_link(link, filename)

            print(f"Failed to download {filename} | Error: {e}")
            return None

    def download_laws_selenium(
        self, cards: list[BeautifulSoup], output_dir: str, filter_dir: str
    ):
        results = []
        for card in cards:
            results.append(self.download_law_selenium(card, output_dir, filter_dir))

        return results

    def wait_for_page_load(self):
        # while body has no other tags than canvas and style, reload page
        body = None
        while not body or len(body.contents) <= 2:
            try:
                # wait for  p tag to be visible
                self.driver.wait_element_to_be_visible(
                    By.XPATH,
                    "//p[contains(text(), 'ENCONTRE AQUI A CONSTITUIÇÃO BRASILEIRA')]",
                    timeout=15,
                )

                self.set_soup(self.driver.get_driver().page_source)

                body = self.soup.find("body")

                if not body:
                    self.driver.get_driver().refresh()

            except Exception as e:
                print(e)

                self.driver.get_driver().refresh()
                continue

    def resume_from_date(self, checkpoint: dict):
        advanced_search_button = self.driver.get_driver().find_element(
            By.CLASS_NAME, "quick-sidebar-toggler"
        )
        advanced_search_button.click()

        time.sleep(2)

        start_date = checkpoint.get("start_date")
        end_date = checkpoint.get("end_date")

        # set start and end dates on form and search. Click before setting dates t avoid automatic overwrite from website
        start_date_input = self.driver.get_driver().find_element(By.ID, "dat_inicio")
        self.driver.type_text(start_date_input, start_date)

        end_date_input = self.driver.get_driver().find_element(By.ID, "dat_termino")
        self.driver.type_text(end_date_input, end_date)

        time.sleep(2)
        self.driver.get_driver().execute_script("pesquisaLegislacao('0');")

        time.sleep(2)

    def set_ordering(self, ordering: str = "Menor data de publicação"):
        dropdown = self.driver.get_driver().find_element(By.ID, "dropdownOrdenacao")
        dropdown.click()

        time.sleep(2)

        dropdown_menu = self.driver.get_driver().find_element(
            By.CLASS_NAME, "dropdown-menu"
        )

        dropdown_items = dropdown_menu.find_elements(
            By.XPATH, "//a[contains(@onclick, 'selecionaOrdenacao')]"
        )
        for item in dropdown_items:
            if ordering.lower() in item.text.lower():
                item.click()
                break

        time.sleep(2)

    def run_scraper(
        self,
        filters: list = [
            "decretos",
            "leis",
            "medidas provisórias",
            "constituição federal",
        ],
    ):
        """Run the scraper: Download all html files from the website, filter laws by active only"""
        self.driver.get(self.url)
        self.driver.get_driver().maximize_window()
        self.wait_for_page_load()

        self.main_window_handle = self.driver.get_driver().current_window_handle

        # download by filters
        for filt in filters:
            self.set_filter(filt)
            self.set_ordering()
            self.set_soup(self.driver.get_driver().page_source)

            checkpoint = self.load_checkpoint(filt)
            if checkpoint:
                self.resume_from_date(checkpoint)

            self.set_soup(self.driver.get_driver().page_source)

            # calculate total number of pages
            total_results = self.soup.find("h4", class_="pb-2 fw-bold").text
            total_results = int(total_results.split(" ")[0].replace(".", ""))
            total_pages = total_results // RESULTS_PER_PAGE

            if total_results > 1 and total_results % RESULTS_PER_PAGE != 0:
                total_pages += 1

            # iterate over pages
            for page in tqdm(range(total_pages)):
                cards = self.soup.find_all("div", class_="card p-2 pr-3 pl-3 w-100")

                # download laws
                finished_futures = self.download_laws_selenium(
                    cards, OUTPUT_DIR, filter_dir=filt
                )

                time.sleep(1)

                self.set_soup(self.driver.get_driver().page_source)

                next_page_button = self.driver.get_driver().find_element(
                    By.XPATH, "//i[contains(@class, 'ti-angle-right')]"
                )
                next_page_button.click()

                # wait until 'Carregando' vanishes
                retries = 10
                while retries > 0:
                    try:
                        self.driver.get_driver().find_element(
                            By.XPATH, "//div[contains(text(), 'Carregando...')]"
                        )
                        time.sleep(3)

                        retries -= 1

                    except:
                        self.set_soup(self.driver.get_driver().page_source)
                        break

                if retries == 0:
                    print("Failed to load page. Trying again")
                    self.wait_for_page_load()

                    # set filter again
                    self.set_filter(filt)
                    self.set_ordering()
                    self.set_soup(self.driver.get_driver().page_source)

                    # refresh will come back to first page. Need to go to `page` again
                    checkpoint = self.load_checkpoint(filt)
                    if checkpoint:
                        self.resume_from_date(checkpoint)

                    self.set_soup(self.driver.get_driver().page_source)

                # save checkpoint each page. Get last future's card info that have finished without errors
                result_cards = list(filter(lambda x: x is not None, finished_futures))
                if len(result_cards) > 0:
                    last_card = result_cards[-1]
                    filename = self.format_filename(
                        last_card.find("h4", class_="card-title").text.strip()
                    )

                    date_str = self.format_date(filename)

                    checkpoint = {
                        "page": page,
                        "start_date": date_str,
                        "end_date": datetime.now().strftime("%d/%m/%Y"),
                        "filter": filt,
                    }

                    self.save_checkpoint(checkpoint, filt)

        self.driver.close()

    def run(
        self,
        filters: list = [
            "decretos",
            "leis",
            "medidas provisórias",
            "constituição federal",
        ],
    ):
        while True:
            # since legislacao federal is an unstable website, we need to retry some times, thats why we use this while loop
            try:
                self.run_scraper(filters)
                break
            except Exception as e:
                print("Error in legislacao federal scraper:", e)

                # close windows and leave only one so the scraper can restart
                for window in self.driver.get_driver().window_handles[1:]:
                    self.driver.get_driver().switch_to.window(window)
                    self.driver.get_driver().close()

                self.driver.get_driver().switch_to.window(
                    self.driver.get_driver().window_handles[0]
                )

                continue
