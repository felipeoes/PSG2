from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

# from webdriver_manager.chrome import ChromeDriverManager
# from undetected_chromedriver import Chrome

from threading import Thread
import time
import json

USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'

class CloseDriverThread(Thread):
    def __init__(self, driver):
        Thread.__init__(self, daemon=True)
        self.driver = driver

    def run(self):
        time.sleep(3)
        self.driver.close()


class SeleniumHelper:
    def __init__(
        self,
        options: dict = {
            "download.default_directory": r"data\\",
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "printing.print_preview_sticky_settings.appState": json.dumps(
                {
                    "recentDestinations": [
                        {
                            "id": "Save as PDF",
                            "origin": "local",
                            "account": "",
                        }
                    ],
                    "selectedDestinationId": "Save as PDF",
                    "version": 2,
                }
            ),
        },
    ):
        self.options = options
        self.chrome_options = None

        self.set_options(self.options)
        self.close_driver_thread = CloseDriverThread(self.driver)
        self.actions = ActionChains(self.driver)

    def set_driver(self):
        self.driver = webdriver.Chrome(options=self.chrome_options) # type: ignore
        self.close_driver_thread = CloseDriverThread(self.driver)
        self.actions = ActionChains(self.driver)
        

    def set_options(self, options: dict, headless: bool = True, **kwargs):
        self.chrome_options = webdriver.ChromeOptions()

        # auto download pdf files
        self.chrome_options.add_experimental_option("prefs", options)
        
        if headless:
            self.chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            self.chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            self.chrome_options.add_experimental_option('useAutomationExtension', False)
            self.chrome_options.add_argument("--headless")
            self.chrome_options.add_argument(f"--user-agent={USER_AGENT}")
            self.chrome_options.add_argument("--window-size=1280,720")
            self.chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            
            
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--disable-extensions")
        self.chrome_options.add_argument("--disable-3d-apis")
        self.chrome_options.add_argument("--kiosk-printing")

        if not hasattr(self, "driver"):
            self.driver = webdriver.Chrome(options=self.chrome_options)
        else:
            self.driver.options = self.chrome_options

    def get(self, url: str):
        self.driver.get(url)

    def get_driver(self):
        return self.driver

    def click(self, by, value):
        self.driver.find_element(by, value).click()

    def check_if_element_exists(self, by, value):
        return len(self.driver.find_elements(by, value)) > 0
    
    def type_text(self, element, text):
        # clear text and type new text
        self.actions.move_to_element(element).click().send_keys(Keys.CONTROL + "a").send_keys(Keys.DELETE).perform()
        self.actions.move_to_element(element).click().send_keys(text).perform()
    
    def wait_element_to_be_clickable(self, by, value, timeout: int = 5):
        WebDriverWait(self.driver, timeout).until(
            EC.element_to_be_clickable((by, value))
        )
        
    def wait_element_to_be_visible(self, by, value, timeout: int = 5):
        WebDriverWait(self.driver, timeout).until(
            EC.visibility_of_element_located((by, value))
        )
    
    def wait_number_of_windows_to_be(self, number: int, timeout: int = 5):
        WebDriverWait(self.driver, timeout).until(EC.number_of_windows_to_be(number))

    def wait_frame_to_be_available_and_switch_to_it(self, by, value, timeout: int = 5):
        WebDriverWait(self.driver, timeout).until(
            EC.frame_to_be_available_and_switch_to_it((by, value))
        )

    def close(self):
        self.close_driver_thread.start()
