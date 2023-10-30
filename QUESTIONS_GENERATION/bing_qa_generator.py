import time
import json
import random
import json
import re
import time
import random
from pathlib import Path
from requests import Session
from dotenv import load_dotenv
from os import environ
from datetime import datetime
from multiprocessing import Queue
from queue import Empty

from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.chrome.options import Options as ChromeOptions
import undetected_chromedriver as uc
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementNotInteractableException,
)
from markdownify import markdownify as md
from threading import Thread, Lock, Timer

BING_URL = "https://www.bing.com/search?form=NTPCHB&q=Bing+AI&showconv=1"

lock = Lock()


# decorator to click reconnect button before executing function
def click_reconnect_decorator(func):
    def wrapper(self, *args, **kwargs):
        self.click_reconnect_button()
        return func(self, *args, **kwargs)

    return wrapper


class SeleniumBing:
    """Class to handle interactions with Bing website to get QA pairs"""

    def __init__(self, driver: webdriver.Edge, driver_id: int):
        self.driver = driver
        self.driver_id = driver_id
        self.responses_count = 0
        self.first_parent_element = None
        self.setup_driver()

    def setup_driver(self):
        """Setup driver before starting to get QA pairs"""
        self.driver.maximize_window()
        self.driver.get(BING_URL)
        time.sleep(8)

    def click_reject_button(self):
        """Click reject button"""
        try:
            reject_button = self.driver.find_element(
                By.XPATH, '//button[@id="bnp_btn_reject"]'
            )
            reject_button.click()
            time.sleep(1)
        except ElementNotInteractableException:
            pass
        except Exception as e:
            # print(e)
            pass
        
    @click_reconnect_decorator
    def change_to_creative_tone(self):
        """Change to creative tone"""
        # change to creative tone. It's inside shadow root, so need to query inside shadowRoot of cib-tone-selector
        first_parent_element = self.driver.find_element(By.CSS_SELECTOR, "cib-serp")
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", first_parent_element
        )
        second_parent_element = shadow_root.find_element(
            By.CSS_SELECTOR, "#cib-conversation-main"
        )
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", second_parent_element
        )
        third_parent_element = shadow_root.find_element(
            By.CSS_SELECTOR, "cib-welcome-container"
        )
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", third_parent_element
        )
        fourth_parent_element = shadow_root.find_element(
            By.CSS_SELECTOR, "cib-tone-selector"
        )
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", fourth_parent_element
        )
        creative_tone = shadow_root.find_element(
            By.CSS_SELECTOR, "button.tone-creative"
        )
        try:
            creative_tone.click()
            time.sleep(1)
        except Exception as e:
            pass

        # set first parent element to be used later
        self.first_parent_element = first_parent_element

    @click_reconnect_decorator
    def type_text(self, text: str):
        # execute script to paste text into the input text box
        # document.querySelector("#b_sydConvCont > cib-serp").shadowRoot.querySelector("#cib-action-bar-main").shadowRoot.querySelector("div > div.main-container > div > div.input-row > cib-text-input").shadowRoot.querySelector("#searchboxform > label")
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", self.first_parent_element
        )
        second_parent_element = shadow_root.find_element(
            By.CSS_SELECTOR, "#cib-action-bar-main"
        )
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", second_parent_element
        )
        third_parent_element = shadow_root.find_element(
            By.CSS_SELECTOR, " cib-text-input"
        )
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", third_parent_element
        )
        input = shadow_root.find_element(By.CSS_SELECTOR, "#searchboxform > label")
        input.click()

        textarea_ref = self.driver.execute_script(
            "return arguments[0].textAreaRef", third_parent_element
        )
        # change text area value
        self.driver.execute_script(
            "arguments[0].value = arguments[1]", textarea_ref, text
        )

        # use action chains to type text
        actions = ActionChains(self.driver)
        # actions.send_keys(Keys.CONTROL + "a")
        # actions.send_keys(Keys.DELETE)
        actions.send_keys(Keys.SPACE)
        actions.perform()

        # input.send_keys(Keys.CONTROL + "a")
        # input.send_keys(Keys.DELETE)
        # input.send_keys(text)
        # # send space to trigger the text input
        # input.send_keys(Keys.SPACE)
        time.sleep(1)

    @click_reconnect_decorator
    def click_send_button(self):
        """Click send button"""
        # document.querySelector("#b_sydConvCont > cib-serp").shadowRoot.querySelector("#cib-action-bar-main").shadowRoot.querySelector("div > div.main-container > div > div.bottom-controls > div.bottom-right-controls > div.control.submit > button")
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", self.first_parent_element
        )
        second_parent_element = shadow_root.find_element(
            By.CSS_SELECTOR, "#cib-action-bar-main"
        )
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", second_parent_element
        )
        send_button = shadow_root.find_element(
            By.CSS_SELECTOR,
            "div.bottom-controls > div.bottom-right-controls > div.control.submit > button",
        )
        send_button.click()
        time.sleep(1)

    def wait_until_bot_stops_answering(self):
        """Wait until bot stops answering"""
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", self.first_parent_element
        )
        second_parent_element = shadow_root.find_element(
            By.CSS_SELECTOR, "#cib-action-bar-main"
        )
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", second_parent_element
        )
        cib_typing_indicator = shadow_root.find_element(
            By.CSS_SELECTOR, "cib-typing-indicator"
        )
        # shadow_root = self.driver.execute_script(
        #     "return arguments[0].shadowRoot", cib_typing_indicator
        # )
        # stop_answering_button = shadow_root.find_element(
        #     By.CSS_SELECTOR, "#stop-responding-button"
        # )
        # get ariaHidden attribute
        while cib_typing_indicator.get_attribute("ariaHidden") == "false":
            # while stop_answering_button.is_enabled():
            time.sleep(1)
            # print(f"Driver id: {self.driver_id} | Waiting for bot to stop answering...")
            try:
                # stop_answering_button = shadow_root.find_element(
                #     By.CSS_SELECTOR, "#stop-responding-button"
                # )
                cib_typing_indicator = shadow_root.find_element(
                    By.CSS_SELECTOR, "cib-typing-indicator"
                )
            except NoSuchElementException:
                break
            except Exception as e:
                print(e)
                break
        time.sleep(2)

    @click_reconnect_decorator
    def click_get_started_button(self):
        """Click get started button if it appears"""

        try:
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", self.first_parent_element
            )
            second_parent_element = shadow_root.find_element(
                By.CSS_SELECTOR, "#cib-conversation-main"
            )
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", second_parent_element
            )
            third_parent_element = shadow_root.find_element(
                By.CSS_SELECTOR, "#cib-chat-main > cib-chat-turn"
            )
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", third_parent_element
            )
            fourth_parent_element = shadow_root.find_element(
                By.CSS_SELECTOR, "cib-message-group.response-message-group"
            )
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", fourth_parent_element
            )
            # get last cib-message inside the message group
            cib_messages = shadow_root.find_elements(By.CSS_SELECTOR, "cib-message")

            fifth_parent_element = cib_messages[-1]
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", fifth_parent_element
            )
            sixth_parent_element = shadow_root.find_element(
                By.CSS_SELECTOR, "cib-shared > div > cib-muid-consent"
            )
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", sixth_parent_element
            )
            button = shadow_root.find_element(
                By.CSS_SELECTOR, "div.get-started-btn-wrapper-inline > button"
            )
            button.click()
            time.sleep(0.5)
        except NoSuchElementException:
            pass
        except Exception as e:
            print(e)

    @click_reconnect_decorator
    def get_response(self):
        """Get response from Bing"""
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", self.first_parent_element
        )
        second_parent_element = shadow_root.find_element(
            By.CSS_SELECTOR, "#cib-conversation-main"
        )
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", second_parent_element
        )
        chat_turns = shadow_root.find_elements(
            By.CSS_SELECTOR, "#cib-chat-main > cib-chat-turn"
        )
        last_chat_turn = chat_turns[-1]
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", last_chat_turn
        )
        # find last cib-message-group that is a response-message-group
        message_groups = shadow_root.find_elements(
            By.CSS_SELECTOR, "cib-message-group.response-message-group"
        )
        last_message_group = message_groups[-1]
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", last_message_group
        )
        # find last cib-message inside the message group
        messages = shadow_root.find_elements(By.CSS_SELECTOR, "cib-message")
        # filter 'Ao continuar sua interação com o Bing, você está aceitando o Termos de uso e confirmando que você revisou o Política de privacidade.\nContinuar' message
        cib_messages = [
            cib_message
            for cib_message in messages
            if "Ao continuar sua interação com o Bing" not in cib_message.text
        ]
        last_message = cib_messages[-1]
        shadow_root = self.driver.execute_script(
            "return arguments[0].shadowRoot", last_message
        )
        # find cib-shared inside the message
        try:
            shared = shadow_root.find_element(By.CSS_SELECTOR, "cib-shared")
            response = shared.text
            if not response or not response.strip():
                response = cib_messages[-2].text
        except NoSuchElementException:  # if any excepttion occurs, get previous message
            response = cib_messages[-2].text
            # print(f"Error at get_response: {response}")
        except Exception as e:
            print(f"Error at get_response: {e}")
            response = cib_messages[-2].text

        # markdownify response
        json_strings = md(
            response.replace("“", '"').replace("”", '"'), strip=["a"]
        )  # remove links. if need sources, remove 'a' from strip

        return json_strings

    def click_reconnect_button(self):
        """Click reconnect button if it appears"""
        try:
            first_parent_element = self.driver.find_element(By.CSS_SELECTOR, "cib-serp")
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", first_parent_element
            )
            second_parent_element = shadow_root.find_element(
                By.CSS_SELECTOR, "#cib-conversation-main"
            )
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", second_parent_element
            )
            third_parent_element = shadow_root.find_element(
                By.CSS_SELECTOR, "#cib-chat-main > cib-notification-container"
            )
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", third_parent_element
            )
            fourth_parent_element = shadow_root.find_element(
                By.CSS_SELECTOR, "cib-notification"
            )
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", fourth_parent_element
            )
            fifth_parent_element = shadow_root.find_element(
                By.CSS_SELECTOR, "div > div"
            )

            fifth_parent_element.click()
            time.sleep(1)
        except NoSuchElementException:
            pass
            return None
        except ElementNotInteractableException:
            return None
        except Exception as e:
            print(f"Error at click_reconnect_button: {e}")
            return None

    def check_limit(self):
        "Click reconnect link if it appears"
        #  document.querySelector("#b_sydConvCont > cib-serp").shadowRoot.querySelector("#cib-conversation-main").shadowRoot.querySelector("#cib-chat-main > cib-notification-container").shadowRoot.querySelector("div > div > cib-notification").shadowRoot.querySelector("div > div > button")
        try:
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", self.first_parent_element
            )
            second_parent_element = shadow_root.find_element(
                By.CSS_SELECTOR, "#cib-conversation-main"
            )
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", second_parent_element
            )
            third_parent_element = shadow_root.find_element(
                By.CSS_SELECTOR, "#cib-chat-main > cib-notification-container"
            )
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", third_parent_element
            )
            fourth_parent_element = shadow_root.find_element(
                By.CSS_SELECTOR, "cib-notification"
            )
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", fourth_parent_element
            )
            fifth_parent_element = shadow_root.find_element(
                By.CSS_SELECTOR, "div > div"
            )

            return fifth_parent_element.text
            # fifth_parent_element.click()
            # time.sleep(1)
        except NoSuchElementException:
            pass
            return None
        except ElementNotInteractableException:
            return None
        except Exception as e:
            print(f"Error at check_limit: {e}")
            return None

    def get_answer(self, prompt: str):
        """Get answer from Bing based on context"""
        with lock:
            self.click_reject_button()
            self.change_to_creative_tone()
            error = self.check_limit()
            if error:
                # refresh page and setup driver again
                print(f"Error at driver {self.driver_id}: {error}")
                self.driver.refresh()
                self.setup_driver()
                self.responses_count = 0

                # # try again
                # self.click_reject_button()
                # self.change_to_creative_tone()
                # error = self.check_reconnect_or_limit()
                # if error:
                #     print(f"Error at driver {self.driver_id}: {error}")
                return {"content": "", "json_strings": ""}

            self.type_text(prompt)
            self.click_send_button()

        time.sleep(5)

        self.wait_until_bot_stops_answering()

        with lock:
            self.click_get_started_button()
            time.sleep(1)
            json_strings = self.get_response()

        result = {"content": "", "json_strings": json_strings}
        random_wait = random.randint(1, 5)
        print(f"Driver {self.driver_id} waiting {random_wait} seconds...")
        time.sleep(random_wait)

        # increment responses count
        self.responses_count += 1
        # if responses count is greater than 10, refresh page
        if self.responses_count >= 9:
            print(
                f"Refreshing page for driver {self.driver_id}... | Responses count: {self.responses_count}"
            )
            self.driver.refresh()
            self.setup_driver()
            self.responses_count = 0

        return result

    def close(self):
        """Close selenium driver."""
        self.driver.close()


class DriverManager(Thread):
    """Thread class to manage drivers in order to generate QA"""

    def __init__(
        self,
        context_queue: Queue,
        qa_queue: Queue,
        driver_id: int,
    ):
        super().__init__(daemon=True)
        self.context_queue = context_queue
        self.qa_queue = qa_queue
        self.driver_id = driver_id
        self.prompt = """--------- CONTEXTO ---------
        {filename}
        
        {context}
        --------- FIM DE CONTEXTO ---------"""
        self.post_prompt = """Dadas as pergunta e resposta geradas, melhore o resultado ao gerar uma resposta longa e que traga mais informações para a pergunta. Gere outras 2 perguntas diferentes e longas. Faça BUSCAS NA INTERNET e o retorne o resultado no FORMATO JSON. Use aspas simples, caso necessário, nos valores JSON.Lembre-se de especificar o número e data do documento legislativo nas perguntas (quando necessário) e respostas.
A saída esperada de você, em JSON, deve ser no formato:
    [{   
        "pergunta": "{pergunta1}",
        "resposta": "{resposta1}"
        },
        {   
        "pergunta": "{pergunta2}",
        "resposta": "{resposta2}"
        },
        {   
        "pergunta": "{pergunta3}",
        "resposta": "{resposta3}"
    }]
Retorne as 3 perguntas e respostas no formato especificado. Não gere nada além do JSON.
"""
        self.bing_driver: "SeleniumBing" = None
        self.working = True
        self.initialize_driver()

    def initialize_driver(self):
        """Initialize driver manager thread."""
        options = EdgeOptions()
        # chrome_options = ChromeOptions()
        chrome_options = uc.ChromeOptions()
    
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument("--disable-dev-shm-using")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-application-cache")

        # add headless option
        # options.use_chromium = True
        
        # # add incognito mode
        # options.add_argument("-inprivate")
        
        # # options.add_argument("headless") # commented because of error
        # options.add_argument("disable-gpu")
        # chrome_options.add_argument("--headless")
        # chrome_options.add_argument("--disable-gpu")

        self.bing_driver = SeleniumBing(
            driver=webdriver.Edge(options=options),
            # driver=webdriver.Chrome(options=chrome_options),
            # driver=uc.Chrome(options=chrome_options, headless=False, version_main=118),
            driver_id=self.driver_id,
        )

    def format_prompt(self, context: str):
        # context is a tuple (file_index, file_name, text, question, answer)
        self.context = (
            '{"pergunta": "' + context[3] + '", "resposta": "' + context[4] + '"}'
        )
        self.filename = Path(context[1]).stem

        self.formatted_prompt = f"{self.prompt.format(context=self.context, filename=self.filename)}\n{self.post_prompt}"
        return self.formatted_prompt

    def load_json(self, json_string: str):
        """Load JSON from string."""
        json_blocks = re.findall(r"{(.*?)}", json_string, re.DOTALL)

        # check if block contains double quotes in 'resposta' or 'pergunta' content
        for block in json_blocks:
            if '"resposta":' in block:
                block_content = block.split('"resposta":')[1]

                # remove first and last double quotes
                block_content = block_content.strip()[1:-1]
                if '"' in block_content:
                    block_content = block_content.split('"')[1]
                    block = block.replace(
                        block_content, json.dumps(block_content, ensure_ascii=False)
                    )
            if '"pergunta":' in block:
                block_content = block.split('"pergunta":')[1]
                if '"' in block_content:
                    block_content = block_content.split('"')[1]
                    block = block.replace(
                        block_content, json.dumps(block_content, ensure_ascii=False)
                    )

        json_data = []
        for block in json_blocks:
            # convert block to json.

            while True:
                try:
                    format_block = block.strip()
                    format_block = "{" + format_block + "}"

                    # check if json block ends with comma and remove it. May have \n before or after comma.
                    regex = re.compile(r",\s*}")
                    format_block = regex.sub("}", format_block)

                    json_block = json.loads(format_block, strict=False)
                    break
                except json.decoder.JSONDecodeError as e:
                    if e.args[0].startswith("Unterminated string starting at"):
                        # add double quotes
                        block = block + '"'

                    # invalid escape sequence '\ '
                    elif "Invalid" in e.args[0] and "escape" in e.args[0]:
                        block = block.replace("\\", "")
                    else:
                        raise e

            # check if block is a list of dicts
            if isinstance(json_block, list):
                json_data.extend(json_block)
            else:
                json_data.append(json_block)

        return json_data

    def get_qa_json(self, generated_text: str):
        """Get QA JSON from generated text."""
        json_object = self.load_json(generated_text)
        questions = []
        answers = []
        if json_object:
            try:
                for item in json_object:
                    questions.append(item["pergunta"])
                    answers.append(item["resposta"])

                return (
                    self.context,
                    questions,
                    answers,
                )
            except Exception as e:
                print(f"Error at get_qa_json: {e}")

        return None, None, None

    def get_qa(self, generated_text: str):
        """Get QA from context."""
        return self.get_qa_json(generated_text)

    def run(self):
        "Indefinetely get context from queue and generate QA." ""
        empty_queue_count = 0
        while True:
            try:
                # check if queue is empty
                if (
                    getattr(self.context_queue, "start_monitoring", False)
                    and self.context_queue.empty()
                ):
                    empty_queue_count += 1
                    if empty_queue_count >= 10:
                        print(f"Driver {self.driver_id} exiting...")
                        break
                    time.sleep(1)
                    continue

                context = self.context_queue.get(timeout=1)
                if context:
                    # print(f"File {context[1]} has id {context[0]}")
                    formatted_context = self.format_prompt(context)
                    result = self.bing_driver.get_answer(formatted_context)
                    if not result["json_strings"]:
                        continue

                    # generated_text = result["content"]
                    generated_jsons = result["json_strings"]
                    qa_result = self.get_qa(generated_jsons)

                    print(
                        f"Driver id: {self.driver_id} | Generated {len(qa_result[1]) if qa_result[1] else 0} questions and answers for file {context[1]}"
                    )
                    qa_obj = {
                        "file_index": context[0],
                        "file_name": context[1],
                        "context": qa_result[0],
                        "prompt": formatted_context,
                        "question": context[3],
                        "answer": context[4],
                        "new_questions": qa_result[1],
                        "new_long_answers": qa_result[2],
                    }
                    self.qa_queue.put(qa_obj)

                    empty_queue_count = 0
            except Empty:
                continue
            except Exception as e:
                print(f"Error at DriverManager.run | Driver id: {self.driver_id}: {e}")
                continue

        # close selenium driver
        self.bing_driver.close()
        self.working = False


class BingQAGenerator:
    """Class to generate QA pairs from Bing website."""

    def __init__(
        self,
        contents_queue: Queue,
        num_drivers: int = 1,
    ):
        self.context_queue = contents_queue
        self.num_drivers = num_drivers
        self.qa_queue = Queue()
        self.drivers_threads: "list[DriverManager]" = []
        self.context = ""
        self.initialize_drivers()

    def initialize_drivers(self):
        """Initialize drivers threads."""
        for index in range(self.num_drivers):
            driver_manager = DriverManager(
                self.context_queue, self.qa_queue, driver_id=index
            )
            driver_manager.start()
            self.drivers_threads.append(driver_manager)
            print(f"Initialized driver manager thread {index}.")

        time.sleep(3)

        print(f"Initialized {len(self.drivers_threads)} driver manager threads.")

    def generate_qa(self):
        # wait for qa to be generated and return it
        while True:
            try:
                qa_result = self.qa_queue.get(timeout=5)
                return qa_result
            except Empty:
                print("Queue empty. Waiting 5 seconds...")
                return None
            except Exception as e:
                print(f"Error at generate_qa: {e}")
                continue

    def get_working_drivers(self):
        return [driver.driver_id for driver in self.drivers_threads if driver.working]

    def __del__(self):
        # join all threads
        for driver in self.drivers_threads:
            driver.join()

        # close all drivers
        for driver in self.drivers_threads:
            driver.bing_driver.close()
