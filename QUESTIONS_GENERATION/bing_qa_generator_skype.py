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
    ElementClickInterceptedException,
    StaleElementReferenceException,
)
from markdownify import markdownify as md
from threading import Thread, Lock, Timer

load_dotenv()

BING_URL = "https://web.skype.com"

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
        self.formatted_prompt = ""
        self.driver_stopped = False
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

    # @click_reconnect_decorator
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

    # @click_reconnect_decorator
    def type_text(self, text: str):
        # paste text into the input div
        # <div data-offset-key="0-0-0" class="public-DraftStyleDefault-block public-DraftStyleDefault-ltr"><span data-offset-key="0-0-0"><br data-text="true"></span></div>

        input = self.driver.find_element(
            By.CSS_SELECTOR, "div.public-DraftStyleDefault-block"
        )
        input.click()

        # use action chains to type text. Avoid submitting text before it's fully typed beucase of \n
        actions = ActionChains(self.driver)
        for part in text.split("\n"):
            actions.send_keys(part)
            actions.key_down(Keys.SHIFT).key_down(Keys.ENTER).key_up(Keys.SHIFT).key_up(
                Keys.ENTER
            ).perform()

        # input.send_keys(Keys.CONTROL + "a")
        # input.send_keys(Keys.DELETE)
        # input.send_keys(text)
        # # send space to trigger the text input
        # input.send_keys(Keys.SPACE)
        time.sleep(1)

    # @click_reconnect_decorator
    def click_send_button(self):
        """Click send button"""
        #    <button role="button" title="Enviar mensagem" aria-label="Enviar mensagem" aria-disabled="false" style="position: relative; display: flex; flex-direction: column; flex-grow: 0; flex-shrink: 0; overflow: visible; align-items: center; justify-content: center; app-region: no-drag; background-color: transparent; border-color: transparent; text-align: left; border-width: 0px; height: 44px; width: 44px; padding: 0px; cursor: pointer; border-style: solid;"><div role="none" style="position: absolute; display: flex; flex-direction: column; flex-grow: 0; flex-shrink: 0; overflow: hidden; align-items: center; background: linear-gradient(135deg, rgb(16, 84, 173), rgb(9, 154, 222)); height: 40px; width: 40px; border-radius: 20px; top: 2px; left: 2px; justify-content: center;"><div role="none" aria-hidden="true" style="position: relative; display: flex; flex-direction: column; flex-grow: 0; flex-shrink: 0; overflow: hidden; align-items: stretch; margin-left: 2px;"><svg width="20" height="20" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="#FFFFFF" gradientcolor1="#FFFFFF" gradientcolor2="#FFFFFF"><path d="M5.694 12 2.299 3.272c-.236-.608.356-1.189.942-.982l.093.04 18 9a.75.75 0 0 1 .097 1.283l-.097.058-18 9c-.583.291-1.217-.245-1.065-.847l.03-.096L5.694 12 2.299 3.272 5.694 12ZM4.402 4.54l2.61 6.71h6.627a.75.75 0 0 1 .743.648l.007.102a.75.75 0 0 1-.649.743l-.101.007H7.01l-2.609 6.71L19.322 12 4.401 4.54Z"></path></svg></div></div></button>

        button = self.driver.find_element(
            By.CSS_SELECTOR, "button[aria-label='Enviar mensagem']"
        )
        button.click()
        time.sleep(1)

    def click_try_it_button_and_resend(self):
        """ After clciking send button for the 1st time, there may appear a popup with try it button. Click it and then click send button again."""
        
        # <button role="button" title="Try it" aria-label="Try it" style="position: relative; display: flex; flex-direction: column; flex-grow: 1; flex-shrink: 1; overflow: hidden; align-items: stretch; justify-content: center; app-region: no-drag; background-color: transparent; border-color: transparent; text-align: left; border-width: 0px; margin-left: 6px; margin-right: 6px; min-width: 128px; margin-top: 6px; padding: 0px; cursor: pointer; border-style: solid;"><div role="none" style="position: relative; display: flex; flex-direction: column; flex-grow: 0; flex-shrink: 0; overflow: hidden; align-items: center; justify-content: center; height: 40px; border-radius: 20px; padding-left: 25px; padding-right: 25px; opacity: 1; transform: scale(1) translateX(0px) translateY(0px); transition: none 0s ease 0s;"><div role="none" style="position: absolute; display: flex; flex-direction: column; flex-grow: 0; flex-shrink: 0; overflow: hidden; align-items: stretch; background: linear-gradient(135deg, rgb(9, 154, 222), rgb(16, 84, 173)); min-height: 40px; inset: 0px;"></div><div data-text-as-pseudo-element="Try it" dir="auto" style="position: relative; display: inline; flex-grow: 0; flex-shrink: 0; overflow: hidden; white-space: pre; text-overflow: ellipsis; font-size: 15px; font-family: &quot;SF Regular&quot;, &quot;Segoe System UI Regular&quot;, &quot;Segoe UI Regular&quot;, sans-serif; font-weight: 400; color: rgb(255, 255, 255); cursor: inherit;"></div></div></button>
        try:
            button = self.driver.find_element(
                By.CSS_SELECTOR, "button[aria-label='Try it']"
            )
            button.click()
            time.sleep(1)
            self.click_send_button()
        except NoSuchElementException:
            pass
    
    def wait_until_bot_stops_answering(self):
        """Wait until bot stops answering. Only finishes this function after 3 tries finding the element. Because sometimes the element vanishes but then appears again."""
        #    <div data-text-as-pseudo-element="Bing está digitando" dir="auto" style="position: relative; display: inline; flex-grow: 0; flex-shrink: 1; overflow: hidden; white-space: pre; text-overflow: ellipsis; font-size: 12px; color: rgb(110, 110, 110); font-family: &quot;SF Regular&quot;, &quot;Segoe System UI Regular&quot;, &quot;Segoe UI Regular&quot;, sans-serif; font-weight: 400; margin-right: 5px; cursor: inherit;"></div>
        n_tries = 5
        while n_tries > 0:
            try:
                typing_div = self.driver.find_element(
                    By.CSS_SELECTOR,
                    "div[data-text-as-pseudo-element='Bing está digitando']",
                )

                # check if typing div is visible
                if typing_div.is_displayed():
                    n_tries = 5

                if n_tries == 0:
                    break
            except NoSuchElementException:
                n_tries -= 1
            # stale element exception may happen if element is not visible anymore
            except StaleElementReferenceException:
                n_tries -= 1
            except Exception as e:
                print(f"Driver id: {self.driver_id} | Error at wait_until_bot_stops_answering: {e}")
                break

            time.sleep(2)

        time.sleep(1)

    # @click_reconnect_decorator
    def get_response(self, retries: int = 3):
        """Get response from Bing"""

        # <div role="region" tabindex="-1" style="position: absolute; display: flex; flex-direction: column; flex-grow: 0; flex-shrink: 0; overflow: visible; align-items: stretch; will-change: transform; width: 571px; transform: translateY(23671px);" aria-label="Bing,

        if retries == 0:
            # stop driver. Probably limit reached
            self.stop_driver()
            return ""
        
        # find last <div role="region" with aria-label="Bing, ..."
        regex_aria_label = re.compile(r"Bing, (.*?)>")
        messages = self.driver.find_elements(By.CSS_SELECTOR, "div[role='region']")

        try:
            if regex_aria_label.search(
                messages[-1].get_attribute("outerHTML")
            ):  # check to see if message is what we are looking for
                message = messages[-1]
            else:
                message = messages[-2]
        except NoSuchElementException:
            message = messages[-2]
            
        limit_messages = ["Você atingiu seu limite diário de chats", "Vamos buscar novamente amanhã!", "You've reached your daily chat limit", "We'll try again tomorrow!"]
        
        # check if there was error in getting response: "Você atingiu seu limite diário de chats"
        if any(limit.lower() in message.get_attribute("textContent").lower() for limit in limit_messages):
            # stop driver
            self.stop_driver()
            return None
        
        error_messages = ['Está demorando mais do que o normal', 'Avisaremos quando você puder falar comigo', 'estarei respondendo muito em breve', 'Desculpe', 'Sorry', 'mais tempo para responder']
        # check if there was error in getting response: "Desculpe..." 
        # if "Desculpe" in message.get_attribute("textContent") or "Sorry" in message.get_attribute("textContent"):
        if any(error.lower() in message.get_attribute("textContent").lower() for error in error_messages):
            # try sending message again
            self.type_text(self.formatted_prompt)
            self.click_send_button()
            self.click_try_it_button_and_resend()
            time.sleep(5)
            
            self.wait_until_bot_stops_answering()
            return self.get_response(retries - 1)
        # recursive call until get response
             

        # markdownify response
        response = message.get_attribute("textContent")
        json_strings = md(
            response.replace("“", '"').replace("”", '"'), strip=["a"]
        )  # remove links. if need sources, remove 'a' from strip

        return json_strings

    def click_bing_chat(self):
        """Clicks Bing chat inside Skype. Bing wil be pinned as first chat"""
        # <div role="button" tabindex="0" id="rx-vlv-5" aria-label="pinned, bot, Bing, chat, Olá, isso é  Bing ! Como posso ajudá-lo hoje? 
        time.sleep(1)
        # find div role=button with aria-label="pinned, bot, Bing
        try:
            bing_chat = self.driver.find_element(
                By.XPATH,
                '//div[@role="button" and contains(@aria-label, "bot, Bing")]',
            )
            bing_chat.click()
        except ElementClickInterceptedException:
            # try it popup may have appeared. Click it and continue
            self.click_try_it_button_and_resend()
        except Exception as e:
            print(f"Error at click_bing_chat: {e}")
            
        time.sleep(1)

    def get_answer(self, prompt: str):
        """Get answer from Bing based on context"""
        self.formatted_prompt = prompt
        # with lock:
            # self.click_reject_button()
            # self.change_to_creative_tone()
            # error = self.check_limit()
            # if error:
            #     # refresh page and setup driver again
            #     print(f"Error at driver {self.driver_id}: {error}")
            #     self.driver.refresh()
            #     self.setup_driver()
            #     self.responses_count = 0

            #     # # try again
            #     # self.click_reject_button()
            #     # self.change_to_creative_tone()
            #     # error = self.check_reconnect_or_limit()
            #     # if error:
            #     #     print(f"Error at driver {self.driver_id}: {error}")
            #     return {"content": "", "json_strings": ""}

        self.click_bing_chat()

        self.type_text(prompt)
        self.click_send_button()
        self.click_try_it_button_and_resend()

        time.sleep(5)

        self.wait_until_bot_stops_answering()

        # with lock:
            # self.click_get_started_button()
        time.sleep(1)
        json_strings = self.get_response()

        result = {"content": "", "json_strings": json_strings}
        random_wait = random.randint(1, 5)
        print(f"Driver {self.driver_id} waiting {random_wait} seconds...")
        time.sleep(random_wait)

        # increment responses count
        self.responses_count += 1
        
         
        
        # if responses count is greater than 10, refresh page
        # if self.responses_count >= 9: # NOT USING NOW FOR BING SKYPE
        #     print(
        #         f"Refreshing page for driver {self.driver_id}... | Responses count: {self.responses_count}"
        #     )
        #     self.driver.refresh()
        #     self.setup_driver()
        #     self.responses_count = 0

        return result
    
    def stop_driver(self):
        """Stop driver."""
        self.driver_stopped = True

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
        data_dir: str,
        profile_directory: str,
    ):
        super().__init__(daemon=True)
        self.context_queue = context_queue
        self.qa_queue = qa_queue
        self.driver_id = driver_id
        self.data_dir = data_dir
        self.profile_directory = profile_directory
        self.prompt = """--------- CONTEXTO ---------
        {filename}
        
        {context}
        --------- FIM DE CONTEXTO ---------"""
        self.post_prompt = """Dada a pergunta e resposta gerada, melhore o resultado ao gerar três perguntas e respostas longas, detalhadas e que tragam mais informações para as perguntas. A primeira pergunta é Gere outras 2 perguntas diferentes, detalhadas e longas. TODAS NO FORMATO JSON ESPECIFICADOS. Faça BUSCAS NA INTERNET e o retorne o resultado no FORMATO JSON. Use aspas simples, caso necessário, nos valores JSON.Lembre-se de especificar o número e data do documento legislativo nas perguntas (quando necessário) e respostas.
A saída esperada de você, em JSON, deve estar no formato abaixo:
    [{   
        "pergunta": "{pergunta_original}",
        "resposta": "{resposta_melhorada}"
        },
        {   
        "pergunta": "{pergunta2}",
        "resposta": "{resposta2}"
        },
        {   
        "pergunta": "{pergunta3}",
        "resposta": "{resposta3}"
    }]
Retorne as 3 perguntas e respostas no formato JSON especificado acima. Gere conteúdo somente baseando-se nos documentos que você encontrar na internet Não retorne nada além do JSON.
"""
        self.bing_driver: "SeleniumBing" = None
        self.working = True
        self.initialize_driver()

    def initialize_driver(self):
        """Initialize driver manager thread."""
        # options = EdgeOptions()
        chrome_options = ChromeOptions()
        # chrome_options = uc.ChromeOptions()

        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument("--disable-dev-shm-using")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-application-cache")

        # options.use_chromium = True
        # options.add_argument("--disable-gpu")
        # options.add_argument("--no-sandbox")
        # options.add_argument("--disable-setuid-sandbox")

        # add user data dir
        # options.add_argument(f"--user-data-dir={self.data_dir}")
        # options.add_argument(f"--profile-directory=Profile {self.profile_directory.strip()}")
        chrome_options.add_argument(f"--user-data-dir={self.data_dir}")
        
        # add headless option
        # # options.add_argument("headless") # commented because of error

        # # add incognito mode
        # options.add_argument("-inprivate")

        self.bing_driver = SeleniumBing(
            # driver=webdriver.Edge(options=options),
            driver=webdriver.Chrome(options=chrome_options),
            # driver=uc.Chrome(options=chrome_options, headless=False, version_main=118),
            driver_id=self.driver_id,
        )

    def format_prompt(self, context: str):
        # context is a tuple (file_index, file_name, text, question, answer)
        self.context = (
            '{"pergunta": "' + context[3] + '", "resposta": "' + context[4] + '"}'
        )
        self.filename = Path(context[1]).stem

        self.formatted_prompt = f"{self.prompt.format(context=self.context, filename=self.filename)}\n{self.post_prompt.replace('{pergunta_original}', context[3])}"
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
                
                # check if driver_stop (due to limit reached) and exit
                if self.bing_driver.driver_stopped:
                    print(f"Driver {self.driver_id} stopped (Limit reached). Exiting...")
                    break
                
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
        # self.num_drivers = num_drivers
        self.qa_queue = Queue()
        self.drivers_threads: "list[DriverManager]" = []
        self.context = ""
        self.initialize_drivers()

    def initialize_drivers(self):
        """Initialize drivers threads."""
        # get user data dirs from env (will be EDGE_USER_DIR{index}). Filter all env variables that start with EDGE_USER_DIR
        data_dirs = [
            environ[key] for key in environ.keys() if key.startswith("EDGE_USER_DIR")
        ]
        print(f"Initializing {len(data_dirs)} drivers...")

        for index in range(len(data_dirs)):
        # for index in range(1):  # temp for testing
            driver_manager = DriverManager(
                self.context_queue,
                self.qa_queue,
                driver_id=index,
                # remove profile part from dir EDGE_USER_DIR0=C:\Users\Felipe O E Santo\AppData\Local\Microsoft\Edge\User Data\Profile 1
                # data_dir=str(Path(data_dirs[index])).split("Profile")[0],
                data_dir=Path(data_dirs[index]).as_posix(),
                # ("profile-directory=Profile 1");
                profile_directory=str(Path(data_dirs[index])).split("Profile")[1],
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
