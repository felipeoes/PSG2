import json
import re
import time
import random
from pathlib import Path
from requests import Session
from dotenv import load_dotenv
from os import environ
from datetime import datetime

from threading import Thread, Lock, Timer
from selenium_helper import SeleniumHelper, By, Keys
from markdownify import markdownify as md
from multiprocessing import Queue, Process
from queue import Empty

load_dotenv()
BARD_URL = "https://bard.google.com"

lock = Lock()


class SeleniumBard:
    """Selenium class that interacts with Bard in browser to get QA."""

    def __init__(
        self,
        driver: SeleniumHelper,
        driver_id: int,
    ):
        self.driver = driver
        self.driver_id = driver_id
        self.driver.get(BARD_URL).wait_time(5)
        self.last_response = None
        self.responses_count = 0

    def get_previous_text_query(self):
        # get previous text query. Attribute is textContent
        previous_text_query = None

        try:
            previous_text_query = self.driver.find_elements(
                By.CSS_SELECTOR, "#user-query-content-0"
            )[-1]
        except Exception as e:
            # print(f"Error at get_previous_text_query: {e}")
            return ""

        return previous_text_query.get_attribute("textContent")

    def type_text(self, text: str) -> bool:
        # get textarea element by cdktextareaautosize attribute
        textarea = None
        p_element = None
        try:
            # with lock:
            textarea = self.driver.find_element(
                By.CSS_SELECTOR, "[cdktextareaautosize]"
            )
        except Exception as e:
            # get p element inside rich-textarea element
            # with lock:
            p_element = self.driver.find_element(By.CSS_SELECTOR, "rich-textarea p")

        # self.driver.paste_text(textarea, text)

        # send ctrl v to paste text
        # textarea.send_keys(Keys.CONTROL + "v")

        # use javascript to paste text
        if textarea:
            # get previous text and check if it is different from current text
            previous_text = self.get_previous_text_query()

            if previous_text.strip().lower() != text.strip().lower():
                # clear text first
                textarea.clear()

                self.driver.driver.execute_script(
                    "arguments[0].value = arguments[1];", textarea, text
                )

                # send space key at the end to trigger text change
                textarea.send_keys(Keys.SPACE)
            else:
                print("Previous text of textarea is equal to current text")
                return False

        elif p_element:
            # get previous text and check if it is different from current text
            previous_text = self.get_previous_text_query()

            if previous_text.strip().lower() != text.strip().lower():
                # clear text first
                p_element.clear()

                self.driver.driver.execute_script(
                    "arguments[0].textContent = arguments[1];", p_element, text
                )

                # send space key at the end to trigger text change
                p_element.send_keys(Keys.SPACE)

            else:
                print("Previous text of p_element is equal to current text")
                return False

        time.sleep(0.5)
        return True

    def click_send(self):
        # get button by mat-icon-button attribute
        try:
            # wait for element to be enabled (aria-disabled="false")

            while True:
                textarea = None
                p_element = None

                try:
                    button = self.driver.find_element(
                        By.CSS_SELECTOR,
                        ".mat-mdc-tooltip-trigger.send-button.mdc-icon-button.mat-mdc-icon-button.gmat-mdc-button-with-prefix.mat-primary.mat-mdc-button-base.gmat-mdc-button",
                    )

                    try:
                        textarea = self.driver.find_element(
                            By.CSS_SELECTOR, "[cdktextareaautosize]"
                        )
                    except Exception as e:
                        # get p element inside rich-textarea element
                        p_element = self.driver.find_element(
                            By.CSS_SELECTOR, "rich-textarea p"
                        )

                    if textarea:
                        if (
                            textarea.get_attribute("value")
                            and textarea.get_attribute("value") != ""
                            and (
                                button.accessible_name == "Enviar mensagem"
                                or button.accessible_name == "Send message"
                            )
                        ):
                            break
                    elif p_element:
                        if (
                            p_element.get_attribute("textContent")
                            and p_element.get_attribute("textContent") != ""
                            and (
                                button.accessible_name == "Enviar mensagem"
                                or button.accessible_name == "Send message"
                            )
                        ):
                            break
                except Exception as e:
                    print(f"Error at click_send inner try except: {e}")
                    continue

            # send click with javascript
            self.driver.driver.execute_script("arguments[0].click();", button)

            time.sleep(0.5)
        except Exception as e:
            print(f"Error at click_send outer try except: {e}")

    def get_model_response(self):
        # wait while last response changes
        while True:
            try:
                response_container_contents = self.driver.find_elements(
                    By.CSS_SELECTOR, ".response-container-content"
                )

                last_response = (
                    response_container_contents[-1].text
                    if response_container_contents[-1].text
                    else None
                )
                if (
                    last_response
                    and last_response != self.last_response
                    and not last_response.startswith("Oi, eu sou o Bard")
                    and not last_response.startswith("I'm Bard")
                ):
                    break
            except Exception as e:
                # print(f"Error at get_model_response: {e}")
                continue

            time.sleep(0.5)

        # select the text of the last .response-container-content element
        # with lock:
        # response_container_content = response_container_contents[-1]
        # html = response_container_content.get_attribute("innerHTML")
        # self.last_response = md(html)

        time.sleep(1)
        
        # get all json code blocks
        try:
            json_codes = self.driver.find_elements(By.CSS_SELECTOR, ".code-container")
            json_strings = [
                json_code.get_attribute("textContent") for json_code in json_codes
            ]
        except Exception as e:
            print(f"Error at get_model_response in json_codes: {e}")
            json_strings = []
        
        

        try:
            draft_panel = self.driver.find_element(
                By.CSS_SELECTOR, "draft-selection-panel"
            )
            html = draft_panel.get_attribute("innerHTML")
            self.last_response = md(html)
        except Exception as e:
            # print(f"Error at draft-selection-panel in get_model_response: {e}")
            try:
                message_content = self.driver.find_elements(
                    By.CSS_SELECTOR, "message-content"
                )[-1]
                html = message_content.get_attribute("innerHTML")
                self.last_response = md(html)
            except Exception as e:
                print(f"Error at message-content in get_model_response: {e}")

        return self.last_response, json_strings

    def get_answer(self, context: str):
        """Get QA from context."""
        while True:
            try:
                with lock:
                    typed_ok = self.type_text(context)
                    if not typed_ok:
                        return {
                            "content": None,
                        }

                    self.click_send()
            except Exception as e:
                print(f"Error at get_answer in type_text or click_send: {e}")
                return {
                    "content": None,
                    "json_strings": [],
                }

            try:
                # check if snackbar with error message is visible and wait 10 seconds
                """<div matsnackbarlabel="" class="mat-mdc-snack-bar-label mdc-snackbar__label"> Por enquanto, essa é a resposta que o Bard pode oferecer. Esse recurso é experimental. Tente novamente mais tarde.
                </div>"""
                snackbar = self.driver.find_element(
                    By.CSS_SELECTOR, ".mat-mdc-snack-bar-label.mdc-snackbar__label"
                )
                if snackbar.is_displayed():
                    time.sleep(10)
                    print(
                        f"Snackbar found | Driver {self.driver_id} waiting 10 seconds..."
                    )
                    continue
            except Exception as e:
                break

        content, json_strings = self.get_model_response()
        result = {"content": content, "json_strings": json_strings}

        random_wait = random.randint(1, 5)
        print(f"Driver {self.driver_id} waiting {random_wait} seconds...")
        time.sleep(random_wait)

        # increase responses count
        self.responses_count += 1

        # if responses count is greater than 5, refresh page
        if self.responses_count >= 5:
            print(f"Refreshing page for driver {self.driver_id}... | Responses count: {self.responses_count}")
            self.driver.get(BARD_URL)
            self.responses_count = 0

        return result

    def close(self):
        """Close selenium driver."""
        self.driver.close()


class DriverManager(Thread):
    """Process class to manage drivers in order to generate QA."""

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
        self.template = """--------- INSTRUÇÃO SISTEMA ---------
Você é um gerador de perguntas e respostas que se comunica apenas usando o FORMATO JSON. Você é treinado para gerar 3 perguntas e as respectivas respostas que ESTEJAM CONTIDAS no CONTEXTO RECEBIDO. Gere a saída APENAS no formato JSON. NÃO GERE perguntas e respostas que NÃO estejam contidas no CONTEXTO. NÃO GERE perguntas e respostas sobre valores de multas. O conteúdo das perguntas e respostas PRECISA ser ENCONTRADO NO CONTEXTO. Gere respostas DETALHADAS e PRECISAS.
A saída esperada, em JSON, deve ser no formato:
{   
  "pergunta": "{pergunta}",
  "resposta": "{resposta}"
}

Exemplo:
--------- CONTEXTO ---------
Decisao_N_06_de_03_de_marco_de_2006

MINISTÉRIO DO MEIO AMBIENTE 
CONSELHO NACIONAL DO MEIO AMBIENTE-CONAMA
DECISAO NQ 0 _6, DE 
0 3 DE .MA:RÇO DE 2006 
O CONSELHO NACIONAL DO MEIO AMBIENTE-CONAMA, no uso das 
competências que lhe são conferidas pela Lei nº 6.938, de 31 de agosto de 1981, regulamentada pelo 
Decreto nº 99.27 4, de 6 de junho de 1990, tendo em vista o disposto em seu Regimento In temo, anexo à 
Portaria nº f 68, de 1 O de junho de 2005, e 
Considerando o disposto no inciso III do art. 8º da Lei nº 6.938, de 1981, que prevê a 
competência do Conselho Nacional do Meio Ambiente-CONAMA para decidir, como última instância 
administrativa em grau de recurso, mediante depósito prévio, sobre as multas e outras penalidades 
aplicadas pe.lo Instituto .Brasileiro de Meio Ambiente e Recursos Naturais Renováveis-IBAMA, decide: 
Art. 1 º Homologar de acordo com os encaminhamentos do Comitê de Políticas 
Ambientais-CIP AM os pareceres referentes aos recursos administrativos interpostos sobre as multas 
aplicadas pelo Instituto Brasileiro de Meio Ambiente e Recursos Naturais Renováveis-IBAMA, 
analisados previamente pela Câmara Técnica de Assuntos Jurídicos-CT AJ, conforme dispõe a Resolução 
nº 338, de 25 de setembro de 2003, a saber: 
I- Processo nº 02022.001077 /02-19; 
Auto de Infração nº 308907-D; 
Interessado: C.M.N. Engenharia Ltda; 
Parecer: pelo improvimento do recurso; 
II- Processo nº 02023.001006/00-17; 
Auto de Infração nº 059320-D; 
Interessado: Petróleo Brasileiro S. A.-PETROBRÁS; 
Parecer: pelo improvimento do recurso; 
III- Processo nº 02022.010879/2002-45; 
Auto de Infração nº 326031-D; 
Interessado: Petrobrás S/A; 
Parecer: pelo improvimento do recurso; 
IV- Processo nº 02022.010872/2002-23; 
Auto de Infração nº 326033-D; 
Interessado: Petrobrás S/ A; 
Parecer: pelo improvimento do recurso; 
V -Processo nº 02022.01 0880/2002-70; 
Auto de Infração nº 326030-D; 
Interessado: Petrobrás S/ A; 
Parecer: pelo improvimento do recurso; 
VI- Processo nº 02022.010881/2002-14; 
Auto de Infração nº 326029-D; 
Interessado: Petrobrás SI A; 
Parecer: pelo improvimento do recurso; 
Art. 2º Esta decisão entra em vigor na data de sua publicação.
--------- FIM DE CONTEXTO ---------
[{   
  "pergunta": "A Petrobrás S/A já sofreu alguma multa aplicada pelo IBAMA?",
  "resposta": "De acordo com a Decisão N 06, de março de 2006, o IBAMA aplicou diversas multas à Petrobrás S/A, que tiveram seus respectivos recursos improvidos. Os motivos das multas não foram informados no documento."
},
{   
  "pergunta": "Do que se trata a Decisão N 06, de março de 2006?",
  "resposta": "A Decisão N 06, de março de 2006, trata do julgamento de recursos administrativos interpostos sobre multas aplicadas pelo IBAMA à Petrobrás S/A e outras empresas. O Conselho Nacional do Meio Ambiente (CONAMA), que é responsável por julgar, como última instância administrativa em grau de recurso, mediante depósito prévio, sobre as multas e outras penalidades aplicadas pelo IBAMA, decidiu pelo improvimento dos recursos."
},
{   
  "pergunta": "{pergunta3}",
  "resposta": "{resposta3}"
}]
--------- FIM DE INSTRUÇÃO SISTEMA ---------"""

        self.prompt = """--------- CONTEXTO ---------
{filename}

{context}
--------- FIM DE CONTEXTO ---------"""
        self.post_prompt = """A saída esperada, em JSON, deve ser no formato:
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

Lembre-se de especificar o nome do documento nas perguntas (quando necessário) e respostas.
"""
        # self.drivers: "list[SeleniumBard]" = []
        self.questions: "list[str]" = []
        self.answers: "list[str]" = []
        self.bard_driver: "SeleniumBard" = None
        # self.start_queue_monitoring = False
        self.working = True
        self.initialize_driver()

    def initialize_driver(self):
        """Initialize drivers threads."""
        data_dir = f"CHROME_USER_DATA_DIR{self.driver_id}"

        driver_options = {
            "user-data-dir": environ.get(data_dir),
            "headless": True,
        }
        driver = SeleniumHelper(driver_options)
        # driver.get(BARD_URL).wait_time(5)
        driver.last_use = datetime.now()
        driver.driver.maximize_window()

        self.bard_driver = SeleniumBard(driver, driver_id=self.driver_id)

    def format_prompt(self, context: str):
        # context is a tuple (id, filename, text)
        self.context = context[2]
        self.filename = Path(context[1]).stem

        self.formatted_prompt = f"{self.template}\n{self.prompt.format(context=self.context, filename=self.filename)}\n{self.post_prompt}"
        return self.formatted_prompt

    def load_json(self, json_string: str):
        """Loads a json string and returns a dict. There may be more than one ```json``` instruct in string."""
        # find all json blocks
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
        try:
            json_data = self.load_json(generated_text)

            for item in json_data:
                self.questions.append(item["pergunta"])
                self.answers.append(item["resposta"])

            return (
                self.context,
                self.questions,
                self.answers,
            )

        except Exception as e:
            print(f"Error at get_qa_json: {e}")
            return None, None, None

    def get_qa_regex(self, generated_text: str):
        try:
            # 'Aqui estão 3 perguntas e respostas que podem ser geradas a partir do contexto fornecido:\n\n**Pergunta 1:**\n\n\n> \n> Qual é o total de multas aplicadas pelo IBAMA à Petrobrás S/A?\n> \n> \n> \n\n**Resposta 1:**\n\n\n> \n> De acordo com a Decisão N 06, de 3 de março de 2006, o IBAMA aplicou 10 multas à Petrobrás S/A, no total de R$ 1.230.000,00.\n> \n> \n> \n\n**Pergunta 2:**\n\n\n> \n> Quais foram as infrações que geraram as multas aplicadas à Petrobrás S/A?\n> \n> \n> \n\n**Resposta 2:**\n\n\n> \n> As multas aplicadas à Petrobrás S/A foram por infrações ambientais, como lançamento de efluentes sem tratamento adequado, desmatamento ilegal e atividades de mineração sem licença.\n> \n> \n> \n\n**Pergunta 3:**\n\n\n> \n> A Petrobrás S/A recorreu das multas aplicadas pelo IBAMA?\n> \n> \n> \n\n**Resposta 3:**\n\n\n> \n> Sim, a Petrobrás S/A recorreu das multas aplicadas pelo IBAMA, mas todos os recursos foram improvidos pelo Conselho Nacional do Meio Ambiente (CONAMA).\n> \n> \n> \n\nEstas perguntas e respostas são baseadas nas informações contidas no contexto. Elas são informativas e relevantes para o tópico em questão.\n\nAqui estão algumas outras perguntas que poderiam ser geradas:\n\n* Qual é a maior multa aplicada pelo IBAMA à Petrobrás S/A?\n* Em que estado do Brasil foram aplicadas a maioria das multas à Petrobrás S/A?\n* Quais foram as consequências das multas aplicadas à Petrobrás S/A?\n\nEstas perguntas são mais abertas e provocativas. Elas podem incentivar o leitor a pensar mais sobre o assunto.\n\n'

            # find all questions and answers. The questions and answers may be in these formats:

            # "pergunta": "Qual é a capital do Brasil?", "resposta": "Brasília"
            # "Pergunta": "Qual é a capital do Brasil?", "Resposta": "Brasília"
            # **Pergunta:** Qual é a capital do Brasil? **Resposta:** Brasília
            # **pergunta:** Qual é a capital do Brasil? **resposta:** Brasília
            # **Pergunta:** Qual é a capital do Brasil?\n**Resposta:** Brasília
            # **pergunta:** Qual é a capital do Brasil?\n**resposta:** Brasília
            # pergunta: Qual é a capital do Brasil?\nresposta: Brasília
            # Pergunta: Qual é a capital do Brasil?\nResposta: Brasília

            # try this regex first, if it doesn't work, try the one above
            questions = re.findall(
                r"\*\*Pergunta \d+:\*\*(.*?)(?=\*\*Resposta \d+:\*|\Z)",
                generated_text,
                re.DOTALL,
            )
            answers = re.findall(
                r"\*\*Resposta \d+:\*\*(.*?)(?=\*\*Pergunta \d+:\*|\Z)",
                generated_text,
                re.DOTALL,
            )

            if len(questions) != len(answers):
                regex = re.compile(
                    r"(?:pergunta|Pergunta|pergunta:|Pergunta:|\*\*Pergunta:\*\*|\*\*pergunta:\*\*)\s*(.*?\n)\s*(?:resposta|Resposta|resposta:|Resposta:|\*\*Resposta:\*\*|\*\*resposta:\*\*)\s*(.*?)\s*"
                )
                matches = regex.findall(generated_text)

                # if no matches, return None
                if not matches:
                    return None, None, None

                # if matches, return questions and answers
                questions = []
                answers = []

                for match in matches:
                    questions.append(match[0])
                    answers.append(match[1])

            return (
                self.context,
                questions,
                answers,
            )

        except Exception as e:
            print(f"Error at get_qa_regex: {e}")
            return None, None, None

    def get_qa(self, generated_text: str, generated_json: "list[str]"):
        # try to get qa from json
        questions, answers = [], []
        for json_string in generated_json:
            context, json_questions, json_answers = self.get_qa_json(json_string)
            if json_questions and json_answers:
                questions.extend(json_questions)
                answers.extend(json_answers)

        # context, questions, answers = self.get_qa_json(generated_json)

        # if no qa from json, try to get qa from regex
        if (
            not questions
            or not answers
            or (len(questions) > 0 and len(questions) != len(answers))
            or len(questions) == 0
        ):
            context, questions, answers = self.get_qa_regex(generated_text)

        return context, questions, answers  # will be None if no qa was found

    def run(self):
        """Indefinetely get context from queue and generate QA."""
        # with ThreadPoolExecutor(max_workers=len(self.drivers)) as executor:
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
                    result = self.bard_driver.get_answer(formatted_context)
                    if not result["content"]:
                        continue

                    generated_text = result["content"]
                    generated_jsons = result["json_strings"]
                    qa_result = self.get_qa(generated_text, generated_jsons)

                    print(
                        f"Driver id: {self.driver_id} | Generated {len(qa_result[1]) if qa_result[1] else 0} questions and answers for file {context[1]}"
                    )
                    qa_obj = {
                        "file_index": context[0],
                        "file_name": context[1],
                        "context": qa_result[0],
                        "prompt": formatted_context,
                        "questions": qa_result[1],
                        "answers": qa_result[2],
                    }
                    self.qa_queue.put(qa_obj)

                    empty_queue_count = 0
            except Empty:
                continue
            except Exception as e:
                print(f"Error at DriverManager.run | Driver id: {self.driver_id}: {e}")
                continue

        # close selenium driver
        self.bard_driver.close()
        self.working = False


class BardQAGenerator:
    def __init__(
        self,
        model_kwargs: dict,
        contents_queue: Queue,
        driver_options: dict = {
            "user-data-dir": environ.get("CHROME_USER_DATA_DIR0"),
            # "head:"
        },
    ):
        self.bard_cookies = ["__Secure-1PSID", "__Secure-1PSIDTS"]
        self.token_name = "__Secure-1PSID"
        # self.cookies = model_kwargs["cookies"]
        self.cookies = {}
        # self.driver = SeleniumHelper(driver_options)
        self.context_queue = contents_queue
        self.qa_queue = Queue()
        self.drivers_threads: "list[DriverManager]" = []
        self.template = """--------- INSTRUÇÃO SISTEMA ---------
Você é um gerador de perguntas e respostas que se comunica apenas usando o FORMATO JSON. Você é treinado para gerar 3 perguntas e as respectivas respostas que ESTEJAM CONTIDAS no CONTEXTO RECEBIDO. Gere a saída APENAS no formato JSON. NÃO GERE perguntas e respostas que NÃO estejam contidas no CONTEXTO. NÃO GERE perguntas e respostas sobre valores de multas. O conteúdo das perguntas e respostas PRECISA ser ENCONTRADO NO CONTEXTO. Gere respostas DETALHADAS e PRECISAS.
A saída esperada, em JSON, deve ser no formato:
{   
  "pergunta": "{pergunta}",
  "resposta": "{resposta}"
}

Exemplo:
--------- CONTEXTO ---------
MINISTÉRIO DO MEIO AMBIENTE 
CONSELHO NACIONAL DO MEIO AMBIENTE-CONAMA
DECISAO NQ 0 _6, DE 
0 3 DE .MA:RÇO DE 2006 
O CONSELHO NACIONAL DO MEIO AMBIENTE-CONAMA, no uso das 
competências que lhe são conferidas pela Lei nº 6.938, de 31 de agosto de 1981, regulamentada pelo 
Decreto nº 99.27 4, de 6 de junho de 1990, tendo em vista o disposto em seu Regimento In temo, anexo à 
Portaria nº f 68, de 1 O de junho de 2005, e 
Considerando o disposto no inciso III do art. 8º da Lei nº 6.938, de 1981, que prevê a 
competência do Conselho Nacional do Meio Ambiente-CONAMA para decidir, como última instância 
administrativa em grau de recurso, mediante depósito prévio, sobre as multas e outras penalidades 
aplicadas pe.lo Instituto .Brasileiro de Meio Ambiente e Recursos Naturais Renováveis-IBAMA, decide: 
Art. 1 º Homologar de acordo com os encaminhamentos do Comitê de Políticas 
Ambientais-CIP AM os pareceres referentes aos recursos administrativos interpostos sobre as multas 
aplicadas pelo Instituto Brasileiro de Meio Ambiente e Recursos Naturais Renováveis-IBAMA, 
analisados previamente pela Câmara Técnica de Assuntos Jurídicos-CT AJ, conforme dispõe a Resolução 
nº 338, de 25 de setembro de 2003, a saber: 
I- Processo nº 02022.001077 /02-19; 
Auto de Infração nº 308907-D; 
Interessado: C.M.N. Engenharia Ltda; 
Parecer: pelo improvimento do recurso; 
II- Processo nº 02023.001006/00-17; 
Auto de Infração nº 059320-D; 
Interessado: Petróleo Brasileiro S. A.-PETROBRÁS; 
Parecer: pelo improvimento do recurso; 
III- Processo nº 02022.010879/2002-45; 
Auto de Infração nº 326031-D; 
Interessado: Petrobrás S/A; 
Parecer: pelo improvimento do recurso; 
IV- Processo nº 02022.010872/2002-23; 
Auto de Infração nº 326033-D; 
Interessado: Petrobrás S/ A; 
Parecer: pelo improvimento do recurso; 
V -Processo nº 02022.01 0880/2002-70; 
Auto de Infração nº 326030-D; 
Interessado: Petrobrás S/ A; 
Parecer: pelo improvimento do recurso; 
VI- Processo nº 02022.010881/2002-14; 
Auto de Infração nº 326029-D; 
Interessado: Petrobrás SI A; 
Parecer: pelo improvimento do recurso; 
Art. 2º Esta decisão entra em vigor na data de sua publicação.
--------- FIM DE CONTEXTO ---------
[{   
  "pergunta": "A Petrobrás S/A já sofreu alguma multa aplicada pelo IBAMA?",
  "resposta": "De acordo com a Decisão N 06, de março de 2006, o IBAMA aplicou diversas multas à Petrobrás S/A, que tiveram seus respectivos recursos improvidos. Os motivos das multas não foram informados no documento."
},
{   
  "pergunta": "Do que se trata a Decisão N 06, de março de 2006?",
  "resposta": "A Decisão N 06, de março de 2006, trata do julgamento de recursos administrativos interpostos sobre multas aplicadas pelo IBAMA à Petrobrás S/A e outras empresas. O Conselho Nacional do Meio Ambiente (CONAMA), que é responsável por julgar, como última instância administrativa em grau de recurso, mediante depósito prévio, sobre as multas e outras penalidades aplicadas pelo IBAMA, decidiu pelo improvimento dos recursos."
},
{   
  "pergunta": "{pergunta3}",
  "resposta": "{resposta3}"
}]
--------- FIM DE INSTRUÇÃO SISTEMA ---------"""

        self.prompt = """--------- CONTEXTO ---------
{context}
--------- FIM DE CONTEXTO ---------"""
        self.post_prompt = """A saída esperada, em JSON, deve ser no formato:
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
"""
        # context is the text that the model will generate questions and answers from
        self.context = ""
        self.questions = []
        self.answers = []
        self.session = Session()
        self.session.headers = {
            "Host": "bard.google.com",
            "X-Same-Domain": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "Origin": "https://bard.google.com",
            "Referer": "https://bard.google.com/",
        }
        self.initialize_drivers()
        
    def set_cookies(self, cookie_dict: dict):
        self.cookies = cookie_dict
        self.session.cookies.update(cookie_dict)
        if not hasattr(self, "bard"):
            self.bard = SeleniumBard(self.driver)
        else:
            self.bard.driver = self.driver

    def get_cookies_selenium(self):
        return self.driver.driver.get_cookies()

    def update_cookies(self):
        """Update cookies from selenium driver to bardapi"""
        for cookie in self.get_cookies_selenium():
            # if cookie["name"] in self.bard_cookies:
            self.cookies[cookie["name"]] = cookie["value"]

        # if cookie["name"] == self.token_name and not hasattr(self, "bard"):
        #     self.bard = Bard(
        #         token=cookie["value"], session=self.session, timeout=30
        #     )

        self.set_cookies(self.cookies)

    def initialize_drivers(self):
        """Initialize drivers threads."""
        data_dirs = [
            key
            for key, value in environ.items()
            if key.startswith("CHROME_USER_DATA_DIR")
        ]

        for index in range(len(data_dirs)):
            # first one only
            # for index in range(1): # temp
            driver_manager = DriverManager(
                self.context_queue, self.qa_queue, driver_id=index
            )
            driver_manager.start()
            self.drivers_threads.append(driver_manager)

        time.sleep(3)  # temp

        # change start_queue_monitoring to True in all drivers
        # for driver in self.drivers_threads:
        #    driver.start_queue_monitoring = True

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
            driver.driver.close()
