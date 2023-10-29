from google.colab import drive
drive.mount('/content/drive')


import torch
torch.cuda.is_available()

import fitz
import re 
import gc

from pathlib import Path
from tqdm import tqdm
import pandas as pd
from transformers import pipeline
from datasets import Dataset

folder_path = '/content/drive/MyDrive/SI - USP/SI - 8 SEM/PSG2_TCC/DATA/conama'
datasets_path = Path('/content/drive/MyDrive/SI - USP/SI - 8 SEM/PSG2_TCC/DATA/datasets')


filtering_keywords = ['oceano', 'mar', 'oceânico', 'oceânica', 'marinha', 'marinho', 'marítima', 'marítimo', 'oceanografia', 'costeira', 'costeiro', 'costa', 'petrolífero', 
                       'pesca', 'aquático', 'amazônia azul', 'zona econômica exclusiva', 'zee', 'plataforma continental', 'águas jurisdicionais']
 
regex = re.compile(r'\b(?:%s)\b' % '|'.join(filtering_keywords), re.IGNORECASE)

# create datasets folder if not exists
datasets_path.mkdir(parents=True, exist_ok=True)

files = [file for file in Path(folder_path).rglob('*')]

# generate question
question_model_name = "rsgrava/ptt5-base-e2e-qg"
question_pipe = pipeline("text2text-generation", model=question_model_name, device=0)

# generate answer
answer_model_name = 'pierreguillou/bert-base-cased-squad-v1.1-portuguese'
answer_pipe = pipeline("question-answering", model=answer_model_name, device=0)

TEXT_LENGTH = 200

def extract_text(file: Path):
    doc = fitz.open(file)
    return ''.join([page.get_text() for page in doc])

def get_all_text_parts(text: str, text_length=TEXT_LENGTH):
    return [text[i:i+text_length] for i in range(0, len(text), text_length)]

def generate_question(text: str):
    question = question_pipe(text)[0]['generated_text'].split('<sep>')[0]
    return question

def check_text(text: str, regex: re.Pattern):
    """ Checks if text contains any of the filtering keywords """
    return regex.search(text)

# def texts_generator(files: list):
#     for file in files:
#         text = extract_text(file)
#         parts = get_all_text_parts(text.strip(), TEXT_LENGTH)
#         for part in parts:
#             yield {"context": part}

# def create_dataset(files: list):
#     return Dataset.from_generator(
#         texts_generator,
#         gen_kwargs={"files": files}
#     )
    
# dataset = create_dataset(files)

  
""" First generate 1 question per document, later will split document in chunks to generate more questions, if needed """

# build dataframe with context, question and answer.
# build dataframe with context, question and answer.
rows = []
for file in tqdm(files):
  try:
    text = extract_text(file)
    
    # check if text contains any of the filtering keywords
    if not check_text(text, regex):
        continue
     
    print(len(text))
    # check if text is not empty or too small
    if len(text) < 50:
        continue
    
    # check if text is too big and split it
    if len(text) > 10000: 
        parts = get_all_text_parts(text.strip(), TEXT_LENGTH)
        for part in parts:
            question = generate_question(part)
            answer = answer_pipe(question=question, context=part)['answer']
            rows.append([part, question, answer])

            # free memory in GPU
            gc.collect()
            torch.cuda.empty_cache()
        continue
    
    question = generate_question(text) 
    answer = answer_pipe(question=question, context=text)['answer']
    rows.append([text, question, answer])
  except Exception as e:
    print(e)

       
     
    
df = pd.DataFrame(rows, columns=['context', 'question', 'answer'])

#save csv to folder path too
df.to_csv(datasets_path/'conama.csv', index=False)

df.to_csv('conama.csv', index=False)
      
# build dataframe with context, question and answer
rows = []
for file in tqdm(files):
    text = extract_text(file)
    parts = get_all_text_parts(text.strip(), TEXT_LENGTH)
    
    for part in parts:
        try:
            question = generate_question(part)
            answer = answer_pipe(question=question, context=part)['answer']
            rows.append([part, question, answer])
        except:
            pass
            
df = pd.DataFrame(rows, columns=['context', 'question', 'answer'])
 
    
df.to_csv('conama.csv', index=False)









 