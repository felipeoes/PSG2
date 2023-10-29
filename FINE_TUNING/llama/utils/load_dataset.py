import re
from tqdm import tqdm
from pathlib import Path
from datasets import Dataset
import pandas as pd

RAW_TEXT_DIR = Path(r"C:\Users\Felipe O E Santo\Downloads\RAW_TEXT")
DATASETS_DIR = Path("datasets")
DATASET_NAME = "filtered_raw_text_dataset"

filtering_keywords = [
    r"ocean\w+",
    # r"mar\w+", # removed because of noise (março, marcelo, marina, etc)
    r"marítim\w+",
    r"marinh\w+",
    r"costeir\w+",
    r"praia\w+",
    r"ilh\w+",
    r"pesc\w+",
    r"pesqueir\w+",
    r"estuar\w+",
    r"aquat\w+",
    r"aquát\w+",
    r"litor\w+",  # litoral, litorâneo, etc
    r"petrolífer\w+",
    r"petróle\w+",
    "baía",
    "arquipélago",
    "mar",
    "costa",
    "margem continental",
    "economia azul",
    "amazônia azul",
    "zona econômica exclusiva",
    "zee",
    "pré-sal",
    "plataforma continental",
    "águas jurisdicionais",
]

regex = re.compile(r"\b(?:%s)\b" % "|".join(filtering_keywords), re.IGNORECASE)


def check_text(text: str):
    """Checks if text contains any of the filtering keywords"""
    return regex.search(text)


def get_or_create_dataset(name: str = DATASET_NAME):
    """Loads dataset if it exists, otherwise creates it"""
    dataset_path = DATASETS_DIR / name
    if dataset_path.exists():
        return Dataset.load_from_disk(dataset_path)

    # dataset does not exist, create it
    dataframe = pd.DataFrame(columns=["text"])

    for file in tqdm(RAW_TEXT_DIR.glob("*.txt")):
        with open(file, "r", encoding="utf-8") as f:
            text = f.read()
            if check_text(text):
                dataframe = pd.concat([dataframe, pd.DataFrame({"text": [text]})])

    print(f"Number of documents: {len(dataframe)}")

    dataset = Dataset.from_pandas(dataframe)
    dataset.save_to_disk(DATASETS_DIR / DATASET_NAME)

    return dataset


# if __name__ == "__main__":
#     get_or_create_dataset(DATASET_NAME)
