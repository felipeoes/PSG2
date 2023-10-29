import time
import fitz
import re
import json
from io import BytesIO
from tqdm import tqdm
from pathlib import Path
from googleapiclient.errors import HttpError
from g_drive_service import (
    create_service,
    find_folder_id_by_name,
    list_folders_or_files,
    read_file_content_thread_safe,
)
from threading import Thread, Lock
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_FOLDER_ID = "1vRRmyecRE71qKmHlmSbW29G1cPDj3E2t"
FOLDER_TO_SEARCH = DATA_FOLDER_ID  # ID of folder to search
# DRIVE_ID = '654321'  # ID of shared drive in which it lives
MAX_PARENTS = 500  # Limit set safely below Google max of 599 parents per query.

CHECKPOINTS_DIR = Path("checkpoints_statistics")
drive_api_ref = create_service()

lock = Lock()


def get_all_folders_in_drive():
    """
    Return a dictionary of all the folder IDs in a drive mapped to their parent folder IDs (or to the
    drive itself if a top-level folder). That is, flatten the entire folder structure.
    """
    folders_in_drive_dict = {}
    page_token = None
    max_allowed_page_size = 1000
    just_folders = "trashed = false and mimeType = 'application/vnd.google-apps.folder'"
    while True:
        results = (
            drive_api_ref.files()
            .list(
                pageSize=max_allowed_page_size,
                fields="nextPageToken, files(id, name, mimeType, parents)",
                # corpora='drive',
                # driveId=DRIVE_ID,
                pageToken=page_token,
                q=just_folders,
            )
            .execute()
        )
        folders = results.get("files", [])
        page_token = results.get("nextPageToken", None)
        for folder in folders:
            if folder.get("parents"):
                folders_in_drive_dict[folder["id"]] = folder["parents"][0]
            else:
                folders_in_drive_dict[folder["id"]] = None
            # folders_in_drive_dict[folder['id']] = folder['parents'][0]
        if page_token is None:
            break

    return folders_in_drive_dict


def get_subfolders_of_folder(folder_to_search, all_folders):
    """
    Yield subfolders of the folder-to-search, and then subsubfolders etc. Must be called by an iterator.
    :param all_folders: The dictionary returned by :meth:`get_all_folders_in-drive`.
    """
    temp_list = [
        k for k, v in all_folders.items() if v == folder_to_search
    ]  # Get all subfolders
    for sub_folder in temp_list:  # For each subfolder...
        yield sub_folder  # Return it
        yield from get_subfolders_of_folder(
            sub_folder, all_folders
        )  # Get subsubfolders etc


def get_relevant_files(relevant_folders):
    """
    Get files under the folder-to-search and all its subfolders.
    """
    relevant_files = {}
    chunked_relevant_folders_list = [
        relevant_folders[i : i + MAX_PARENTS]
        for i in range(0, len(relevant_folders), MAX_PARENTS)
    ]
    for folder_list in chunked_relevant_folders_list:
        query_term = (
            " in parents or ".join('"{0}"'.format(f) for f in folder_list)
            + " in parents"
        )
        relevant_files.update(get_all_files_in_folders(query_term))
    return relevant_files


def get_all_files_in_folders(parent_folders):
    """
    Return a dictionary of file IDs mapped to file names for the specified parent folders.
    """
    files_under_folder_dict = {}
    page_token = None
    max_allowed_page_size = 1000
    just_files = f"mimeType != 'application/vnd.google-apps.folder' and trashed = false and ({parent_folders})"
    while True:
        results = (
            drive_api_ref.files()
            .list(
                pageSize=max_allowed_page_size,
                fields="nextPageToken, files(id, name, mimeType, parents)",
                # corpora='drive',
                # driveId=DRIVE_ID,
                pageToken=page_token,
                orderBy="name",
                q=just_files,
            )
            .execute()
        )
        files = results.get("files", [])
        page_token = results.get("nextPageToken", None)
        for file in files:
            files_under_folder_dict[file["id"]] = file["name"]
        if page_token is None:
            break

        time.sleep(5)
    return files_under_folder_dict


def read_folder_files_content(
    service,
    folder_id: str = None,
    folder_name: str = None,
    parent_folder_id: str = None,
    last_file_name: str = None,
    last_file_index: str = None,
    pdf_only: bool = True,
):
    """Read all files content in a folder
    Returns : List of file content

    """
    try:
        if folder_id is None and folder_name is None:
            return None

        if folder_id is None:
            # folder_name can be of type `folder1/folder2/folder3`. So, we need  to find the folder id of the last folder
            folder_name_list = folder_name.split("/")
            folder_id = parent_folder_id
            for folder_name in folder_name_list:
                folder_id, _, _ = find_folder_id_by_name(
                    service, folder_name, folder_id
                )

        files = list_folders_or_files(service, folder_id, pdf_only=pdf_only)
        total_files = len(files)
        if files is None:
            return None

        # use tqdm to show progress. Make concurrent requests to read file content
        with tqdm(total=total_files) as pbar:
            with ThreadPoolExecutor(max_workers=cpu_count() // 2) as executor:
                futures = []
                for index, file in enumerate(
                    files
                ):  # since we are ordering by name, we can use index to save QAhelper progress
                    # check if file is already processed
                    if last_file_index is not None and index < last_file_index:
                        pbar.update(1)
                        continue

                    # tuple with file_index, file_name and result
                    futures.append(
                        (
                            index,
                            file.get("name"),
                            executor.submit(
                                read_file_content_thread_safe, file.get("id")
                            ),
                        )
                    )

                # do not use as_completed as it will return the result in order of completion
                for future in futures:
                    # save checkpoint
                    file_index, file_name, result = future

                    # save checkpoint
                    # qa_helper.checkpoint_saver.run(file_index, file_name)

                    pbar.update(1)
                    yield file_index, file_name, result.result()

    except HttpError as error:
        print(f"An error occurred: {error}")

    return None


class DocStatisticsCalculator:
    """Calculates statistics for the given documents. Statistics are:
    - number of pages
    - number of words
    - number of characters
    """

    def __init__(
        self,
        text: str,
        n_pages: int,
        folder_name: str,
        filename: str,
        file_index: int,
        save_path: Path,
        statistics: dict,
    ):
        self.file_index = file_index
        self.folder_name = folder_name
        self.filename = filename
        self.n_pages = n_pages
        self.text = text
        self.save_path = save_path
        self.statistics = statistics

    @staticmethod
    def check_text(text: str, regex: re.Pattern):
        """Checks if text contains any of the filtering keywords"""
        return regex.search(text)

    def save_statistics(self):
        """Saves the statistics to a file"""
        with lock:
            if not self.save_path.parent.exists():
                self.save_path.parent.mkdir(parents=True, exist_ok=True)

            with open(self.save_path, "w") as f:
                json.dump(self.statistics, f, indent=4)

    def count_words(self):
        """Count the number of words in a text."""
        return len([word for word in self.text.split() if word.isalpha()])

    def calculate_statistics(self):
        """Calculates the statistics for the given text"""
        with lock:
            self.statistics[self.filename] = {
                "file_index": self.file_index,
                "n_pages": self.n_pages,
                "n_words": self.count_words(),
                "n_characters": len(self.text),
            }

        # save statistics in background
        Thread(target=self.save_statistics, daemon=True).start()


def extract_text(file_path: Path = None, file_content: BytesIO = None):
    if file_path:
        doc = fitz.open(file_path)
    elif file_content:
        doc = fitz.open(stream=file_content, filetype="pdf")
    else:
        raise Exception("You must provide either a file path or a file content.")

    str_text = ""
    n_pages = 0
    for page in doc:
        str_text += page.get_text()
        n_pages += 1

    return str_text, n_pages
    # return "".join([page.get_text() for page in doc])


def load_statistics(save_path: Path):
    """Loads the statistics from a file"""
    if not save_path.exists():
        return {}

    with open(save_path, "r") as f:
        statistics = json.load(f)
    return statistics


if __name__ == "__main__":
    # all_folders_dict = get_all_folders_in_drive()  # Flatten folder structure
    # relevant_folders_list = [FOLDER_TO_SEARCH]  # Start with the folder-to-archive
    # for folder in get_subfolders_of_folder(FOLDER_TO_SEARCH, all_folders_dict):
    #     relevant_folders_list.append(folder)  # Recursively search for subfolders

    # relevant_folders_list = [  # since not webscraping other websites for now, we can hardcode the relevant folders to avoid spend time searching for them
    #     "1vRRmyecRE71qKmHlmSbW29G1cPDj3E2t",
    #     "1rHoEztEu-p0NOntNBo9x6lx6P89Rc25m",
    #     "11OzFmoDHwgbyi6vuaO_onBkG6lbC4SZO",
    #     "1YpYbsR0Xyfs6o1a3JwfQA7vdr-B9cOkJ",
    #     "1OgLR3ljmJ1GxIGgQ8iOt1mN6tIqVf_hZ",
    #     "1e-ZDoca-jEBS8kZv1EscLITZg6N98Bnw",
    #     "1asUEWj9Vz52EVLt0sgrqt6iy-P9JysV8",
    #     "1D2po1QKo3Qa51dgCHlO8B0LnkuZbd79m",
    #     "1epDDvaDISJPh3bB3mnLyocj3YmGnGmya",
    #     "1O8zr2S_w4dv6DhZQph8k1ACSmk_85b4V",
    #     "1eH8NGfgMtnURE92fh5jOiIj2XqLkUJJS",
    #     "1smoVuK9drzQ3IX5k5GbKU1E_2Sc8-j9L",
    #     "1uDvJTQpcHTe5A3ie0yMBfTETl5Blqe30",
    #     "1gXLNf9nyP9DELB0ODKa3lBFZgLkcmK8Q",
    #     "1-08W6X7EOntZyUDCm9B11l7DhxPIiu6e",
    #     "1SQzWMqMynCriLLwFCvEODb0iFlVv7WYq",
    #     "1zdp10wUA0dHxQ0_GGvxhXbAGSNUsbVT3",
    #     "196FTWZn5jV3gC6No0Ng8Ca1T8tSWIGd-",
    #     "1z1BtsfCrluV_MT7nn0J4EOk7JXjrluLN",
    #     "186Vz8owLhz2NU2VVNLP20JlAMqyeX9Q_",
    #     "1SVeBjXZaUWi6iALzMvkX0vCkZ0klxvHW",
    #     "1ylZYXTbOh9KijSJNfnOvLhqeqRvfQVYT",
    #     "1v78Aisl18GGpO9MtwROuB3ldU9FqAb58",
    #     "15_szmqE_KU6Gs8F_fcmtLXIbIhBI02VR",
    #     "19GobdsbZ7TdLXD1YIFOBY-zWb58PLaNo",
    #     "13KDdILwofLoSC1QpX4AJ1tg-cTgBMIky",
    #     "1Lkj8X8pZvBg0MtDtDFLdAoBF2X3N6xdI",
    #     "1FSi8VLxyE-0VANlGUujVLhEB2NYEJs-K",
    #     "1NzAZIfB33QK-RlJrt3yzOa3XICXk-SpH",
    #     "1hXvlNWLgwsAv2m8lMuUxEW0ASUKUTxG5",
    # ]

    relevant_folders_list = [
        # "conama",
        # "legislacao_federal/medidas_provisorias",
        # "legislacao_federal/constituicao_federal",
        # "documentos_diversos",
        # "icmbio/instrucoes_normativas",
        # "icmbio/portarias", # commented because already processed 
        # "legislacao_federal/decretos",
        "legislacao_federal/leis",
    ]

    filtering_keywords: list = [
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
        "águas jurisdicionais", # BELOW ADDED FOR QA FILTER ONLY
        "navio",
        "barco",
        "embarcação",
        r"portuári\w+",
        "porto",
        "canoa",
        "balsa"
    ]

    regex = re.compile(
        r"([^.]*?{}[^.]*\.)".format("|".join(filtering_keywords)),
        re.IGNORECASE,
    )

    for folder in relevant_folders_list:
        full_statistics_path = CHECKPOINTS_DIR / folder / "full_statistics.json"
        filtered_statistics_path = CHECKPOINTS_DIR / folder / "filtered_statistics.json"

        full_statistics = load_statistics(full_statistics_path)
        filtered_statistics = load_statistics(filtered_statistics_path)

        print(f"Full statistics: {len(full_statistics)} records")
        print(f"Filtered statistics: {len(filtered_statistics)} records")

        if len(full_statistics) == 0 or len(filtered_statistics) == 0:
            last_file_index = 0
        else:
            last_file_index_full = max(
                [
                    full_statistics[filename]["file_index"]
                    for filename in full_statistics
                ]
            )
            last_file_index_filtered = max(
                [
                    filtered_statistics[filename]["file_index"]
                    for filename in filtered_statistics
                ]
            )

            last_file_index = max(last_file_index_full, last_file_index_filtered)

        print(f"Resume from last file index: {last_file_index} | Folder: {folder}")

        contents = read_folder_files_content(
            drive_api_ref,
            folder_name=folder,
            last_file_index=last_file_index,
            parent_folder_id=DATA_FOLDER_ID,
        )

        for file_index, file_name, file_content in contents:
            # check already processed files
            if file_name in full_statistics or file_name in filtered_statistics:
                continue

            try:
                text, n_pages = extract_text(file_content=file_content)

                if text is None:
                    continue

                doc_statistics_calculator = DocStatisticsCalculator(
                    text=text,
                    n_pages=n_pages,
                    folder_name=folder,
                    filename=file_name,
                    file_index=file_index,
                    save_path=full_statistics_path,
                    statistics=full_statistics,
                )
                doc_statistics_calculator.calculate_statistics()

                if DocStatisticsCalculator.check_text(text, regex): # also calculate statistics for filtered documents
                    doc_statistics_calculator = DocStatisticsCalculator(
                        text=text,
                        n_pages=n_pages,
                        folder_name=folder,
                        filename=file_name,
                        file_index=file_index,
                        save_path=filtered_statistics_path,
                        statistics=filtered_statistics,
                    )
                    doc_statistics_calculator.calculate_statistics()

            except Exception as e:
                print(f"Error ocurred in main loop: {e}")
                continue

            except KeyboardInterrupt:
                print(
                    f"KeyboardInterrupt | file_name: {file_name} | File index: {file_index}"
                )
                break


import numpy as np
import pandas as pd
from pathlib import Path
import os
import glob

full_datasets_path = Path('/content/drive/MyDrive/SI - USP/SI - 8 SEM/PSG2_TCC/DATA/DATASETS/BARD/full')
filepaths = [path for path in full_datasets_path.glob('*.csv')]
df = pd.concat(map(pd.read_csv, filepaths))
df

# keep rows that passes the regex filter in 'question' or 'answer' columns
def check_text(text: str, regex: re.Pattern):
        """Checks if text contains any of the filtering keywords"""
        return regex.search(text) is not None

regex = re.compile(
        r"([^.]*?{}[^.]*\.)".format("|".join(filtering_keywords)),
        re.IGNORECASE,
    )

# removed nas
df = df.dropna(subset=['question', 'answer'])
df_filtered =  df[(df['question'].apply(lambda x: check_text(x, regex))) | (df['answer'].apply(lambda x: check_text(x, regex)))]