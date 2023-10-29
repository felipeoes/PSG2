import fitz
import os
from pathlib import Path
from io import BytesIO
from google.colab import drive
from tqdm import tqdm

drive.mount("/content/drive")


def extract_text(file_path: Path = None, file_content: BytesIO = None):
    if file_path:
        doc = fitz.open(file_path)
    elif file_content:
        doc = fitz.open(stream=file_content, filetype="pdf")
    else:
        raise Exception("You must provide either a file path or a file content.")

    return "".join([page.get_text() for page in doc])


def save_raw_text(file_path: Path, raw_text: str):
    # check if file already exists
    if file_path.exists():
        return

    with open(file_path, "w") as f:
        f.write(raw_text)


def pdf_to_raw_text(input_folder: Path, output_folder: Path):
    max_leis = 28913
    max_decretos = 165000
    max_files = 0

    if input_folder.name == "leis":
        max_files = max_leis
    elif input_folder.name == "decretos":
        max_files = max_decretos
    else:
        max_files = len(os.listdir(input_folder))

    for file in tqdm(input_folder.iterdir(), total=max_files):
        # check if is file and pdf
        if not file.is_file() or not file.name.endswith(".pdf"):
            continue
        try:
            raw_text = extract_text(file)
            save_raw_text(output_folder / file.name.replace(".pdf", ".txt"), raw_text)
        except Exception as e:
            print(f"Error while processing file {file}: {e}")
            continue


folder = Path("/content/drive/MyDrive/SI - USP/SI - 8 SEM/PSG2_TCC/DATA/conama")
output_folder = Path(
    "/content/drive/MyDrive/SI - USP/SI - 8 SEM/PSG2_TCC/DATA/conama/RAW_TEXT"
)


pdf_to_raw_text(folder, output_folder)
