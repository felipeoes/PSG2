from os import environ
from dotenv import load_dotenv
from qa_helper_bard_full import QAHelper
from g_drive_service import create_service

from pathlib import Path

load_dotenv()

if __name__ == "__main__":
    folder_path = Path(r"C:\Users\Felipe O E Santo\Downloads\RAW_TEXT_FULL") # this is a local folder that contains all the raw files for icmbio, conama and legislacao_federal

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

    cookie_dict = {
        "__Secure-1PSID": environ.get("COOKIE__Secure-1PSID"),
        "__Secure-1PSIDTS": environ.get("COOKIE__Secure-1PSIDTS"),
        "__Secure-1PSIDCC": environ.get("COOKIE__Secure-1PSIDCC"),
    }

    qa_helper = QAHelper(
        g_drive_folder_name=folder_path,
        filtering_keywords=filtering_keywords,
        service=create_service(),
        max_chunk_length=4000,
        chunk_overlap=1000,
        model_kwargs={"cookies": cookie_dict},
    )

    df, csv_path = qa_helper.run()
    print(df)

    print(f"CSV saved at {csv_path}")

    # upload to drive
    qa_helper.upload_dataframe(csv_path)
