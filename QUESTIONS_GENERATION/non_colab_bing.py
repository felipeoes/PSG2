from os import environ
from dotenv import load_dotenv
from long_qa_helper_bing import QAHelper

from pathlib import Path

load_dotenv()

if __name__ == "__main__":
    folder_path = Path("bing/long_answers")
    csv_path = "v2_filtered_qa_blue_amazon_legislation_19k.csv"

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
        "águas jurisdicionais",  # BELOW ADDED FOR QA FILTER ONLY
        "navio",
        "barco",
        "embarcação",
        r"portuári\w+",
        "porto",
        "canoa",
        "balsa",
    ]

    qa_helper = QAHelper(
        g_drive_folder_name=folder_path,
        csv_input_path=csv_path,
        filtering_keywords=filtering_keywords,
        num_drivers=10, # also num_thread that will be spawned
    )

    df, csv_path = qa_helper.run()
    print(df)

    print(f"CSV saved at {csv_path}")
