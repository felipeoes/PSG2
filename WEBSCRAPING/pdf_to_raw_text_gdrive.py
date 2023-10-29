import os
import time
import fitz
from pathlib import Path
from unidecode import unidecode
from io import BytesIO
from g_drive_service import (
    create_service,
    upload_file_string,
    read_folder_files_content,
    find_folder_id_by_name,
    create_folder_or_get_id,
    update_file_string,
)
from threading import Thread
from multiprocessing import Queue, cpu_count
from concurrent.futures import ThreadPoolExecutor


DATA_FOLDER_ID = "1vRRmyecRE71qKmHlmSbW29G1cPDj3E2t"  # where documents are stored

folder = Path("legislacao_federal/leis")
output_folder = folder / "RAW_TEXT"


class FilesUploader(Thread):
    """Background class to upload downloaded files to Google Drive, based on queue"""

    def __init__(self):
        Thread.__init__(self, daemon=True)
        self.queue = Queue()

    def add_file_to_queue(self, file_info: dict):
        self.queue.put(file_info)

    def upload_file(
        self,
        file_path: Path,
        file_content: str,
        folder_id: str,
        **kwargs,
    ):
        """Upload file to Google Drive. Thread safe by creating service on every call"""
        self.service = create_service()

        try:
            # check if file already exists
            file_id, _, _ = find_folder_id_by_name(self.service, file_path.name, folder_id)  # type: ignore

            if file_id is not None:
                # update file
                update_file_string(
                    self.service, file_id, file_path, file_content, **kwargs
                )

                return True

            # upload file
            upload_file_string(
                self.service, str(file_path), folder_id, file_content, **kwargs
            )

        except Exception as e:
            print(f"Error while uploading file {file_path}: {e}")
            raise e

        return True

    def batch_upload_files(self, files_info: list):
        """Upload files in batch"""
        try:
            # upload files
            with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
                for file_info in files_info:
                    executor.submit(self.upload_file, **file_info)

            print(f"Submitted {len(files_info)} files to upload")
        except Exception as e:
            print(f"Error while uploading files: {e}")
            raise e

        return True

    def run(self):
        """Upload files from queue"""
        max_wait_time = 60  # 1 min
        wait_time = 0
        while True:
            # try batch upload
            try:
                files_info = []
                max_batch_size = 50
                while not self.queue.empty() and len(files_info) < max_batch_size:
                    file_info = self.queue.get()
                    files_info.append(file_info)

                if len(files_info) > 0:
                    self.batch_upload_files(files_info)

                if self.queue.empty():
                    print("Queue is empty. Exiting...")
                    time.sleep(1)
                    wait_time += 1

                    if wait_time > max_wait_time:
                        print("Max wait time reached. Upload should be finished.")
                        break
            except Exception as e:
                print(f"Error while uploading files: {e}")
                time.sleep(0.1)
                continue


def extract_text(file_path: Path = None, file_content: BytesIO = None):
    if file_path:
        doc = fitz.open(file_path)
    elif file_content:
        doc = fitz.open(stream=file_content, filetype="pdf")
    else:
        raise Exception("You must provide either a file path or a file content.")

    return "".join([page.get_text() for page in doc])


def pdf_to_raw_text(
    contents: "list[str]", files_uploader: FilesUploader, output_folder_id: str
):
    for file_index, file_name, file_content in contents:
        # start files_uploader only after first file is finished downloading
        if not files_uploader.is_alive():
            files_uploader.start()

        # check if is file and pdf
        if not file_name.endswith(".pdf"):
            continue
        try:
            raw_text = extract_text(file_content=file_content)
            raw_text_file_path = Path(output_folder) / (
                file_name.replace(".pdf", ".txt")
            )

            # upload to gdrive and remove from local
            files_uploader.add_file_to_queue(
                {
                    "file_path": raw_text_file_path,
                    "file_content": raw_text,
                    "folder_id": output_folder_id,
                    "mimetype": "text/plain",
                }
            )

        except Exception as e:
            print(f"Error while processing file {file_name}: {e}")
            continue


if __name__ == "__main__":
    try:
        service = create_service()
        files_uploader = FilesUploader()

        # if folder is composed of subfolders, get files from subfolders. Change parent folder id to subfolder id
        folder_id = DATA_FOLDER_ID
        if "/" in folder.as_posix():
            parent_name = folder.parent.name
            folder_id, _, _ = find_folder_id_by_name(service, parent_name, folder_id)  # type: ignore

        # get files from gdrive
        contents = read_folder_files_content(
            service,
            folder_name=folder.name,
            parent_folder_id=folder_id,
        )

        # create output folder in gdrive if not exists
        folder_id = create_folder_or_get_id(service, folder.name, folder_id)
        output_folder_id = create_folder_or_get_id(
            service, output_folder.name, folder_id
        )

        pdf_to_raw_text(contents, files_uploader, output_folder_id)

        # wait for threads to finish
        files_uploader.join()
    except KeyboardInterrupt:
        print("Interrupted by user")
        os._exit(0)
