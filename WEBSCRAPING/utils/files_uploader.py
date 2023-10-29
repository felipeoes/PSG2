import time
from multiprocessing import Queue
from threading import Thread
from unidecode import unidecode
from pathlib import Path
from utils.files_remover import FilesRemover
from g_drive_service import *

PARENT_FOLDER_ID = "1vRRmyecRE71qKmHlmSbW29G1cPDj3E2t"


class FilesUploader:
    """Background class to upload downloaded files to Google Drive, based on queue"""

    def __init__(self, files_remover: FilesRemover):
        # Thread.__init__(self, daemon=True)
        # self.queue: Queue[dict] = Queue()
        self.files_remover = files_remover

    # def add_file_to_queue(self, file_info: dict):
    #     self.queue.put(file_info)

    def upload_file(
        self, file_path: Path, folder_name: str, parent_folder_name: str = "", **kwargs
    ):
        """Upload file to Google Drive. Thread safe by creating service on every call"""
        self.service = create_service()

        # try find parent folder id
        if parent_folder_name != PARENT_FOLDER_ID:
            parent_folder_id, _, _ = find_folder_id_by_name(
                self.service, parent_folder_name
            )  # type: ignore

            if parent_folder_id is None:
                # create inside `folder_name` folder
                folder_name_id, _, _ = find_folder_id_by_name(
                    self.service, folder_name, parent_folder_id=PARENT_FOLDER_ID
                )  # type: ignore

                parent_folder_id = create_folder_or_get_id(
                    self.service, parent_folder_name, folder_name_id
                )

            folder_id = parent_folder_id
        else:
            folder_name = (
                unidecode(folder_name).lower().replace(" ", "_").replace("\\", "")
            )
            folder_id, _, _ = find_folder_id_by_name(
                self.service, folder_name, parent_folder_id=PARENT_FOLDER_ID
            )  # type: ignore

            if folder_id is None:
                folder_id = create_folder_or_get_id(
                    self.service, folder_name, PARENT_FOLDER_ID
                )

        try:
            # check if file already exists
            file_id, _, _ = find_folder_id_by_name(self.service, file_path.name, folder_id)  # type: ignore

            if file_id is not None:
                # update file and add it file remover's queue remove file from local
                update_file(self.service, file_id, str(file_path), **kwargs)
                self.files_remover.add_file_to_queue(file_path)

                return True

            # upload file and add it to file remover's queue remove file from local
            upload_file(self.service, str(file_path), folder_id, **kwargs)
            self.files_remover.add_file_to_queue(file_path)

        except Exception as e:
            print(f"Error while uploading file {file_path}: {e}")
            raise e

        return True
