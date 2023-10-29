from __future__ import print_function
import os
import tqdm
import io
import time
import httplib2

from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient import _auth
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor

SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]

creds = None


def get_folder_name_from_id(service, folder_id):
    """Get folder name from id
    Returns : Folder name

    """
    try:
        # pylint: disable=maybe-no-member
        file = service.files().get(fileId=folder_id, fields="name").execute()
        return file.get("name")

    except HttpError as error:
        print(f"An error occurred: {error}")
        file = None


def find_folder_id_by_name(service, folder_name, parent_folder_id=None):
    """Find folder by name
    Returns : Id of the folder found

    """

    try:
        query = f"name = '{folder_name}'"

        # pylint: disable=maybe-no-member
        response = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, size, parents)",
                pageToken=None,
            )
            .execute()
        )

        files = response.get("files", [])
        if not files:
            return None, None, None
        else:
            if parent_folder_id is None:
                return files[0]["id"], files[0]["parents"], files[0].get("size")
            else:
                for file in files:
                    if parent_folder_id in file["parents"]:
                        return file["id"], file["parents"], file.get("size")

        return None, None, None

    except HttpError as error:
        print(f"An error occurred: {error}")

    return None


def create_service():
    global creds
    """Create a Drive v3 service object.
    Returns : Drive v3 service object.
    """
    # creds = Credentials.from_authorized_user_file("gdrive_token.json", SCOPES)
    # creds = Credentials.from_authorized_user_file("token_gdrive.json", SCOPES)

    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.

    if os.path.exists("gdrive_token.json"):
        creds = Credentials.from_authorized_user_file("gdrive_token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("gdrive_token.json", "w") as token:
            token.write(creds.to_json())
    return build("drive", "v3", credentials=creds)


def create_folder_or_get_id(service, folder_name, move_to_folder_id):
    """Create a folder and prints the folder ID
    Returns : Folder Id

    Load pre-authorized user credentials from the environment.
    """

    try:
        # cHeck if folder exists
        folder_id, _, _ = find_folder_id_by_name(
            service, folder_name, move_to_folder_id
        )
        if folder_id is not None:
            return folder_id

        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        }

        # pylint: disable=maybe-no-member
        file = service.files().create(body=file_metadata, fields="id").execute()
        print(
            f'Folder has created with name: "{folder_name}" and ID: "{file.get("id")}".'
        )

        # Move to pi-files
        file_id = file.get("id")
        file = service.files().get(fileId=file_id, fields="parents").execute()
        previous_parents = ",".join(file.get("parents"))

        file = (
            service.files()
            .update(
                fileId=file_id,
                addParents=move_to_folder_id,
                removeParents=previous_parents,
                fields="id, parents",
            )
            .execute()
        )
        return file.get("id")

    except HttpError as error:
        print(f"An error occurred: {error}")
        file = None

    return file.get("id")


def list_folders_or_files(service, parent_folder_id, pdf_only: bool = True):
    """List all folders or files in a folder
    Returns : List of folders or files
    """
    try:
        files = []
        page_token = None
        
        q = (
            f"'{parent_folder_id}' in parents and trashed=false and mimeType='application/pdf'"
            if pdf_only
            else f"'{parent_folder_id}' in parents and trashed=false"
        )
        while True:
            try:
                # pylint: disable=maybe-no-member
                response = (
                    service.files()
                    .list(
                        q=q,
                        spaces="drive",
                        fields="nextPageToken, files(id, name, size, parents)",
                        orderBy="folder, name",
                        pageToken=page_token,
                    )
                    .execute()
                )
                files.extend(response.get("files", []))

                page_token = response.get("nextPageToken", None)
                if page_token is None:
                    break
                
                if len(files) > 0 and len(files) % 1000 == 0:
                    print(f"Total files until now: {len(files)}")
                
            except HttpError as error:
                print(f"An error occurred on list_folders_or_files: {error}")
                time.sleep(5)
                continue

        return files
    except Exception as error:
        print(f"An error occurred: {error}")

    return None


def generate_file_download_link(service, file_id):
    """Generate a file download link
    Returns : File download link

    """

    try:
        # pylint: disable=maybe-no-member
        file = service.files().get(fileId=file_id, fields="webContentLink").execute()
        return file.get("webContentLink")

    except HttpError as error:
        print(f"An error occurred: {error}")
        file = None

    return file.get("webContentLink")


def read_gdrive_txt_file_by_chunks(service, file_id, chunk_size=1024 * 1024 * 1024):
    """Read a file from Google Drive by chunks
    Returns : File content

    """

    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=chunk_size)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%.")
    fh.seek(0)
    return fh.read().decode("utf-8")


def download_file(service, file_id, size, destination_path):
    """Download file from google drive
    Returns : None

    """

    try:
        request = service.files().get_media(fileId=file_id)

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        # use tqdm to show progress
        with tqdm.tqdm(total=float(size), unit="B", unit_scale=True) as pbar:
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                pbar.update(len(fh.getvalue()))

        # write to file
        fh.seek(0)

        with open(destination_path, "wb") as f:
            f.write(fh.getvalue())

        return True

    except HttpError as error:
        print(f"An error occurred: {error}")
        file = None


# def upload_file_thread_safe(file_path: str, parent_folder_id: str, mimetype: str = "text/plain"):
#     global creds

#     file_metadata = {"name": os.path.basename(file_path), "parents": [parent_folder_id]}
#     media = MediaFileUpload(file_path, mimetype=mimetype)

#     service = create_service() # need to create service on every thread

#     # pylint: disable=maybe-no-member
#     file = (
#         service.files()
#         .create(body=file_metadata, media_body=media, fields="id")
#         .execute()  # type: ignore
#     )

#     print(f'File has uploaded with name: "{file_metadata["name"]}" and ID: "{file.get("id")}".')


def update_file_string(
    service, file_id, file_path: str, file_content: str, mimetype: str = "text/plain"
):
    """Update file in google drive
    Returns : None

    """
    file_metadata = {"name": os.path.basename(file_path)}
    media = MediaIoBaseUpload(
        io.BytesIO(file_content.encode("utf-8")), mimetype=mimetype
    )

    # pylint: disable=maybe-no-member
    https = _auth.authorized_http(creds)
    file = (
        service.files()
        .update(fileId=file_id, body=file_metadata, media_body=media, fields="id")
        .execute(http=https)  # type: ignore
    )

    print(
        f'File has updated with name: "{file_metadata["name"]}" and ID: "{file.get("id")}".'
    )


def update_file(service, file_id, file_path: str, mimetype: str = "text/plain"):
    """Update file in google drive
    Returns : None

    """
    file_metadata = {"name": os.path.basename(file_path)}
    media = MediaFileUpload(file_path, mimetype=mimetype)

    # pylint: disable=maybe-no-member
    file = (
        service.files()
        .update(fileId=file_id, body=file_metadata, media_body=media, fields="id")
        .execute()  # type: ignore
    )

    print(
        f'File has updated with name: "{file_metadata["name"]}" and ID: "{file.get("id")}".'
    )


def upload_file_string(
    service,
    file_path: str,
    parent_folder_id: str,
    file_content: str,
    mimetype: str = "text/plain",
):
    """Upload file to google drive
    Returns : None

    """
    file_metadata = {"name": os.path.basename(file_path), "parents": [parent_folder_id]}
    media = MediaIoBaseUpload(
        io.BytesIO(file_content.encode("utf-8")), mimetype=mimetype
    )

    # pylint: disable=maybe-no-member
    https = _auth.authorized_http(creds)
    file = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute(http=https)  # type: ignore
    )

    print(
        f'File has uploaded with name: "{file_metadata["name"]}" and ID: "{file.get("id")}".'
    )


def upload_file(
    service, file_path: str, parent_folder_id: str, mimetype: str = "text/plain"
):
    """Upload file to google drive
    Returns : None

    """
    file_metadata = {"name": os.path.basename(file_path), "parents": [parent_folder_id]}
    media = MediaFileUpload(file_path, mimetype=mimetype)

    # pylint: disable=maybe-no-member
    file = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()  # type: ignore
    )

    print(
        f'File has uploaded with name: "{file_metadata["name"]}" and ID: "{file.get("id")}".'
    )


def read_file_content_thread_safe(file_id):
    """Read a file from Google Drive
    Returns : File content

    """

    try:
        service = create_service()
        # pylint: disable=maybe-no-member
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read()

    except HttpError as error:
        print(f"An error occurred: {error}")

    return None


def read_folder_files_content(
    service,
    folder_id: str = None,
    folder_name: str = None,
    parent_folder_id: str = None,
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

        files = list_folders_or_files(service, folder_id)
        total_files = len(files)
        if files is None:
            return None

        # use tqdm to show progress. Make concurrent requests to read file content
        with tqdm.tqdm(total=total_files) as pbar:
            with ThreadPoolExecutor(max_workers=cpu_count() // 2) as executor:
                futures = []
                for index, file in enumerate(
                    files
                ):  # since we are ordering by name, we can use index to save QAhelper progress
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

                    pbar.update(1)
                    yield file_index, file_name, result.result()

    except HttpError as error:
        print(f"An error occurred: {error}")

    return None
