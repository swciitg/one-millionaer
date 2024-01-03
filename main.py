# With this script you can download millions of files from a onedrive folder.
# This script was copied from https://github.com/O365/python-o365/blame/master/examples/onedrive_example.py
#
# Original Authors:
#   * https://github.com/lamductan
#   * https://github.com/eshifflett
# 
# Modifications By:
#   * https://github.com/iamtalhaasghar
#
# 2024-01-04

import os
import sqlite3
import argparse
from O365 import Account
from O365 import FileSystemTokenBackend
import logging
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv('CLIENT_ID')  # Your client_id
client_secret = os.getenv('CLIENT_SECRET')  # Your client_secret, create an (id, secret) at https://apps.dev.microsoft.com
scopes = ['basic', 'https://graph.microsoft.com/Files.ReadWrite.All']
CHUNK_SIZE = 1024 * 1024 * 5
logging.basicConfig(filename='log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(console_handler)


class O365Account():
    def __init__(self, client_id=client_id,
                 client_secret=client_secret, scopes=scopes):
        self.client_id = client_id
        self.client_secret = client_secret
        self.account = Account(credentials=(client_id, client_secret))
        self.token_backend = FileSystemTokenBackend(token_path='.', token_filename='o365_token.txt')
        #self.authenticate(scopes)
        self.storage = self.account.storage()
        self.drives = self.storage.get_drives()
        self.my_drive = self.storage.get_default_drive()  # or get_drive('drive-id')
        self.root_folder = self.my_drive.get_root_folder()

    def authenticate(self, scopes=scopes):
        try:
            # Attempt to load a stored token
            result = self.account.authenticate(scopes=scopes, token_backend=self.token_backend)
        except FileNotFoundError as e:
            # If no stored token is found, prompt for consent
            result = self.account.authenticate(scopes=scopes, token_backend=self.token_backend)

        if not result:
            # Handle authentication failure (e.g., user did not grant consent)
            print("Authentication failed. Please check your credentials and try again.")
            exit()

    def get_drive(self):
        return self.my_drive

    def get_root_folder(self):
        return self.root_folder

    def fetch_tweets(self):
        folder = self.my_drive.get_item_by_path('/tweets/')

        conn = sqlite3.connect('tweets.db')
        cursor = conn.cursor()


        cursor.execute("SELECT COUNT(*) FROM files;")
        skip_count = cursor.fetchone()[0]
       
        # Commit the changes and close the connection

        #folder.download_contents('/mnt/data/o365/tweets')
        all_items = folder.get_items()
        file_count = 0
        for item in all_items:
            if item.is_file:
                try:
                    file_count += 1
                    
                    # save changes to db after a while
                    if file_count % 1000 == 0:
                        logging.info(f'saving changes {file_count}')
                        conn.commit()

                    # skip alread saved files
                    if file_count < skip_count:
                        continue
                    logging.debug(f"{file_count}. {item.name}")
                    #p = f'/mnt/data/o365/tweets/'
                    #item.download(p)
                    # Use parameterized query to prevent SQL injection
                    cursor.execute('''
                          INSERT INTO files (name) VALUES (?);
                    ''', (item.name,))                    
                except sqlite3.IntegrityError:
                    pass                

        conn.commit()
        conn.close()

        '''
        # List the first 10 files in the "tweets" folder
        files = tweets_folder.get_items(limit=10)

        # Print the names of the files
        for file in files:
            print(file.name)
        
        '''

    def download_files_in_parallel(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Use submit to asynchronously execute download_file for each file
            futures = [executor.submit(self.download_file, folder_path, file_name, os.path.join(destination_folder, file_name)) for file_name in file_names]

            # Wait for all futures to complete
            concurrent.futures.wait(futures)


    def get_folder_from_path(self, folder_path):
        if folder_path is None:
            return self.my_drive

        subfolders = folder_path.split('/')
        if len(subfolders) == 0:
            return self.my_drive

        items = self.my_drive.get_items()
        for subfolder in subfolders:
            try:
                subfolder_drive = list(filter(lambda x: subfolder in x.name, items))[0]
                items = subfolder_drive.get_items()
            except:
                raise ('Path {} not exist.'.format(folder_path))
        return subfolder_drive

    ''' Upload a file named $filename to onedrive folder named $destination. '''

    def upload_file(self, filename, destination=None):
        folder = self.get_child_folder(self.root_folder, destination)
        print('Uploading file ' + filename)
        folder.upload_file(item=filename)

    ''' Download a file named $filename to local folder named $to_path. '''

    def download_file(self, filename, to_path=None):
        dirname = os.path.dirname(filename)
        basename = os.path.basename(filename)
        folder = self.get_folder_from_path(dirname)
        items = folder.get_items()
        if not os.path.exists(to_path):
            os.makedirs(to_path)
        try:
            file = list(filter(lambda x: basename == x.name, items))[0]
            print('Downloading file ' + filename)
            file.download(to_path, chunk_size=CHUNK_SIZE)
            return True
        except:
            print('File {} not exist.'.format(filename))
            return False

    def _get_child_folder(self, folder, child_folder_name):
        items = folder.get_items()
        found_child = list(filter(lambda x: x.is_folder and x.name == child_folder_name, items))
        return found_child[0] if found_child else folder.create_child_folder(child_folder_name)

    ''' Get child folder, folder tree from root folder. If child folder not exist, make it. '''

    def get_child_folder(self, folder, child_folder_name):
        child_folder_names = child_folder_name.split('/')
        for _child_folder_name in child_folder_names:
            folder = self._get_child_folder(folder, _child_folder_name)
        return folder

    '''
    Upload entire folder named $folder_name from local to onedrive folder named $destination.
    Keep cloud folder structure as that of local folder.
    '''

    def upload_folder(self, folder_name, destination=None):
        print()
        print('Uploading folder ' + folder_name)
        if destination is None:
            destination = folder_name
        destination_item = self.get_child_folder(self.root_folder, destination)

        for file in os.listdir(folder_name):
            path = os.path.join(folder_name, file)
            if os.path.isfile(path):
                self.upload_file(path, destination)
            else:
                folder = self.get_folder_from_path(destination)
                child_destination = self.get_child_folder(folder, file)
                self.upload_folder(path, os.path.join(destination, file))

    '''
    Download entire folder named $folder_name from cloud to local folder named $to_folder.
    Keep local folder structure as that of cloud folder.
    '''

    def download_folder(self, folder_name, to_folder='.', file_type=None):
        to_folder = os.path.join(to_folder, folder_name)
        self._download_folder(folder_name, to_folder, file_type)

    def _download_folder(self, folder_name, to_folder='.', file_type=None):
        print()
        print('Downloading folder ' + folder_name)
        current_wd = os.getcwd()
        if to_folder is not None and to_folder != '.':
            if not os.path.exists(to_folder):
                os.makedirs(to_folder)
            os.chdir(to_folder)

        if folder_name is None:
            folder = self.get_drive()
        folder = self.get_folder_from_path(folder_name)

        items = folder.get_items()
        if file_type is None:
            file_type = ''
        files = list(filter(lambda x: file_type in x.name or x.is_folder, items))

        for file in files:
            file_name = file.name
            abs_path = os.path.join(folder_name, file_name)
            if file.is_file:
                print('Downloading file ' + abs_path)
                file.download(chunk_size=CHUNK_SIZE)
            else:
                child_folder_name = abs_path
                self._download_folder(child_folder_name, file_name, file_type)
        os.chdir(current_wd)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--function", help="method used")
    parser.add_argument("-s", "--source", help="source path", default=".")
    parser.add_argument("-d", "--destination", help="destination path", default=".")
    parser.add_argument("-t", "--file-type", default="")
    return parser.parse_args()


def main():
    account = O365Account()
    account.fetch_tweets()
    exit()
    args = parse_arguments()
    function_name = args.function
    source = args.source
    destination = args.destination

    if function_name == 'download_file':
        account.download_file(source, destination)
    elif function_name == 'upload_file':
        account.upload_file(source, destination)
    elif function_name == 'download_folder':
        account.download_folder(source, destination, args.file_type)
    elif function_name == 'upload_folder':
        account.upload_folder(source, destination)
    else:
        print('Invalid function name')


if __name__ == '__main__':
    '''
    Usage:

    1. To download a file, run:
            python -f download_file -s <Your onedrive-file-path> -d <Your local-folder-path>

    2. To upload a file, run:
            python -f upload_file -s <Your local-file-path> -d <Your onedrive-folder-path>

    3. To download a folder, run:
            python -f download_folder -s <Your onedrive-folder-path> -d <Your local-folder-path>

    4. To upload a folder, run:
            python -f upload_folder -s <Your local-folder-path> -d <Your onedrive-folder-path>

    (onedrive-folder-path/onedrive-file-path must be relative path from root folder of your onedrive)
    '''

    main()

