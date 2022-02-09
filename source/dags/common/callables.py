import kaggle


def download_and_unzip(dataset_name: str, datalake_folder_path: str):
    print(f'Downloading dataset {dataset_name}')

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(dataset_name, path=datalake_folder_path, unzip=True, force=True)

    print(f'Files was successfully saved into {datalake_folder_path}')
