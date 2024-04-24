import os
from kaggle.api.kaggle_api_extended import KaggleApi
from zipfile import ZipFile
import csv

def download_dataset_kaggle(dataset_name):
    """
    Downloads a dataset from Kaggle.

    Args:
        dataset_name (str): The name of the dataset on Kaggle.

    Raises:
        Exception: If an error occurs during dataset download or extraction.
    """
    try:
        
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(dataset_name, path=dataset_folder, force=True, quiet=True)  

        with ZipFile(os.path.join(dataset_folder, dataset_name.split('/')[-1] + '.zip'), 'r') as zip_ref:
            zip_ref.extract('crime.csv', dataset_folder)
        os.remove(os.path.join(dataset_folder, dataset_name.split('/')[-1] + '.zip'))  

    except Exception as e:
        print(f"An error occurred: {e}")

def preview_csv_content(dataset_folder, num_rows=10):
    """
    Preview the content of the CSV file in the downloaded dataset.

    Args:
        dataset_folder (str): Path to the folder containing the downloaded dataset.
        num_rows (int, optional): Number of rows to preview. Defaults to 10.
    """
    try:
        csv_path = os.path.join(dataset_folder, 'crime.csv')
        with open(csv_path, 'r', newline='') as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)  # Read the header
            print("Header:", header)
            print(f"Preview of the first {num_rows} rows of the CSV file:")
            for i, row in enumerate(csv_reader):
                if i >= num_rows:
                    break
                print(row)
    except Exception as e:
        print(f"An error occurred while previewing CSV content: {e}")

if __name__ == "__main__":
    dataset_name = "AnalyzeBoston/crimes-in-boston"
    dataset_folder = download_dataset_kaggle(dataset_name)
    preview_csv_content(dataset_folder)
