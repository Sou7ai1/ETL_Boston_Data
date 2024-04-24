import csv
import logging
import sys
import os
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def fetch_csv_content_BostonGOV(url: str, dataset_name: str) -> bytes:
    """
    Fetches CSV content from the specified URL.

    Args:
        url (str): The URL of the CSV file to fetch.
        dataset_name (str): Name of the dataset.

    Returns:
        bytes: Content of the CSV file as bytes.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

        return response.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading file from URL: {e}")
        raise


def preview_csv_content(data: bytes, num_rows: int) -> None:
    """
    Preview the content of a CSV file.

    Args:
        data (bytes): The content of the CSV file as bytes.
        num_rows (int, optional): Number of rows to preview. Defaults to 10.

    Raises:
        Exception: If an error occurs while previewing the CSV content.
    """
    try:
        decoded_data = data.decode("utf-8")
        csv_reader = csv.DictReader(decoded_data.splitlines())

        logging.info(f"Preview of the first {num_rows} rows of the CSV file:")
        for i, row in enumerate(csv_reader):
            if i >= num_rows:
                break
            logging.info(row)
    except Exception as e:
        logging.error(f"Error previewing CSV content: {e}")


def save_as_csv(data: bytes, filename: str) -> None:
    """
    Save CSV content to a file.

    Args:
        data (bytes): The content of the CSV file as bytes.
        filename (str, optional): The name of the CSV file. Defaults to "output.csv".

    Raises:
        Exception: If an error occurs while saving the data as a CSV file.
    """
    try:
        with open(filename, "wb") as f:
            f.write(data)
        logging.info(f"CSV data saved to {filename}")
    except Exception as e:
        logging.error(f"Error saving data as CSV file: {e}")
        raise


def main():
    url = "https://data.boston.gov/dataset/e63a37e1-be79-4722-89e6-9e7e2a3da6d1/resource/73c7e069-701f-4910-986d-b950f46c91a1/download/tmp8mntlmrz.csv"
    dataset_name = "SHOOTINGS_BostonGOV"
    file_content = fetch_csv_content_BostonGOV(url, dataset_name)
    preview_csv_content(file_content, num_rows=10)
    save_as_csv(file_content, "ShootingsBostonGOV.csv")


if __name__ == "__main__":

    main()
