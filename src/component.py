import csv
import logging
import requests
import json
from datetime import datetime
import hashlib
import pandas as pd

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# Configuration variables
KEY_API_TOKEN = '#api_key'

class Component(ComponentBase):
    """
    Extends the base class for general Python components. Initializes the CommonInterface
    and performs configuration validation.

    For easier debugging, the data folder is picked up by default from `../data` path,
    relative to the working directory.

    If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self): 
        super().__init__()
        self._output_writer = None

    def send_data_to_api(self, payload):
        """
        Sends data to the specified API endpoint.
        """
        url = "https://api.brevo.com/v3/contacts/batch"
        headers = {
            'Content-Type': 'application/json',
            'api-key': self.configuration.parameters.get(KEY_API_TOKEN)
        }
        response = requests.post(url, headers=headers, data=json.dumps(payload))

        if response.status_code == 204:
            return "Email updated (Sms,Email)"
        elif response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            logging.warning(f"API returned 404 status. Response: {response.text}")
            return "Email not found"
        else:
            raise UserException(f"Failed to send data to API. Status Code: {response.status_code}, Response: {response.text}")

    def delete_data_from_api(self, email):
        """
        Deletes data from the specified API endpoint based on email.
        """
        url = f"https://api.brevo.com/v3/smtp/blockedContacts/{email}"
        headers = {
            "accept": "application/json",
            "api-key": self.configuration.parameters.get(KEY_API_TOKEN)
        }
        response = requests.delete(url, headers=headers)

        if response.status_code == 204:
            return "Unblock or resubscribe a transactional Email"
        elif response.status_code == 404:
            logging.warning(f"API returned 404 status. Response: {response.text}")
            return "Transactional Email not found"
        else:
            raise UserException(f"Failed to delete data from API. Status Code: {response.status_code}, Response: {response.text}")

    def _parse_table(self):
        """
        Parses the data table.
        """
        # Get tables configuration
        in_tables = self.get_input_tables_definitions()

        if len(in_tables) == 0:
            raise UserException('There is no table specified on the input mapping! You must provide one input table!')
        elif len(in_tables) > 1:
            raise UserException('There is more than one table specified on the input mapping! You must provide one input table!')

        # Get table
        table = in_tables[0]

        # Get table data
        logging.info(f'Processing input table: {table.name}')
        df = pd.read_csv(f'{table.full_path}', dtype=str)

        # Return error if there is no data
        if df.empty:
            logging.info(f'Input table {table.name} is empty!')

        return df

    def _create_tables_definitions(self):
        """
        Creates the tables definitions for output tables.
        """
        # Create tables definitions
        self._stats_table = self.create_out_table_definition('stats.csv', incremental=True, primary_key=['timestamp'])
        self.output_table = self.create_out_table_definition(
            'output.csv', incremental=True, primary_key=['id'])

        # Open output file, set headers, writer and write headers
        self._output_file = open(self.output_table.full_path, 'wt', encoding='UTF-8', newline='')
        output_fields = ['id',  'timestamp','email', 'status']
        self._output_writer = csv.DictWriter(self._output_file, fieldnames=output_fields)
        self._output_writer.writeheader()

    def create_hash(self, email, timestamp):
        """
        Creates a hash using email and timestamp.
        """
        data = f"{email}{timestamp}"
        return hashlib.md5(data.encode()).hexdigest()

    def run(self):
        """
        Main entrypoint
        """
        try:
            # Initialize _output_writer
            self._create_tables_definitions()

            # Parse the input table
            csv_data = self._parse_table()

            # Select specific columns from CSV data
            selected_columns = ['email', 'emailBlacklisted', 'smsBlacklisted', 'transactionalContact']
            csv_data = csv_data[selected_columns]
            
            # Convert 'emailBlacklisted' and 'smsBlacklisted' columns to boolean
            csv_data['emailBlacklisted'] = csv_data['emailBlacklisted'].apply(lambda x: True if x.lower() == 'true' else False)
            csv_data['smsBlacklisted'] = csv_data['smsBlacklisted'].apply(lambda x: True if x.lower() == 'true' else False)

            # Prepare payload with a maximum of 100 records
            max_records_per_request = 1
            payloads = [csv_data.iloc[i:i + max_records_per_request].to_dict(orient='records') for i in range(0, len(csv_data), max_records_per_request)]

            # Collect responses for each payload
            responses = []
            for payload in payloads:
                if payload:  # Check if payload is not empty before sending
                    api_response = self.send_data_to_api({"contacts": payload})
                    responses.append(api_response)

                    # Log email, timestamp, status, and create hash
                    for record in payload:
                        email = record['email']
                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        if isinstance(api_response, dict):
                            status = api_response.get('status', 'Error')
                        else:
                            status = api_response

                        # Create a hash using email and timestamp
                        record_hash = self.create_hash(email, timestamp)

                        # Write the data to the output CSV file
                        self._output_writer.writerow({
                            'id': record_hash,
                            'email': email,
                            'status': status,
                            'timestamp': timestamp
                        })

                        # Check if 'transactionalContact' is 'yes'
                        if record.get('transactionalContact', '').lower() == 'true':
                            # Delete the email using the delete_data_from_api method
                            delete_api_response = self.delete_data_from_api(email)

                            # Write the deleted data to the output CSV file
                            deleted_record_hash = self.create_hash(email, timestamp)
                            self._output_writer.writerow({
                                'id': deleted_record_hash,
                                'email': email,
                                'status': delete_api_response,
                                'timestamp': timestamp
                            })

        except UserException as exc:
            logging.exception(exc)
            exit(1)
        except Exception as exc:
            logging.exception(exc)
            exit(2)
        finally:
            # Close the file in the finally block to ensure it's closed even if an error occurs
            if self._output_file:
                self._output_file.close()

if __name__ == "__main__":
    comp = Component()
    comp.execute_action()
