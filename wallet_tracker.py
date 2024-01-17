from pushover import *
from wallet_tracker import *
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def human_readable_number(num):
    """
    Convert a number into a human-readable format with suffixes.
    """
    for unit in ['', 'K', 'M', 'B', 'T']:
        if abs(num) < 1000:
            return f"{num:.1f}{unit}"
        num /= 1000
    return f"{num:.1f}T"


def format_message(data):
    formatted_data = {key: human_readable_number(value) if isinstance(value, (int, float)) else value for key, value in
                      data.items()}
    return "\n".join(f"{key} -> {value}" for key, value in formatted_data.items()) + "\n\n"


if __name__ == "__main__":
    api_key = ""
    wallet_addresses = [""]

    app_token = ""
    user_key = ""

    current_day_tracker = CurrentDayCryptoTransactionTracker(wallet_addresses, api_key)
    notifier = PushoverNotifier(app_token, user_key)

    previous_data = []
    while True:
        try:
            current_day_transactions = current_day_tracker.fetch_transactions()

            if current_day_transactions.empty:
                logging.info("No transactions found, fetching again...")
                continue

            current_day_combined_transactions = current_day_tracker.filter_and_label_transactions(
                current_day_transactions)
            current_day_combined_transactions['value'] = pd.to_numeric(current_day_combined_transactions['value'],
                                                                       errors='coerce')

            required_columns = ['tokenSymbol', 'contractAddress', 'Type', 'WalletAddress', 'value']
            if not all(column in current_day_combined_transactions.columns for column in required_columns):
                logging.error("One or more required columns are missing in the DataFrame.")
                continue

            if not current_day_combined_transactions.empty:
                grouped = current_day_combined_transactions.groupby(['tokenSymbol', 'contractAddress', 'Type'])

                multiple_addresses_filter = grouped.filter(lambda x: x['WalletAddress'].nunique() >= 3)
                multiple_addresses_filter['transaction_type'] = 'Multiple Addresses'

                same_address_multiple_trades_filter = current_day_combined_transactions.groupby(
                    ['tokenSymbol', 'contractAddress', 'WalletAddress', 'Type']).filter(lambda x: len(x) > 1)
                same_address_multiple_trades_filter['transaction_type'] = 'Same Address Multiple Times'

                combined_filtered = pd.concat(
                    [multiple_addresses_filter, same_address_multiple_trades_filter]).drop_duplicates()

                result = combined_filtered.groupby(['tokenSymbol', 'contractAddress', 'Type', 'transaction_type']).agg(
                    {'timeStamp': 'max', 'value': 'mean'}).reset_index()

                json_result = result.to_json(orient='records')
                json_result_dicts = json.loads(json_result)

                if 'previous_data' not in locals():
                    previous_data = []

                if isinstance(previous_data, str):
                    previous_data_dicts = json.loads(previous_data)
                else:
                    previous_data_dicts = previous_data

                new_records = [record for record in json_result_dicts if record not in previous_data_dicts]

                if new_records:
                    message_str = "".join(format_message(record) for record in new_records)
                    response = notifier.send_notification(message_str, url_title='Coingecko Chart',
                                                          url='https://www.geckoterminal.com/pl/zksync/pools/',
                                                          title="Whale alert")
                    logging.info("Scan has been done, found new transactions, alert has been sent")

            else:
                logging.info("Filtered transactions are empty, waiting for new transactions...")

            previous_data = json_result
            logging.info("Scan has been done, starting a new scan")

        except KeyError as e:
            logging.error(f"A KeyError occurred: {e}")

        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")