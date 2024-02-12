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
    api_key = "AKHAMYBR2CHS5GYRU8PYAK1S19GKAVPYD4"
    wallet_addresses = ["0x618d6c2a6cc1aa17f411e2b2000ee00895334d87",
                        "0x4322fd98f95a219d2aae2bba6664e3574b4c3708",
                        "0x6400f6ebf6e958ec1887c92f30515bbad3745073",
                        "0x4401fe38292256a2fc3b2054c44a2fae12b68ac1",
                        "0xbba84019aeaaa8b736e1d9c7c1c6074fd47d75db",
                        "0xa3737deca37c6d15c582309d766322bad789e6fa",
                        "0xea55c928c2549722a21541d3b6c20d845eb28c2c",
                        "0x8b94f9c38a6cacc49cbd48ad867c0a9c2f89eb11",
                        "0x390a7731bb3573617c59060c6b3764664f15dada",
                        "0x25cd302e37a69d70a6ef645daea5a7de38c66e2a",
                        "0x618d6c2a6cc1aa17f411e2b2000ee00895334d87",
                        "0xa2d18c5ca7170c55aed88793f1b06386d1e20a2d",
                        "0x287e2c76aab4720786076c3deedd7dd386092050",
                        "0x59220979d662bb96b40b5b47c5cc8939254f4d72",
                        "0x0b6a06c07c58d13b6c725dbdb9cd1e2f6bac5527",
                        "0x4823be7f266c5bca85b7dbd8e41f4710e1c49a7b",
                        "0x9bcb2c3cbf1073e47f3c5c31f8ae8c7b951bb016",
                        "0x97b8dc4683514ae076490001fa39a02602ae97a0",
                        "0xed50e9e7ca905d2018b2db81005e39039a5b71c1",
                        "0xf8ce9f1bd06c7559ff45336d55b1c9de46ce019d",
                        "0x4ab89a958214b1f65ff9c3b110dc4eed1d021323",
                        "0xa45afb274e1028227ee11fd2a09f57ac9b2c2df6",
                        "0x33c6eb2167c95e3366dafdf3a11415e78657f48e",
                        "0xdb22ca143f6396ad289c79cdfa5cc47f65884162",
                        "0x3bdb66087e2760a9f7f6b4c0a3b49e69355e8a3c",
                        "0x0a62a72cfe67c25e077ff7b979135af224e35941",
                        "0x03770b07c5c315722c5866e64cde04e6e5793714",
                        "0xa294e2ca0e189953fddb3b12166845e487a15021"
                        ]

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
                new_column_names = ["TOKEN", "ADDRESS", "TYPE", "WHO", "WHEN", "VALUE"]
                result.columns = new_column_names

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