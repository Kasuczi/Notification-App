from pushover import *
from GeckoTerminalApi import *
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def format_message(data):
    formatted_data = {key: human_readable_number(value) if isinstance(value, (int, float)) else value for key, value in
                      data.items()}
    return "\n".join(f"{key} -> {value}" for key, value in formatted_data.items()) + "\n\n"


def convert_to_float(column):
    try:
        return column.astype(float)
    except ValueError:
        return column


def human_readable_number(num):
    """
    Convert a number into a human-readable format with suffixes.
    """
    for unit in ['', 'K', 'M', 'B', 'T']:
        if abs(num) < 1000:
            return f"{num:.1f}{unit}"
        num /= 1000
    return f"{num:.1f}T"


if __name__ == "__main__":
    app_token = ""
    user_key = ""

    notifier = PushoverNotifier(app_token, user_key)
    api = GeckoTerminalAPI()

    all_pools = pd.DataFrame()

    specified_networks = ['eth', 'ton']

    previous_data = []
    previous_ids = set()
    while True:
        for network in specified_networks:
            logging.info(f"Fetching new pools for network: {network}")
            new_pools = api.get_new_pools(network)
            all_pools = pd.concat([all_pools, new_pools], ignore_index=True)

        all_pools = all_pools.sort_values(by=['attributes.pool_created_at', 'attributes.volume_usd.h24'],
                                          ascending=False)
        all_pools = all_pools.apply(convert_to_float)
        all_pools['chain'] = all_pools['id'].str.split('_', expand=True)[0]

        result = all_pools[
            ["id", "attributes.name", "attributes.pool_created_at", "attributes.fdv_usd", "attributes.reserve_in_usd",
             "attributes.volume_usd.h24", "attributes.price_change_percentage.h1",
             "attributes.price_change_percentage.h24"]]
        new_column_names = ["id", "name", "pool_created_at", "fdv_usd", "reserve_in_usd", "volume_usd_24h",
                            "price_change_percentage_1h", "price_change_percentage_24h"]
        result.columns = new_column_names
        result['pool_created_at'] = pd.to_datetime(result['pool_created_at'])
        result['pool_created_at'] = result['pool_created_at'] + pd.Timedelta(hours=1)

        current_pools_dicts = result.to_dict(orient='records')

        new_records = [record for record in current_pools_dicts if record['id'] not in previous_ids]

        if new_records:
            message_str = "".join(format_message(record) for record in new_records)
            response = notifier.send_notification(message_str, url_title='Coingecko Chart',
                                                  url='https://www.geckoterminal.com/pl/zksync/pools/',
                                                  title="New pool alert")
            logging.info("Scan has been done, found new pool, alert has been sent")
            previous_ids.update(record['id'] for record in new_records)
        else:
            logging.info("No new pools found, waiting for new pools...")

        previous_data = current_pools_dicts
        logging.info("Scan has been done, starting a new scan")