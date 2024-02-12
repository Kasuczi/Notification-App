from pushover import *
from GeckoTerminalApi import *
import json
import logging
import warnings
from GoPlus import GoPlusInteractor

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def format_message(data, additional_info=None):
    formatted_data = {key: human_readable_number(value) if isinstance(value, (int, float)) else value for key, value in data.items()}
    message = "\n".join(f"{key} -> {value}" for key, value in formatted_data.items())
    if additional_info:
        message += "\n----------\n" + "\n".join(additional_info)
    return message + "\n\n"


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


def check_flags(df):
    if df is None:
        return []  # or return None, depending on your handling logic

    red_flags = ['_is_airdrop_scam', '_transfer_pausable', '_trading_cooldown', '_selfdestruct', '_is_honeypot',
                 '_honeypot_with_same_creator', '_fake_token', '_is_proxy', '_external_call', '_cannot_sell_all',
                 '_personal_slippage_modifiable', '_cannot_buy', '_owner_change_balance']
    warning_flags = ['_hidden_owner', '_is_whitelisted', '_trust_list', '_is_blacklisted', '_slippage_modifiable',
                     '_is_mintable', '_anti_whale_modifiable', '_is_anti_whale']
    features = ['_buy_tax', '_sell_tax', '_holders']

    # Check red flags
    for flag in red_flags:
        if flag in df.columns and df[flag].notnull().any() and (df[flag] != '0').any():
            return None  # Has red flag, return None to skip this record

    # Collect warning flags and features
    alerts = []
    for flag in warning_flags + features:
        if flag in df.columns:
            value = df.iloc[0][flag]
            flag_upper = flag.upper()  # Convert flag to uppercase
            if flag in warning_flags and ((flag == '_is_anti_whale' and value != '1') or (
                    flag != '_is_anti_whale' and (value == '0' or pd.isnull(value)))):
                alerts.append(f"{flag_upper} alert")
            elif flag == '_holders':
                if isinstance(value, str):  # If it's a string, it might be JSON
                    try:
                        holders_list = json.loads(value)
                        if isinstance(holders_list, list):
                            holders_count = len(holders_list)
                            alerts.append(f"HOLDERS: {holders_count}")
                    except json.JSONDecodeError:
                        # If it's not valid JSON, log an error or handle as appropriate
                        logging.error("Unable to decode _holders, unexpected format.")
                elif isinstance(value, list):
                    # If it's already a list (of dicts, presumably), directly count the items
                    holders_count = len(value)
                    alerts.append(f"HOLDERS: {holders_count}")
                else:
                    # If it's neither a string nor a list, log an error or handle as appropriate
                    logging.error(f"Unexpected type for _holders: {type(value)}")
            else:
                alerts.append(f"{flag_upper}: {value}")

    return alerts



if __name__ == "__main__":
    app_token = ""
    user_key = ""

    notifier = PushoverNotifier(app_token, user_key)
    api = GeckoTerminalAPI()

    all_pools = pd.DataFrame()

    specified_networks = ['eth']

    previous_data = []
    previous_ids = set()
    interactor = GoPlusInteractor(access_token=None)

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
            ["id", "relationships.base_token.data.id", "attributes.name", "attributes.pool_created_at",
             "attributes.fdv_usd", "attributes.reserve_in_usd",
             "attributes.volume_usd.h24", "attributes.price_change_percentage.h24",
             "attributes.transactions.h24.buyers", "attributes.transactions.h24.buys",
             "attributes.transactions.h24.sellers", "attributes.transactions.h24.sells"]]
        new_column_names = ["ID", "TOKEN_ID", "NAME", "CREATED", "FDV", "LIQUIDITY", "VOLUME",
                            "% CHANGE 24", "BUYERS", "BUYS", "SELLERS", "SELLS"]
        result.columns = new_column_names
        result['CREATED'] = pd.to_datetime(result['CREATED'])
        result['CREATED'] = result['CREATED'] + pd.Timedelta(hours=1)
        result['TOKEN_ID'] = result['TOKEN_ID'].apply(lambda x: x[4:])

        current_pools_dicts = result.to_dict(orient='records')
        new_records = [record for record in current_pools_dicts if record['ID'] not in previous_ids]

        if new_records:
            for record in new_records:
                response = interactor.fetch_data(chain_id="1", addresses=[record['TOKEN_ID']])
                df = interactor.parse_to_dataframe(response)

                alerts = check_flags(df)
                if alerts is not None:  # Only proceed if no red flags
                    message_str = format_message(record, additional_info=alerts)
                    response = notifier.send_notification(message_str, url_title='Coingecko Chart',
                                                          url='https://www.geckoterminal.com/pl/zksync/pools/',
                                                          title="New pool alert")
                    logging.info("Scan has been done, found new pool without red flags, alert has been sent")
                else:
                    logging.info(f"Pool {record['TOKEN_ID']} skipped due to red flags")

            previous_ids.update(record['ID'] for record in new_records)
        else:
            logging.info("No new pools found, waiting for new pools...")

        logging.info("Scan has been done, starting a new scan")