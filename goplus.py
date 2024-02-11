import pandas as pd
from goplus.token import Token


class GoPlusInteractor:
    def __init__(self, access_token):
        self.token = Token(access_token=access_token)

    def fetch_data(self, chain_id, addresses):
        try:
            data = self.token.token_security(chain_id=chain_id, addresses=addresses)
            return data
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def parse_to_dataframe(self, response):
        try:
            if hasattr(response, 'result'):
                results = response.result
                flattened_data = []
                for address, details in results.items():
                    # Convert details into a dictionary if it's not already one
                    details_dict = vars(details) if not isinstance(details, dict) else details
                    details_dict['address'] = address  # Add the address to the details
                    flattened_data.append(details_dict)
                df = pd.json_normalize(flattened_data)
                return df
            else:
                print("Response does not have a 'result' attribute.")
                return None
        except Exception as e:
            print(f"An error occurred while parsing the data: {e}")
            return None

