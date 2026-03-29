from box import Box
from dotenv import load_dotenv
import os
import websocket
import yaml

def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("closed")

def on_open_for_symbols(symbols):
    def on_open(ws):
        for symbol in symbols:
            ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')
    return on_open

def main():
    load_dotenv()

    with open("config.yaml", "r") as f:
        config_dict = yaml.safe_load(f);

    config = Box(config_dict)

    api_key = os.getenv("FINNHUB_API_KEY")
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={api_key}",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open_for_symbols(config.finnhub.symbols)
    ws.run_forever()

if __name__ == "__main__":
    main()