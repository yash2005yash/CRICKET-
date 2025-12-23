from flask import Flask
from threading import Thread
import os

app = Flask(__name__)


@app.route('/')
def home():
    return "I am alive! Safari Bot and Gredex Bot are running."


@app.route('/safari')
def safari():
    return "I am alive! Safari Bot is running."


@app.route('/gredex')
def gredex():
    return "I am alive! Gredex Bot is running."


def run():
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)


def keep_alive():
    t = Thread(target=run)
    t.daemon = True
    t.start()