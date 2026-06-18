from flask import Flask, jsonify, request, render_template
import os
import glob
import json

app = Flask(__name__)

@app.route('/')
def index():
    return "running"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)