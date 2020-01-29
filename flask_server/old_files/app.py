from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin


app = Flask(__name__)

CORS(app)

entity = []


@app.route('/', methods=['POST'])
def test():
    data = request.json
    entity.append(data['entity'])
    return request.json

@app.route('/view', methods=['GET', 'POST'])
def view():

    return str(entity[0])

if __name__ == '__main__':
     app.run(port=5000)
