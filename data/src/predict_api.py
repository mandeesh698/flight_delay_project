from flask import Flask, request, jsonify
import joblib
import pandas as pd

app = Flask(__name__)
model = joblib.load("models/rf_delay_model.joblib")

@app.route('/predict', methods=['POST'])
def predict():
    data = request.json
    row = pd.DataFrame([data])
    prob = model.predict_proba(row)[0][1]
    pred = int(prob >= 0.5)
    return jsonify({'predicted_delay': pred, 'probability_delay': float(round(prob,3))})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
