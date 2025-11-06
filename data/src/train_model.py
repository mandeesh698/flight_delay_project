import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import joblib

df = pd.read_csv("data/processed/processed.csv")
X = df.drop(columns=["delayed"])
y = df["delayed"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)
clf = RandomForestClassifier(n_estimators=150, random_state=42)
clf.fit(X_train, y_train)
joblib.dump(clf, "models/rf_delay_model.joblib")
# Save metrics (accuracy, classification report) into a text file for resume/demo
