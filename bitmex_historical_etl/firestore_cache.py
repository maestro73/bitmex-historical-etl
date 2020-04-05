import datetime
import os

import firebase_admin
from firebase_admin import firestore
from firebase_admin.credentials import Certificate

from .constants import BIGQUERY_TABLE_NAME, FIREBASE_ADMIN_CREDENTIALS, FIREBASE_URL
from .utils import is_local


class FirestoreCache:
    def __init__(self, date):
        self.stop_execution = False
        self.date = date
        self.timestamp = datetime.datetime.combine(
            self.date, datetime.datetime.min.time()
        )

        if "FIREBASE_INIT" not in os.environ:
            if is_local():
                certificate = Certificate(os.environ[FIREBASE_ADMIN_CREDENTIALS])
                firebase_admin.initialize_app(certificate)
            else:
                options = {"databaseURL": os.environ[FIREBASE_URL]}
                firebase_admin.initialize_app(options=options)
            os.environ["FIREBASE_INIT"] = "true"

        self.firestore = firestore.client()

        self.collection = os.environ[BIGQUERY_TABLE_NAME]
        self.document = self.date.isoformat()
        data = self.get()
        if data and data.get("ok", False):
            self.stop_execution = True
            print(f"Bitmex data: {self.document} exists")

    def get(self):
        document = (
            self.firestore.collection(self.collection).document(self.document).get()
        )
        return document.to_dict()

    def set(self, data):
        self.firestore.collection(self.collection).document(self.document).set(data)

    def delete(self):
        self.firestore.collection(self.collection).document(self.document).delete()
