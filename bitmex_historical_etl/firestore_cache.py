import datetime
import os

import firebase_admin
from firebase_admin import firestore
from firebase_admin.credentials import Certificate

from .constants import FIREBASE_ADMIN_CREDENTIALS, FIREBASE_URL, FIRESTORE_COLLECTION
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

        self.collection = os.environ[FIRESTORE_COLLECTION]
        self.document = self.date.isoformat()
        data = self.get_data(self.collection, self.document)
        if data:
            self.stop_execution = True
            print(f"Data exists: {self.document}")

    def get_data(self, collection, document):
        document = self.firestore.collection(collection).document(document).get()
        return document.to_dict()

    def set_cache(self, symbols):
        data = {"symbols": symbols}
        self.firestore.collection(self.collection).document(self.document).set(data)
