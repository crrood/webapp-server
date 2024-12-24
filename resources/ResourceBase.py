import logging

from db import DB
from flask import request
from flask_restful import Resource


class ResourceBase(Resource):
    def __init__(self, resource_name):
        self.resource_name = resource_name
        self.db = DB()

    def get(self, resource_id):
        return self.db.query_document_by_id(self.resource_name, resource_id)

    def delete(self, resource_id):
        return self.db.delete_document_by_id(self.resource_name, resource_id)

    # resource_id is just included here to match the method signature of the
    # other methods
    def put(self, resource_id):
        data = request.get_json()
        return self.db.upsert_document(self.resource_name, data)


class ResourceListBase(Resource):
    def __init__(self, resource_name):
        self.resource_name = resource_name
        self.db = DB()

    def get(self):
        page_number = request.args.get("page", default=0, type=int)

        query = request.args.to_dict()
        if "page" in query:
            del query["page"]

        return self.db.query_collection(self.resource_name, page_number, query)

    def put(self):
        data = request.get_json()
        return self.db.upsert_document(self.resource_name, data)
