import json
import logging
import os
import traceback

from bson import ObjectId, json_util
from flask import Response, jsonify, make_response
from pymongo import MongoClient


class DB:

    DATABASE = "mongoDatabase"
    ITEMS_PER_PAGE = 10

    def __init__(self):
        self.client = self.__get_client()

    # connect to DB
    def __get_client(self) -> MongoClient:
        mongodb_username = os.environ.get("MONGO_INITDB_ROOT_USERNAME")
        mongodb_password = os.environ.get("MONGO_INITDB_ROOT_PASSWORD")
        mongodb_connstring = f"mongodb://{mongodb_username}:{mongodb_password}@mongodb"
        client = MongoClient(mongodb_connstring)
        return client

    def __get_database(self) -> MongoClient:
        return self.client[DB.DATABASE]

    def __get_collection(self, collection: str) -> MongoClient:
        return self.__get_database()[collection]

    def query_collection(
        self, collection: str, page_number: int, query: object = {}
    ) -> Response:
        """Query ITEMS_PER_PAGE documents from a collection, offset by page_number

        Parameters:
        collection (string): Name of the db collection
        page_number (int): Offset to query from, 0-indexed
        query (object): Query to filter the results

        Returns:
        (Response): Body is an array of JSON documents
        """
        logging.info(f"query_collection in {collection}")
        if query != {}:
            logging.info(f"query: {query}")

        client = self.__get_collection(collection)

        offset = page_number * DB.ITEMS_PER_PAGE
        results = client.find(limit=DB.ITEMS_PER_PAGE, skip=offset, filter=query)

        result_array = []
        for result in results:
            result_array.append(json.loads(json_util.dumps(result)))

        logging.info(result_array)
        return make_response(jsonify(result_array), 200)

    def query_document_by_id(self, collection: str, id: str) -> Response:
        """Query a document from the database using an _id value

        Parameters:
        collection (string): Name of the db collection
        id (string): ID of the document - must convert to an ObjectId

        Returns:
        (Response): JSON dump of document OR error string
        """
        logging.info(f"query_document_by_id in {collection} with id {id}")

        client = self.__get_collection(collection)
        try:
            object_id = self.__convert_to_oid(id)
        except:
            response = f"Could not convert {id} to ObjectId"
            status_code = 422
            return make_response(response, status_code)

        data = client.find_one({"_id": object_id})

        if data != None:
            response = json.loads(json_util.dumps(data))
            status_code = 200
        else:
            response = f"id {id} not found in {collection}"
            status_code = 404

        logging.info(data)
        return make_response(response, status_code)

    def upsert_document(self, collection: str, data: dict) -> Response:
        """Upsert a document in the database. Replaces the document completely if it
        exists. If "_id" is present in the object, assumed to be an update
        operation. If "_id" is not present, assumed to be an insert operation.

        Parameters:
        collection (string): Name of the db collection
        data (dict): JSON document to be upserted

        Returns:
        (Response): Status message
        """
        logging.info(f"upsert_document into {collection}")
        logging.info(data)
        if "_id" in data:
            if self.query_document_by_id(collection, data["_id"]).status_code == 200:
                return self.__update_document(collection, data)
            else:
                return make_response("data had _id but wasn't found in database", 400)
        else:
            return self.__insert_document(collection, data)

    def __update_document(self, collection: str, data: dict) -> Response:
        """Update a document in the database. Throws an error if it
        doesn't already exist.

        Parameters:
        collection (string): Name of the db collection
        data (dict): JSON document to be updated

        Returns:
        (Response): Status message
        """
        logging.info(f"update_document in {collection}")
        logging.info(data)

        id = data.pop("_id", None)
        client = self.__get_collection(collection)

        try:
            object_id = self.__convert_to_oid(id)
        except:
            response_object = {
                "success": False,
                "message": f"Could not convert {id} to ObjectId",
            }
            status_code = 422
            return make_response(response_object, status_code)

        result = client.replace_one({"_id": object_id}, data)

        if result.matched_count == 1:
            response_object = {
                "success": True,
                "message": f"updated document with id {id}",
                "id": str(id),
                "updatedExisting": True,
            }
            status_code = 200
        elif result.matched_count == 0:
            # upstream error checking should catch this case
            response_object = {
                "success": False,
                "message": f"document with id {id} not found",
            }
            status_code = 404
        elif result.matched_count > 1:
            response_object = {
                "success": False,
                "message": f"too many documents matched id {id}",
            }
            status_code = 400
        elif result.modified_count == 0:
            response_object = {
                "success": False,
                "message": f"document with id {id} not updated",
            }
            status_code = 400

        logging.info(response_object)
        return make_response(response_object, status_code)

    def __insert_document(self, collection: str, data: dict) -> Response:
        """Insert a document into the database.

        Parameters:
        collection (string): Name of the db collection
        data (dict): JSON document to be inserted

        Returns:
        (Response): Status message
        """
        logging.info(f"insert_document in {collection}")
        logging.info(data)

        client = self.__get_collection(collection)
        result = client.insert_one(data)

        response_object = {
            "success": True,
            "message": "inserted document",
            "id": str(result.inserted_id),
            "updatedExisting": False,
        }
        status_code = 200

        logging.info(response_object)
        return make_response(response_object, status_code)

    def delete_document_by_id(self, collection: str, id: str) -> Response:
        """Delete a document from the database

        Parameters:
        collection (string): Name of the db collection
        id (string): ID of the document - must convert to an ObjectId

        Returns:
        (Response): Status message
        """
        logging.info(f"delete_document_by_id in {collection} with id {id}")

        client = self.__get_collection(collection)
        try:
            object_id = self.__convert_to_oid(id)
        except:
            response_object = {
                "success": False,
                "message": f"Could not convert {id} to ObjectId",
            }
            status_code = 422
            return make_response(response_object, status_code)

        result = client.delete_one({"_id": object_id})

        if result.deleted_count > 0:
            response_object = {
                "success": True,
                "message": f"deleted document with id {id}",
                "id": str(id),
            }
            status_code = 200
        else:
            response_object = {
                "success": False,
                "message": f"document with id {id} not found",
            }
            status_code = 404

        logging.info(response_object)
        return make_response(response_object, status_code)

    def reset(self) -> str:
        """Drop the DB and refill with test data from resources.json"""
        logging.info("resetting DB")
        self.client.drop_database(DB.DATABASE)

        with open("/config/resources.json") as f:
            sample_data = json.load(f)

        for resource in sample_data:
            for i in range(len(sample_data[resource])):
                result = self.__get_collection(resource).insert_one(
                    sample_data[resource][i]
                )

        response_object = {"success": True, "message": str(result.inserted_id)}
        return make_response(response_object, 200)

    def test_db(self):
        """Upserts a document, queries the collection, queries the document,
        updates the document, and deletes the document"""
        collection = "test_collection"
        operations = []

        try:
            # insert the document
            operations.append("insert")
            test_data = {"name": "test", "value": 42}
            response = self.upsert_document(collection, test_data)
            if response.status_code != 200:
                return f"upsert failed: {response.data}"
            data = json.loads(response.data)
            if data["success"] != True:
                return f"upsert failed: {response.data}"
            id = data["id"]

            # query the collection
            operations.append("query collection")
            response = self.query_collection(collection, 0)
            if response.status_code != 200:
                return f"query collection failed: {response.data}"
            if len(json.loads(response.data)) == 0:
                return "query collection failed: no results"
            if json.loads(response.data)[0]["value"] != 42:
                return f"query collection failed: wrong data\n{response.data}"

            # query the document
            operations.append("query document")
            response = self.query_document_by_id(collection, id)
            if response.status_code != 200:
                return f"query document failed: {response.data}"
            if json.loads(response.data)["value"] != 42:
                return f"query document failed: wrong data\n{response.data}"

            # insert the same document again
            operations.append("insert again")
            response = self.upsert_document(collection, test_data)
            data = json.loads(response.data)
            if response.status_code != 200:
                return f"upsert failed: {response.data}"
            if data["success"] != True:
                return f"upsert failed: {response.data}"
            if data["id"] != id:
                return f"upsert failed: wrong id\n{response.data}"

            # update the document
            operations.append("update")
            new_test_data = {
                "name": "test",
                "value": 43,
                "_id": self.__convert_to_oid(id),
            }
            response = self.upsert_document(
                collection,
                new_test_data,
            )
            if response.status_code != 200:
                return f"update failed: {response.data}"
            data = json.loads(response.data)
            if data["success"] != True:
                return f"upsert failed: {response.data}"
            response = self.query_document_by_id(collection, id)
            if response.status_code != 200:
                return f"query document after update failed: {response.data}"
            if json.loads(response.data)["value"] != 43:
                return (
                    f"query document after update failed: wrong data\n{response.data}"
                )

            # delete the document
            operations.append("delete")
            response = self.delete_document_by_id(collection, id)
            if response.status_code != 200:
                return f"delete failed: {response.data}"
            if json.loads(response.data)["id"] != id:
                return f"delete failed: wrong id\n{response.data}"
            response = self.query_document_by_id(collection, id)
            if response.status_code != 404:
                return (
                    f"query document after delete failed (succeeded): {response.data}"
                )

            # clean up
            operations.append("clean up")
            self.__get_database().drop_collection(collection)

            return f"DB test passed: {operations}"
        except Exception as e:
            self.__get_database().drop_collection(collection)
            return f"DB test failed: {e}\n{operations}\n{traceback.format_exc()}"

    # utility methods
    def __convert_to_oid(self, id):
        if type(id) == dict:
            return ObjectId(id["$oid"])
        else:
            return ObjectId(id)
