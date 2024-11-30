import json
import logging
import os

from bson import ObjectId, json_util
from flask import Response, jsonify, make_response
from pymongo import MongoClient

DATABASE = "mongoDatabase"
ITEMS_PER_PAGE = 10


# connect to DB
def get_client() -> MongoClient:
    mongodb_username = os.environ.get("MONGO_INITDB_ROOT_USERNAME")
    mongodb_password = os.environ.get("MONGO_INITDB_ROOT_PASSWORD")
    mongodb_connstring = f"mongodb://{mongodb_username}:{mongodb_password}@mongodb"
    client = MongoClient(mongodb_connstring)
    return client


def get_database() -> MongoClient:
    client = get_client()
    return client[DATABASE]


def get_collection(collection: str) -> MongoClient:
    client = get_database()
    return client[collection]


def query_collection(collection: str, page_number: int) -> Response:
    """Query n documents from a collection, offset by page_number

    Parameters:
    collection (string): Name of the db collection
    page_number (int): Offset to query from, 0-indexed

    Returns:
    (Response): Body is an array of JSON documents
    """
    client = get_collection(collection)

    offset = page_number * ITEMS_PER_PAGE
    results = client.find(limit=ITEMS_PER_PAGE, skip=offset)

    result_array = []
    for result in results:
        result_array.append(json.loads(json_util.dumps(result)))

    return make_response(jsonify(result_array), 200)


def query_document_by_id(collection: str, id: str) -> Response:
    """Query a document from the database using an _id value

    Parameters:
    collection (string): Name of the db collection
    id (string): ID of the document - must convert to an ObjectId

    Returns:
    (Response): JSON dump of document OR error string
    """
    logging.info("query_document_by_id")

    client = get_collection(collection)
    try:
        object_id = convert_to_oid(id)
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


def upsert_document(collection: str, data: dict) -> Response:
    """Upsert a document in the database. Replaces the document completely if it
    exists. If "_id" is present in the object, assumed to be an update
    operation. If "_id" is not present, assumed to be an insert operation.

    Parameters:
    collection (string): Name of the db collection
    data (dict): JSON document to be upserted

    Returns:
    (Response): Status message
    """
    if "_id" in data:
        if query_document_by_id(collection, data["_id"]).status_code == 200:
            return update_document(collection, data)
        else:
            return make_response("data had _id but wasn't found in database", 400)
    else:
        return insert_document(collection, data)


def update_document(collection: str, data: dict) -> Response:
    """Update a document in the database. Throws an error if it
    doesn't already exist.

    Parameters:
    collection (string): Name of the db collection
    data (dict): JSON document to be updated

    Returns:
    (Response): Status message
    """
    logging.info("update_document")
    logging.info(data)

    id = data.pop("_id", None)
    client = get_collection(collection)

    try:
        object_id = convert_to_oid(id)
    except:
        response = f"Could not convert {id} to ObjectId"
        status_code = 422
        return response, status_code

    result = client.replace_one({"_id": object_id}, data)

    if result.matched_count == 1:
        response = str(id)
        status_code = 200
    elif result.matched_count == 0:
        # upstream error checking should catch this case
        response = f"id {id} not found in {collection}"
        status_code = 404
    elif result.matched_count > 1:
        response = f"too many documents matched id {id}"
        status_code = 400
    elif result.modified_count == 0:
        response = f"document with id {id} not updated"
        status_code = 400

    return make_response(response, status_code)


def insert_document(collection: str, data: dict) -> Response:
    """Insert a document into the database

    Parameters:
    collection (string): Name of the db collection
    data (dict): JSON document to be inserted

    Returns:
    (Response): Status message
    """
    logging.info("insert_document")
    logging.info(data)

    client = get_collection(collection)
    result = client.insert_one(data)

    response = str(result.inserted_id)
    status_code = 200

    return make_response(response, status_code)


def delete_document_by_id(collection: str, id: str) -> Response:
    """Delete a document from the database

    Parameters:
    collection (string): Name of the db collection
    id (string): ID of the document - must convert to an ObjectId

    Returns:
    (Response): Status message
    """
    client = get_collection(collection)
    try:
        object_id = convert_to_oid(id)
    except:
        response = f"Could not convert {id} to ObjectId"
        status_code = 422
        return make_response(response, status_code)

    result = client.delete_one({"_id": object_id})

    if result.deleted_count > 0:
        response = f"deleted document with id {id}"
        status_code = 200
    else:
        response = f"id {id} not found in {collection}"
        status_code = 404

    return make_response(response, status_code)


def reset() -> str:
    """Drop the DB and refill with test data from resources.json"""
    client = get_client()
    client.drop_database(DATABASE)

    with open("config/resources.json") as f:
        sample_data = json.load(f)

    client = get_database()
    for resource in sample_data:
        for i in range(len(sample_data[resource])):
            result = client[resource].insert_one(sample_data[resource][i])

    return f"db reset - test resource id = {result.inserted_id}"


def test_db():
    """Upserts a document, queries the collection, queries the document,
    updates the document, and deletes the document"""
    collection = "test_collection"
    operations = []

    try:
        # insert the document
        operations.append("insert")
        test_data = {"name": "test", "value": 42}
        response = upsert_document(collection, test_data)
        if response.status_code != 200:
            return f"upsert failed: {response.data}"
        id = response.data.decode("utf-8")

        # query the collection
        operations.append("query collection")
        response = query_collection(collection, 0)
        if response.status_code != 200:
            return f"query collection failed: {response.data}"
        if len(json.loads(response.data)) == 0:
            return "query collection failed: no results"
        if json.loads(response.data)[0]["value"] != 42:
            return f"query collection failed: wrong data\n{response.data}"

        # query the document
        operations.append("query document")
        response = query_document_by_id(collection, id)
        if response.status_code != 200:
            return f"query document failed: {response.data}"
        if json.loads(response.data)["value"] != 42:
            return f"query document failed: wrong data\n{response.data}"

        # insert the same document again
        operations.append("insert again")
        response = upsert_document(collection, test_data)
        if response.status_code != 200:
            return f"upsert failed: {response.data}"
        if response.data.decode("utf-8") != id:
            return f"upsert failed: wrong id\n{response.data}"

        # update the document
        operations.append("update")
        response = upsert_document(
            collection, {"name": "test", "value": 43, "_id": convert_to_oid(id)}
        )
        if response.status_code != 200:
            return f"update failed: {response.data}"
        response = query_document_by_id(collection, id)
        if response.status_code != 200:
            return f"query document after update failed: {response.data}"
        if json.loads(response.data)["value"] != 43:
            return f"query document after update failed: wrong data\n{response.data}"

        # delete the document
        operations.append("delete")
        response = delete_document_by_id(collection, id)
        if response.status_code != 200:
            return f"delete failed: {response.data}"
        response = query_document_by_id(collection, id)
        if response.status_code != 404:
            return f"query document after delete failed: {response.data}"

        # clean up
        operations.append("clean up")
        get_database().drop_collection(collection)

        return f"DB test passed: {operations}"
    except Exception as e:
        return f"DB test failed: {e}\n{operations}"


# utility methods
def convert_to_oid(id):
    if type(id) == dict:
        return ObjectId(id["$oid"])
    else:
        return ObjectId(id)
