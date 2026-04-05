from pymongo import MongoClient
import bcrypt

MONGO_URI = "mongodb://localhost:27017/"

client = MongoClient(MONGO_URI)
auth_db = client["auth_db"]

def register(username, password):

    if auth_db.users.find_one({"username": username}):
        print("User already exists")
        return None

    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt())

    result = auth_db.users.insert_one({
        "username": username,
        "password": hashed
    })

    print("User created")

    return str(result.inserted_id)


def login(username, password):

    user = auth_db.users.find_one({"username": username})

    if not user:
        print("User not found")
        return None

    if bcrypt.checkpw(password.encode(), user["password"]):
        return str(user["_id"])

    print("Wrong password")
    return None