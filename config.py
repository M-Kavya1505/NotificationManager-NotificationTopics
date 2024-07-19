import firebase_admin
from firebase_admin import credentials
import configparser
 
# Read configuration from the config file
config = configparser.ConfigParser()
config.read(r"./notifier.cfg")
path_to_firebase_app_sdk=config["DEFAULT"]["path_to_firebase_app_sdk"]

# Replace XYZ with the generate private key file path(generated from Firebase console project)
cred = credentials.Certificate(r"./XYZ")

#cred = credentials.Certificate(f"{path_to_firebase_app_sdk}")
try:
    firebase_admin.initialize_app(cred)
except ValueError:
    # Firebase app is already initialized
    pass
 
db_config = {
    'host': config["DATABASE"]["host"],
    'user': config["DATABASE"]["user"],
    'password': config["DATABASE"]["password"],
    'database': config["DATABASE"]["database"]
}
 
