import mysql.connector
from mysql.connector import errorcode

config = {
    "user": "movies_user",
    "password": "popcorn",
    "host": "127.0.0.1",
    "database": "movies",
    "raise_on_warnings": True
}

try: 
    db = mysql.connector.connect(**config)
    print("\n database user {} connected to mysql on host {} with database {}".format(config{"user"}, config{"host"}, config{"database"}))
    input("\n\n press any key to cont...")
except mysql.connector.Error as err:
    print(err)
finally:
    db.close()