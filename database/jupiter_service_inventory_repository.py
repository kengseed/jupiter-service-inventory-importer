from mysql.connector import MySQLConnection, Error


def init():
    db = MySQLConnection(
        host="localhost",
        user="root",
        password="P@ssw0rd",
        database="jupiter_service_inventory",
    )
    return db


def addTempJupiterServiceCatalogOpen(list: list):
    try:
        db = init()
        cursor = db.cursor()

        # Logics

        # Commit & Close Connection
        db.commit()
        cursor.close()
        db.close()
    except Error as e:
        print(e)
        raise e
