import psycopg2

def get_connection():
    return psycopg2.connect(
        host='35.208.37.65',
        dbname='dbmupvjj30fiep',
        user='u3jxzcgl1vira',
        password='gj3nxiotwqvm',
        port=5432
    )