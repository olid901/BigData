import requests
import json
import sqlite3

if __name__ == '__main__':
    # Database setup
    connection = sqlite3.connect("politicians.db")
    cursor = connection.cursor()
    create_politiocian_table = "CREATE TABLE IF NOT EXISTS politician (" \
                               "id INTEGER UNIQUE PRIMARY KEY," \
                               "first_name STRING," \
                               "last_name STRING," \
                               "birth_name STRING," \
                               " sex STRING," \
                               "year_of_birth INTEGER," \
                               "party_id INTEGER" \
                               ")"
    cursor.execute(create_politiocian_table)

    # Es gibt 30k Politker auf Abgeordnetenwatch, daher range(31) (geht von 0 bis 30)
    for cnt in range(31):
        response = requests.get("https://www.abgeordnetenwatch.de/api/v2/politicians?range_start=%d&range_end=%d"%(cnt*1000, cnt*1000 + 1000))
        json_data = json.loads(response.content.decode("ASCII"))
        for politician in json_data['data']:
            #Format insert Command with id, first name, last name, birth name, sex, party
            insert_command = 'INSERT INTO politician VALUES ({}, \'{}\', \'{}\', {}, \'{}\', {}, {})'.format(
            politician['id'],
            politician['first_name'].replace("'", "''"), #mögl. Apostroph im Namen durch "''" ersetzen, damit der Befehl SQL-konform ist
            politician['last_name'].replace("'", "''"),
            "'"+politician['birth_name'].replace("'", "''")+"'" if politician['birth_name'] is not None else 'null',
            politician['sex'], politician['year_of_birth'] if politician['year_of_birth'] is not None else "null",
            politician['party']['id'] if politician['party'] is not None else "null")
            cursor.execute(insert_command)

    cursor.execute("SELECT * FROM politician")
    all = cursor.fetchall()
    print(all)
