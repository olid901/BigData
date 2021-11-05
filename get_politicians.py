import requests
import json
import sqlite3

if __name__ == '__main__':
    # Database setup
    connection = sqlite3.connect("politicians.db")
    cursor = connection.cursor()
    cursor.execute("DROP TABLE politician")
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
    for page in range(31):
        url = f"https://www.abgeordnetenwatch.de/api/v2/politicians?page=%d&pager_limit=1000" % page
        print(url)
        response = requests.get(url)
        json_data = json.loads(response.content.decode("ASCII"))
        print(len(json_data['data']))
        for politician in json_data['data']:

            # Schlechte Daten rausfiltern: Es gibt eine Politiker, der überall außer bei der Partei null als Wert hat
            if politician['label'] is None or politician['label'] == "" or politician['label'] == " ":
                continue

            # Format insert Command with id, first name, last name, birth name, sex, party
            insert_command = 'INSERT INTO politician VALUES ({}, \'{}\', \'{}\', {}, \'{}\', {}, {})'.format(
                politician['id'],
                # mögl. Apostroph im Namen durch "''" ersetzen, damit der Befehl SQL-konform ist
                politician['first_name'].replace("'", "''"),
                politician['last_name'].replace("'", "''"),
                "'"+politician['birth_name'].replace("'", "''")+"'" if politician['birth_name'] is not None else 'null',
                politician['sex'], politician['year_of_birth'] if politician['year_of_birth'] is not None else "null",
                politician['party']['id'] if politician['party'] is not None else "null")
            cursor.execute(insert_command)

    #Kurz gucken ob die Größe der Tabelle stimmt, sollten ~29k sein
    cursor.execute("SELECT COUNT(*) FROM politician")
    all = cursor.fetchall()
    print(all)
    # Changes an der Datenbank commiten, damit sie gespeichert werden
    connection.commit()
