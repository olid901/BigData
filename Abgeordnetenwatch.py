import requests
import json
import sqlite3
import time


def get_parliaments_to_database(cursor):
    create_parliaments_table = "CREATE TABLE IF NOT EXISTS parliaments (" \
                               "id INTEGER UNIQUE PRIMARY KEY," \
                               "federal_state STRING" \
                               ")"
    cursor.execute(create_parliaments_table)
    response = requests.get("https://www.abgeordnetenwatch.de/api/v2/parliaments")
    json_data = json.loads(response.content.decode("ASCII"))
    for parliament in json_data['data']:
        insert_command = f"INSERT INTO parliaments VALUES (%d, '%s')" % (parliament['id'], parliament['label'])
        print(insert_command)
        cursor.execute(insert_command)


def get_parliament_periods_to_database(cursor):
    create_parliament_periods_table = "CREATE TABLE IF NOT EXISTS parliament_periods (" \
                                      "id INTEGER UNIQUE PRIMARY KEY," \
                                      "parliament_id INTEGER," \
                                      "label STRING," \
                                      "election_date DATE," \
                                      "start_date_period DATE," \
                                      "end_date_period DATE," \
                                      "FOREIGN KEY (parliament_id) REFERENCES parliaments(id)" \
                                      ")"
    cursor.execute(create_parliament_periods_table)
    response = requests.get("https://www.abgeordnetenwatch.de/api/v2/parliament-periods?page=0&pager_limit=1000")
    json_data = json.loads(response.content.decode("ASCII"))
    for period in json_data['data']:
        insert_command = "INSERT INTO parliament_periods VALUES ({}, {}, '{}', {}, {}, {})".format(
            period['id'],
            period['parliament']['id'],
            period['label'],
            "'" + period['election_date'].replace("'", "''") + "'" if period['election_date'] is not None else 'null',
            "'" + period['start_date_period'].replace("'", "''") + "'" if period[
                                                                              'start_date_period'] is not None else 'null',
            "'" + period['end_date_period'].replace("'", "''") + "'" if period[
                                                                            'end_date_period'] is not None else 'null'
        )
        print(insert_command)
        cursor.execute(insert_command)


def get_mandates_and_candidacies_to_database(cursor):
    create_mandates_table = "CREATE TABLE IF NOT EXISTS mandates (" \
                            "id INTEGER UNIQUE PRIMARY KEY," \
                            "parliament_period_id INTEGER," \
                            "politician_id INTEGER," \
                            "start_date DATE," \
                            "end_date DATE," \
                            "FOREIGN KEY (parliament_period_id) REFERENCES parliament_periods(id)," \
                            "FOREIGN KEY (politician_id) REFERENCES politician(id)" \
                            ")"
    #cursor.execute("DROP TABLE mandates")
    cursor.execute(create_mandates_table)

    create_candidacies_table = "CREATE TABLE IF NOT EXISTS candidacies (" \
                               "id INTEGER UNIQUE PRIMARY KEY," \
                               "parliament_period_id INTEGER," \
                               "politician_id INTEGER," \
                               "party_id INTEGER," \
                               "start_date DATE," \
                               "end_date DATE," \
                               "FOREIGN KEY (parliament_period_id) REFERENCES parliament_periods(id)," \
                               "FOREIGN KEY (politician_id) REFERENCES politician(id)," \
                               "FOREIGN KEY (party_id) REFERENCES parties(id)" \
                               ")"
    #cursor.execute("DROP TABLE candidacies")
    cursor.execute(create_candidacies_table)

    # Es gibt ~53k Mandate und Kandidaturen, daher range (55)
    for page in range(55):
        url = f"https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates?page=%d&pager_limit=1000" % page
        #time.sleep(31)
        print("page:%d" % page)
        response = requests.get(url)
        json_data = json.loads(response.content.decode("ASCII"))
        for candidacy_or_mandate in json_data['data']:
            insert_command = ""
            if candidacy_or_mandate['type'] == "mandate":
                if 'fraction_membership' not in candidacy_or_mandate:
                    fraktion = 'fraktionslos'
                else:
                    fraktion = candidacy_or_mandate['fraction_membership'][0]['label'].lower()
                if fraktion != 'fraktionslos':
                    cursor.execute(f"SELECT id FROM parties WHERE '%s' like lower(parties.short_name) || '%s'" % (
                    fraktion.lower(), "%"))
                    res = cursor.fetchall()
                    # Fehlerfall: Scheiß Daten, bspw 'Grüne' bei Fraktion, anstatt dem richtigen 'Bündnis 90/Die Grünen'
                    if res == []:
                        fraktion = 'null'
                    else:
                        fraktion = res[0][0]
                else:
                    fraktion = 'null'
                insert_command = "INSERT INTO mandates VALUES({}, {}, {}, {}, {} )".format(
                    candidacy_or_mandate['id'],
                    candidacy_or_mandate['parliament_period']['id'],
                    fraktion,
                    "'" + candidacy_or_mandate['start_date'] + "'" if candidacy_or_mandate[
                                                                          'start_date'] is not None else 'null',
                    "'" + candidacy_or_mandate['end_date'] + "'" if candidacy_or_mandate[
                                                                        'end_date'] is not None else 'null',
                )

            if candidacy_or_mandate['type'] == 'candidacy':
                insert_command = "INSERT INTO candidacies VALUES({}, {}, {}, {}, {}, {})".format(
                    candidacy_or_mandate['id'],
                    candidacy_or_mandate['parliament_period']['id'],
                    candidacy_or_mandate['politician']['id'],
                    candidacy_or_mandate['party']['id'] if candidacy_or_mandate['party'] is not None else 'null',
                    candidacy_or_mandate['start_date'] if candidacy_or_mandate['start_date'] is not None else 'null',
                    candidacy_or_mandate['end_date'] if candidacy_or_mandate['end_date'] is not None else 'null',
                )

            cursor.execute(insert_command)

    cursor.execute("SELECT COUNT(*) from candidacies")
    res = cursor.fetchall()
    print("candidacies: " + str(res))
    cursor.execute("SELECT COUNT(*) from mandates")
    res = cursor.fetchall()
    print("mandates: " + str(res))


def get_parties_to_database(cursor):
    create_parties_table = "CREATE TABLE IF NOT EXISTS parties (" \
                           "id INTEGER UNIQUE PRIMARY KEY," \
                           "full_name STRING," \
                           "short_name" \
                           ")"
    cursor.execute(create_parties_table)
    response = requests.get("https://www.abgeordnetenwatch.de/api/v2/parties?page=0&pager_limit=1000")
    json_data = json.loads(response.content.decode("ASCII"))
    for party in json_data['data']:
        cursor.execute(
            f"INSERT INTO parties VALUES (%d, '%s', '%s')" % (party['id'], party['full_name'], party['short_name']))


def get_politicians_to_database(cursor):
    create_politiocian_table = "CREATE TABLE IF NOT EXISTS politicians (" \
                               "id INTEGER UNIQUE PRIMARY KEY," \
                               "first_name STRING," \
                               "last_name STRING," \
                               "birth_name STRING," \
                               "sex STRING," \
                               "year_of_birth INTEGER," \
                               "party_id INTEGER," \
                               "FOREIGN KEY (party_id) REFERENCES parties(id)" \
                               ")"
    cursor.execute(create_politiocian_table)

    # Es gibt 30k Politker auf Abgeordnetenwatch, daher range(31) (geht von 0 bis 30)
    for page in range(31):
        url = f"https://www.abgeordnetenwatch.de/api/v2/politicians?page=%d&pager_limit=1000" % page
        response = requests.get(url)
        json_data = json.loads(response.content.decode("ASCII"))
        print(len(json_data['data']))
        for politician in json_data['data']:

            # Schlechte Daten rausfiltern: Es gibt einen Politiker, der überall außer bei der Partei null als Wert hat
            if politician['label'] is None or politician['label'] == "" or politician['label'] == " ":
                continue

            # Format insert Command with id, first name, last name, birth name, sex, party
            insert_command = 'INSERT INTO politicians VALUES ({}, \'{}\', \'{}\', {}, \'{}\', {}, {})'.format(
                politician['id'],
                # mögl. Apostroph im Namen durch "''" ersetzen, damit der Befehl SQL-konform ist
                politician['first_name'].replace("'", "''"),
                politician['last_name'].replace("'", "''"),
                "'" + politician['birth_name'].replace("'", "''") + "'" if politician[
                                                                               'birth_name'] is not None else 'null',
                politician['sex'], politician['year_of_birth'] if politician['year_of_birth'] is not None else "null",
                politician['party']['id'] if politician['party'] is not None else "null")
            cursor.execute(insert_command)


def main():
    connection = sqlite3.connect("politicians.db")
    cursor = connection.cursor()
    get_parliaments_to_database(cursor)
    get_parliament_periods_to_database(cursor)
    get_parties_to_database(cursor)
    get_politicians_to_database(cursor)
    get_mandates_and_candidacies_to_database(cursor)
    # write changes to DB
    connection.commit()


if __name__ == '__main__':
    main()
