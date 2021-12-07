import sqlite3
import json

# Handy python script to check  if there is a wikipedia article for a politician


def main():
    connection = sqlite3.connect("politicians.db")
    cursor = connection.cursor()

    # We already have (almost) all politicians in our politicians database
    # In this case, we only need the first and last name
    cursor.execute("SELECT DISTINCT id, first_name, last_name FROM politicians ORDER BY first_name, last_name ASC")
    res = cursor.fetchall()

    politicians = dict()

    for id, first, last in res:
        politicians[id] = str(first + ' ' + last).replace(' ', '_')

    # We use a list of all wikipedia articles, which can be downloaded here:
    # https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/dewiki/20211001/dewiki-20211001-all-titles-in-ns0.gz
    my_file = open("files/dewiki-20211001-all-titles-in-ns0", "rt", encoding='utf-8')
    list_file = my_file.read().splitlines()
    articles = set(list_file)

    matches = {int(key): val for key, val in politicians.items() if val in articles}

    print("Politicians: " + str(len(politicians)))
    print("Matches: " + str(len(matches)))

    # in rev_multidict werden alle Einträge gespeichert, deren values (Vor und Nachname) in Matches mehrmals vorkommt
    rev_multidict = {}
    for key, value in matches.items():
        rev_multidict.setdefault(value, set()).add(key)
    dup_list = [value for key, value in rev_multidict.items() if len(value) > 1]

    # Problem: There are politicians with the same Name but (obviously) different IDs.
    # It cant be determined which politician the wikipedia page refers to, there it's impossible to know which politicianID to map to the article.
    # So it is better to remove them and have consistent Data, where every wikipedia page refers to one politicianID
    for id_set in dup_list:
        for id in id_set:
            if id in matches:
                del matches[id]

    print("Politicians after duplicate removal: ", len(matches))

    # Flip key and values: We need the Name as key, as we will never search this json by a politicianID, but in by name
    flipped = dict((v, k) for k, v in matches.items())

    with open("article_list.json", "w", encoding="utf-8") as json_file:
        json.dump(flipped, json_file)


if __name__ == '__main__':
    main()
