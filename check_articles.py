import sqlite3

# Handy python script to check  if there is a wikipedia article for a politician 
def main():
    connection = sqlite3.connect("politicians.db")
    cursor = connection.cursor()
    
    # We already have (almost) all politicians in our politicians database
    # In this case, we only need the first and last name
    cursor.execute("SELECT DISTINCT first_name, last_name FROM politicians ORDER BY first_name, last_name ASC")
    res = cursor.fetchall()

    politicians = set()

    for first, last in res:
        name = first + ' ' + last
        name = name.replace(' ', '_')
        politicians.add(name)
    
    # We use a list of all wikipedia articles, which can be downloaded here:
    # https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/dewiki/20211001/dewiki-20211001-all-titles-in-ns0.gz
    my_file = open("files/dewiki-20211001-all-titles-in-ns0", "rt", encoding='utf-8')
    list_file = my_file.read().splitlines()
    articles = set(list_file)

    matches = politicians.intersection(articles)

    print("Politicians: " + str(len(politicians)))
    print("Matches: " + str(len(matches)))
    
    file = open("article_list.txt", "w", encoding="utf-8")
    for name in matches:
        file.write(name + "\n")
    file.close()

if __name__ == '__main__':
    main()