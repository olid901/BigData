import requests  # Library for parsing HTML
from bs4 import BeautifulSoup
import time
import sqlite3
import subprocess
import xml.sax

dump_url = "https://dumps.wikimedia.org/dewiki/20211101/"
base_url = "https://dumps.wikimedia.org"


class DatabaseHandler:
    def __init__(self):
        self._connection = sqlite3.connect("articles.db")
        self._cursor = self._connection.cursor()

    def commit(self):
        self._connection.commit()

    def create_database_tables(self):
        self._cursor.execute("DROP TABLE IF EXISTS Contributor")
        self._cursor.execute("DROP TABLE IF EXISTS Page")
        self._cursor.execute("DROP TABLE IF EXISTS Revision")
        self._cursor.execute("CREATE TABLE IF NOT EXISTS Contributor (id INTEGER PRIMARY KEY, name STRING)")
        self._cursor.execute("INSERT INTO Contributor VALUES(-1, null)")
        self._cursor.execute("CREATE TABLE IF NOT EXISTS Page ("
                             "id INTEGER UNIQUE PRIMARY KEY,"
                             "title STRING)")
        self._cursor.execute("CREATE TABLE IF NOT EXISTS Revision ("
                             "id INTEGER UNIQUE PRIMARY KEY,"
                             "timestamp TIMESTAMP,"
                             "text_len INTEGER,"
                             "contributor_id INTEGER,"
                             "page_id INTEGER,"
                             "FOREIGN KEY (contributor_id) REFERENCES Contributor(id),"
                             "FOREIGN KEY (page_id) REFERENCES Page(id))")

    def insert_contributor(self, contributor):
        if contributor.id is not None and contributor.name is not None:
            self._cursor.execute(
                f"""INSERT OR IGNORE INTO Contributor VALUES({contributor.id}, '{contributor.name.replace("'", "''") if contributor.name else "null"}')""")

    def insert_page(self, page):
        self._cursor.execute(f"INSERT INTO Page VALUES({page.id}, '{page.title}')")

    def insert_revision(self, revision):
        self._cursor.execute(f"INSERT OR IGNORE INTO Revision (timestamp, text_len, contributor_id, page_id) VALUES("
                             f" '{revision.timestamp}',"
                             f" {revision.text_len},"
                             f"{revision.contributor_id if revision.contributor_id else -1},"
                             f" {revision.page_id})")


class Contributor:
    def __init__(self, contrib_id, name):
        self.id = contrib_id
        self.name = name


class Revision:
    def __init__(self, revision_id, contributor_id, timestamp, text_len, page_id):
        self.id = revision_id
        self.contributor_id = contributor_id
        self.timestamp = timestamp
        self.text_len = text_len
        self.page_id = page_id

    def __str__(self):
        return f" \
        timestamp: {self.timestamp}\n \
        id: {self.contributor_id}\n \
        text_len: {self.text_len}\n"


class HistoryPage:

    def __init__(self, page_id, title, revisions, text):
        self.text = text
        self.revisions = revisions
        self.title = title
        self.id = page_id

    def __str__(self):
        s = f" \
        title: {self.title}\n \
        id: {self.id}\n \
        revisions: \n"

        for r in self.revisions:
            s += "revision: \n" \
                 f"  - contrib_id: {r.contributor_id}\n" \
                 f"  - timestamp: {r.timestamp}\n" \
                 f"  - text_len: {r.text_len}\n"

        return s


''' Tree Diagram:

        n pages ----------------> n revisions
        |---> id                    |---> id
        |---> title                 |---> text_len (after edit)
                                    |---> timestamp
                                    |---> contributor
                                            |---> id
                                            |---> name
'''


class WikiXmlHandler(xml.sax.handler.ContentHandler):
    """Content handler for Wiki XML data using SAX"""

    def __init__(self):
        xml.sax.handler.ContentHandler.__init__(self)
        self._in_tag = None
        # buffers (storing all data from needed tags)
        self._page_buffer = []
        self._revision_buffer = []
        self._contributor_buffer = []
        # values (storing all data fields that are needed for a tag)
        self._page_values = {}
        self._contributor_values = {}
        self._revision_values = {}
        # current tag
        self._current_tag = None

        self._contributor_object_buffer = None
        self._revisions_object_buffer = []
        self._latest_text = None
        # actual public Data
        self.HistoryPages = []

    def characters(self, content):
        """Characters between opening and closing tags"""
        if self._in_tag == "page":
            if self._current_tag:
                self._page_buffer.append(content)
        elif self._in_tag == "revision":
            if self._current_tag:
                if self._current_tag == "timestamp" and content.strip() != "":
                    self._revision_buffer.append(content)
                else:
                    self._revision_buffer.append(content)
        elif self._in_tag == "contributor":
            if self._current_tag:
                self._contributor_buffer.append(content)

    def startElement(self, name, attrs):
        """Opening tag of element"""

        if name == "page":
            self._page_buffer = []
            self._in_tag = "page"

        if name == "revision":
            self._in_tag = "revision"
            self._revision_buffer = []

        if name == "contributor":
            self._in_tag = "contributor"
            self._contributor_buffer = []

        if name in ('title', 'text', 'timestamp', 'id', 'contributor', 'username'):
            self._current_tag = name
        else:
            self._current_tag = None

    def endElement(self, name):
        """Closing tag of element"""
        if name == self._current_tag:
            if self._in_tag == "contributor":
                self._contributor_values[name] = ' '.join(self._contributor_buffer)
                self._contributor_buffer = []

            elif self._in_tag == "revision":
                self._revision_values[name] = ' '.join(self._revision_buffer)
                self._revision_buffer = []

            elif self._in_tag == "page":
                self._page_values[name] = ' '.join(self._page_buffer)
                self._page_buffer = []

        if name == 'contributor':
            self._in_tag = "revision"
            self._contributor_object_buffer = Contributor(
                self._contributor_values['id'].strip().replace("\n", "") if 'id' in self._contributor_values else None,
                self._contributor_values['username'].strip().replace("\n",
                                                                     "") if 'id' in self._contributor_values else None
            )
            self._contributor_values = {}
            #DB_handler.insert_contributor(self._contributor_object_buffer)

        if name == 'revision':
            self._in_tag = "page"
            self._revisions_object_buffer.append(
                Revision(
                    self._revision_values['id'],
                    self._contributor_object_buffer.id,
                    self._revision_values['timestamp'].strip().replace("\n", "").replace("Z", ""),
                    len(self._revision_values['text']),
                    self._page_values['id'])
            )
            self._latest_text = self._revision_values['text']

        if name == 'page':
            self._in_tag = None
            self.HistoryPages.append(
                HistoryPage(self._page_values['id'], self._page_values['title'], self._revisions_object_buffer,
                            self._latest_text)
            )
            self._page_values = {}

            #DB_handler.insert_page(self.HistoryPages[-1])
            #for revision in self.HistoryPages[-1].revisions:
            #    DB_handler.insert_revision(revision)

            if self.HistoryPages[-1].title.replace(" ", "_").strip() in politician_list:
                print("Found a politician!")
                print(self.HistoryPages[-1].title)
            else:
                print("No politician (",self.HistoryPages[-1].title,")")



# Code to get all .bz2 files
'''
# Retrieve the html
dump_html = requests.get(dump_url).text  # Convert to a soup
soup_dump = BeautifulSoup(dump_html, 'html.parser')  # Find list elements with the class file
bz2_files = [x.get('href') for x in soup_dump.find_all('a') if
             "dewiki-20211101-pages-meta-history" in x.get('href') and x.get('href')[-4:] == ".bz2"]
'''

# Content handler for Wiki XML
handler = WikiXmlHandler()

DB_handler = DatabaseHandler()
DB_handler.create_database_tables()

# Parsing object
parser = xml.sax.make_parser()
parser.setContentHandler(handler)

my_file = open("article_list.txt", "rt", encoding='utf-8')
politician_list = my_file.read().splitlines()

print(politician_list)

for i, line in enumerate(
        subprocess.Popen(['bzcat'], stdin=open("files/dewiki-20211001-pages-meta-history1.xml-p1p1598.bz2"),
                         stdout=subprocess.PIPE).stdout):
    parser.feed(line)

    #if i > 100000:
    #    break

DB_handler.commit()

for x in handler.HistoryPages:
    if x.title in politician_list or x.title.replace(" ", "_") in politician_list or x.title.replace(" ", "_").strip() in politician_list or x.title.replace("(Politiker)","").replace(" ", "_").strip() in politician_list:
        print(x.title)
