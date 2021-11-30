from multiprocessing import Queue

import requests  # Library for parsing HTML
from bs4 import BeautifulSoup
import time
import sqlite3
import subprocess
import xml.sax
from time import time, sleep
from multiprocessing import Process, JoinableQueue
from os import listdir
from os.path import isfile, join

dump_url = "https://dumps.wikimedia.org/dewiki/20211101/"
base_url = "https://dumps.wikimedia.org"


class Contributor:
    def __init__(self, contrib_id, name):
        self.id = contrib_id
        self.name = name


class Revision:
    def __init__(self, revision_id, contributor: Contributor, timestamp, text_len, page_id):
        self.id = revision_id
        self.contributor = contributor
        self.timestamp = timestamp
        self.text_len = text_len
        self.page_id = page_id

    def __str__(self):
        return f" \
        timestamp: {self.timestamp}\n \
        id: {self.id}\n \
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
                 f"  - contrib_id: {r.contributor.id}\n" \
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


class DatabaseHandler:
    def __init__(self):
        self._connection = sqlite3.connect("articles.db", timeout=5)
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

    @staticmethod
    def insert_contributor_command(contributor: Contributor):
        if contributor.id is not None and contributor.name is not None:
            return f"""INSERT OR IGNORE INTO Contributor VALUES({contributor.id}, '{contributor.name.replace("'", "''") if contributor.name else "null"}')"""
        else:
            return None

    @staticmethod
    def insert_page_command(page: HistoryPage):
        # print("Inserting Page: " + page.title)
        return f"INSERT INTO Page VALUES({page.id}, '{page.title}')"

    @staticmethod
    def insert_revision_command(revision: Revision):
        # print("inserting Revision: ", revision.id)
        return f"INSERT OR IGNORE INTO Revision (timestamp, text_len, contributor_id, page_id) VALUES(" \
               f" '{revision.timestamp}'," \
               f" {revision.text_len}," \
               f"{revision.contributor.id if revision.contributor.id else -1}," \
               f" {revision.page_id})"

    def execute_command(self, command):
        # print("executing command: ", command)
        self._cursor.execute(command)


class WikiXmlHandler(xml.sax.handler.ContentHandler):
    """Content handler for Wiki XML data using SAX"""

    def __init__(self, sql_queue: JoinableQueue):
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

        self._sql_queue = sql_queue

        self._performance_skip = False

        # There are noe Thread/Process-safe global variables in python, so every parser needs to know this
        my_file = open("article_list.txt", "rt", encoding='utf-8')
        self._politician_list = my_file.read().splitlines()

    def characters(self, content):
        """Characters between opening and closing tags"""

        # I AM SPEED: If an article is surely not a politician (does NOT imply it is a politician!), dont write anything to buffers
        if self._in_tag == "page" and self._current_tag == "title" and content.strip().replace("\n", "") != "":
            if content.replace(" ", "_").strip() not in self._politician_list:
                print("This is not a politician: ", content)
                self._performance_skip = True
            else:
                self._performance_skip = False

        # We can skip the page if the title doesnt fit to any politician
        if not self._performance_skip:

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
            self._performance_skip = False

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
        if not self._performance_skip:
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
                    self._contributor_values['id'].strip().replace("\n",
                                                                   "") if 'id' in self._contributor_values else None,
                    self._contributor_values['username'].strip().replace("\n",
                                                                         "") if 'id' in self._contributor_values else None
                )
                self._contributor_values = {}
                # DB_handler.insert_contributor(self._contributor_object_buffer)

            if name == 'revision':
                self._in_tag = "page"
                self._revisions_object_buffer.append(
                    Revision(
                        self._revision_values['id'],
                        self._contributor_object_buffer,
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

                if is_politician(self.HistoryPages[-1]):
                    print("Found a politician!")
                    print(self.HistoryPages[-1].title)
                    for rev in self.HistoryPages[-1].revisions:
                        self._sql_queue.put(DatabaseHandler.insert_contributor_command(rev.contributor))
                        self._sql_queue.put(DatabaseHandler.insert_revision_command(rev))
                    self._sql_queue.put(DatabaseHandler.insert_page_command(self.HistoryPages[-1]))
                else:
                    print("No politician (", self.HistoryPages[-1].title, ")")


# Worker process method for concurrent XML parsing
def dump_wikipedia_worker(queue: JoinableQueue, sql_queue: Queue):
    while not queue.empty():

        # Setting a timeout as a security measure
        # If I understood the docs correctly, there is a possibility that empty() returns something
        # wrong, therefore freezing get(), because its waiting indefinitely until it gets something new
        filename = queue.get(timeout=5)
        print("Got: " + filename)

        # Content handler for Wiki XML
        handler = WikiXmlHandler(sql_queue)

        # Parsing object
        parser = xml.sax.make_parser()
        parser.setContentHandler(handler)

        for i, line in enumerate(
                subprocess.Popen(['bzcat'], stdin=open(filename), stdout=subprocess.PIPE).stdout):
            parser.feed(line)

        queue.task_done()


def end_program_when_done(file_queue: JoinableQueue, sql_queue: Queue, db_handler: DatabaseHandler):
    # Wait for every element in the queue to be finished
    file_queue.join()



# TODO: Make this more failsafe, maybe by looking at the text: Does ist contain or Link "Politiker"
def is_politician(page: HistoryPage):
    my_file = open("article_list.txt", "rt", encoding='utf-8')
    politician_list = my_file.read().splitlines()
    if page.title.replace(" ", "_").strip() in politician_list:
        return True
    else:
        return False


# Code to get all .bz2 files
'''
# Retrieve the html
dump_html = requests.get(dump_url).text  # Convert to a soup
soup_dump = BeautifulSoup(dump_html, 'html.parser')  # Find list elements with the class file
bz2_files = [x.get('href') for x in soup_dump.find_all('a') if
             "dewiki-20211101-pages-meta-history" in x.get('href') and x.get('href')[-4:] == ".bz2"]
'''


# According to SQLite3 docs, the library is capable of multiprocessing,
# which means, we share it between all processes
# TODO: Wäre es ggf. schöner, wenn wir nicht einfach so auf des Ding aus dem nichts zugreifen
#       sondern den vlt. der Methode mit übergeben? -> Prüfen, ob das Probleme macht

# Main Method to cover Scope of Variables in here
def main():
    DB_handler = DatabaseHandler()
    DB_handler.create_database_tables()

    file_queue = JoinableQueue(maxsize=0)
    sql_queue = Queue(maxsize=0)
    # TODO: Change number of processes based on user dialog or command argument
    num_threads = 4

    file_path = "./files/"
    file_list = ["./files/" + f for f in listdir(file_path) if isfile(join(file_path, f))]

    for file in file_list:
        file_queue.put(file)

    # For Debugging
    #file_queue.put("files/dewiki-20211001-pages-meta-history1.xml-p1p1598.bz2")

    for i in range(num_threads):
        worker = Process(target=dump_wikipedia_worker, args=(file_queue, sql_queue,))
        worker.start()

    # This needs to be a separate process, as file_queue.join() would block main Thread and we need to execute the sql commands here
    end_script_worker = Process(target=end_program_when_done, args=(file_queue, sql_queue, DB_handler))
    end_script_worker.start()

    # Check for sql Queries ind Queue and execute
    while True:
        if not sql_queue.empty():
            command = sql_queue.get(timeout=5)
            if command is not None:
                DB_handler.execute_command(command)
        else:
            if not end_script_worker.is_alive():
                print("c...")
                DB_handler.commit()
                print("c!")
                exit(1)

    # TODO: This part is broken currently, find out how to fix or delete
    # for x in handler.HistoryPages:
    #     if x.title in politician_list or x.title.replace(" ", "_") in politician_list or x.title.replace(" ", "_").strip() in politician_list or x.title.replace("(Politiker)","").replace(" ", "_").strip() in politician_list:
    #         print(x.title)


if __name__ == "__main__":
    start = time()
    main()
    finish = time()
    print("Duration:"+str((finish - start)//60)+":"+str((start-finish)%60)+"min")
