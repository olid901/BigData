import sqlite3
import subprocess
import xml.sax
from time import time, sleep
from multiprocessing import Queue, Process, JoinableQueue
from os import listdir
from os.path import isfile, join
import json


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

    def __init__(self, page_id, title, politician_id, revisions, text):
        self.text = text
        self.revisions = revisions
        self.title = title
        self.id = page_id
        self.politician_id = politician_id

    def __str__(self):
        s = f" \
        title: {self.title}\n \
        id: {self.id}\n \
        politician_id: {self.politician_id}\n \
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
        self._connection = sqlite3.connect("politicians.db", timeout=5)
        self._cursor = self._connection.cursor()

    def commit(self):
        self._connection.commit()

    def create_database_tables(self):
        self._cursor.execute("DROP TABLE IF EXISTS Contributor")
        self._cursor.execute("DROP TABLE IF EXISTS Page")
        self._cursor.execute("DROP TABLE IF EXISTS Revision")
        self._cursor.execute("CREATE TABLE IF NOT EXISTS Contributor (id INTEGER PRIMARY KEY, name STRING)")
        self._cursor.execute("CREATE TABLE IF NOT EXISTS Page ("
                             "id INTEGER UNIQUE PRIMARY KEY,"
                             "title STRING,"
                             "politician_id INTEGER,"
                             "FOREIGN KEY (politician_id) REFERENCES politicians(id))")
        self._cursor.execute("CREATE TABLE IF NOT EXISTS Revision ("
                             "id INTEGER UNIQUE PRIMARY KEY,"
                             "timestamp TIMESTAMP,"
                             "text_len INTEGER,"
                             "contributor_id INTEGER,"
                             "page_id INTEGER,"
                             "FOREIGN KEY (contributor_id) REFERENCES Contributor(id),"
                             "FOREIGN KEY (page_id) REFERENCES Page(id))")

    # !!! Can return None when contributor only had an ip-Address in xml and no username or id
    @staticmethod
    def insert_contributor_command(contributor: Contributor):
        if contributor.id is not None and contributor.name is not None:
            # replace: escape "'" in SQL-Query
            return f"""INSERT OR IGNORE INTO Contributor VALUES({contributor.id}, '{contributor.name.replace("'", "''")}') """
        else:
            return None

    @staticmethod
    def insert_page_command(page: HistoryPage):
        return f"INSERT INTO Page VALUES({page.id}, '{page.title}', {page.politician_id})"

    @staticmethod
    def insert_revision_command(revision: Revision):
        return f"INSERT INTO Revision (timestamp, text_len, contributor_id, page_id) VALUES(" \
               f" '{revision.timestamp}'," \
               f" {revision.text_len}," \
               f"{revision.contributor.id if revision.contributor.id else 'null'}," \
               f" {revision.page_id})"

    def execute_command(self, command):
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

        # buffers for page and contributor
        self._contributor_object_buffer = None
        self._HistoryPage_object_buffer = None

        # Buffer List for all Revisions of a page
        self._revisions_object_buffer = []
        # We don't save the text of the revisions to the db, so we don't save them to buffer objects either. But we
        # need to keep track of the text from text last processed revision, as we save that to the page.
        self._latest_text = None

        # Queue to put sql Queries in
        self._sql_queue = sql_queue

        # Page is surely not a politician: don't write anything to buffers, as we don't save anything about the page to the db.
        # Performance-Boost: ~25%
        self._performance_skip = False

        # Afaik there are no Thread/Process-safe global variables in python, so every parser needs to know this
        with open('article_list.json') as json_file:
            self._politician_dict = json.load(json_file)

    def characters(self, content):
        """Characters between opening and closing tags"""

        # I AM SPEED: If an article is surely not a politician (does NOT imply it is a politician!), dont write
        # anything to buffers when processing the page
        # Politician Check Stage 1: This only needs the title of the currently processed article
        if self._in_tag == "page" and self._current_tag == "title" and content.strip().replace("\n", "") != "":
            if content.replace(" ", "_").strip() not in self._politician_dict:
                self._performance_skip = True
            else:
                print("This could be a politician: ", content)
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
                self._revision_values = {}

            if name == 'page':
                self._in_tag = None
                self._HistoryPage_object_buffer = HistoryPage(self._page_values['id'], self._page_values['title'],
                                                              self._politician_dict[
                                                           self._page_values['title'].replace(" ", "_")],
                                                              self._revisions_object_buffer,
                                                              self._latest_text)

                self._page_values = {}
                self._revisions_object_buffer = []

                if is_politician(self._HistoryPage_object_buffer):
                    self._sql_queue.put(DatabaseHandler.insert_page_command(self._HistoryPage_object_buffer))
                    for rev in self._HistoryPage_object_buffer.revisions:
                        self._sql_queue.put(DatabaseHandler.insert_contributor_command(rev.contributor))
                        self._sql_queue.put(DatabaseHandler.insert_revision_command(rev))
                self._HistoryPage_object_buffer = None


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


# We check if an article belongs to a politician in two ways.
# 1. The title of the page must match the name of a politician from the database
# 2. The Page must link [[Politiker]] somewhere in the text
# The 1st step is already done in the parser. The 2nd step is here:
def is_politician(page: HistoryPage):
    if "[[Politiker]]" in page.text:
        print("This is in fact a politician: ", page.title)
        return True
    else:
        print("This only seemed to be a politician: ", page.title)
        return False


# According to SQLite3 docs, the library is capable of multiprocessing,
# which means, we share it between all processes

# Main Method to cover Scope of Variables in here
def main():
    db_handler = DatabaseHandler()
    db_handler.create_database_tables()

    file_queue = JoinableQueue(maxsize=0)
    sql_queue = Queue(maxsize=0)
    # TODO: Change number of processes based on user dialog or command argument
    num_threads = 1

    #file_path = "./files/"
    #file_list = ["./files/" + f for f in listdir(file_path) if isfile(join(file_path, f))]

    #for file in file_list:
    #    file_queue.put(file)

    # For Debugging
    file_queue.put("files/dewiki-20211001-pages-meta-history1.xml-p1p1598.bz2")

    for i in range(num_threads):
        worker = Process(target=dump_wikipedia_worker, args=(file_queue, sql_queue,))
        worker.start()

    # This needs to be a separate process, as file_queue.join() would block main Thread and we need to execute the
    # sql commands here
    end_script_worker = Process(target=end_program_when_done, args=(file_queue, sql_queue, db_handler))
    end_script_worker.start()

    # Check for sql Queries in Queue and execute
    while True:
        if not sql_queue.empty():
            command = sql_queue.get(timeout=5)
            if command is not None:
                db_handler.execute_command(command)
        else:
            if not end_script_worker.is_alive():
                print("committing...")
                db_handler.commit()
                print("committed!")
                break


if __name__ == "__main__":
    start = time()
    main()
    finish = time()
    print("Duration: " + str(int((finish - start) // 3600)) + ":" + str(int((finish - start) % 60)) + ":" + str(int((start - finish) % 60)) + "s")
