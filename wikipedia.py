import requests  # Library for parsing HTML
from bs4 import BeautifulSoup
import time

dump_url = "https://dumps.wikimedia.org/dewiki/20211101/"
base_url = "https://dumps.wikimedia.org"
import subprocess
import xml.sax


class Revision:
    def __init__(self, revision_id, contributor_id, timestamp, text_len):
        self.id = revision_id
        self.contributor_id = contributor_id
        self.timestamp = timestamp
        self.text_len = text_len
    def __str__(self):
        return f" \
        timestamp: {self.timestamp}\n \
        id: {self.contributor_id}\n \
        text_len: {self.text_len}\n"

class HistoryPage:

    def __init__(self, page_id, title, revisions):
        self.revisions = revisions
        self.title = title
        self.id = page_id

    def __str__(self):
        s = f" \
        title: {self.title}\n \
        id: {self.id}\n \
        revisions: \n"

        for r in self.revisions:
            s += "revision: \n"\
                 f"  - contrib_id: {r.contributor_id}\n" \
                 f"  - timestamp: {r.timestamp}\n" \
                 f"  - text_len: {r.text_len}\n"

        return s


''' Tree Diagram:

        page ----------------> n revisions
        |---> id                |---> id
        |---> title             |---> text_len (after edit)
                                |---> contributor
                                       |---> id
'''


# known Problems: Usernames of contributors get saved together with the contrib_id, timestamp has actual time and random numbers in it
# TODO: Mein Kopf tut weh
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

        self._contributor_id_buffer = None
        self._revisions_object_buffer = []
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

        if name in ('title', 'text', 'timestamp', 'id', 'contributor'):
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
            self._contributor_id_buffer = self._contributor_values['id'].strip().replace("\n", "") if 'id' in self._contributor_values else None
            self._contributor_values = {}

        if name == 'revision':
            self._in_tag = "page"
            self._revisions_object_buffer.append(
                Revision(self._revision_values['id'], self._contributor_id_buffer, self._revision_values['timestamp'].strip().replace("\n", ""),
                         len(self._revision_values['text'])))

        if name == 'page':
            self._in_tag = None
            self.HistoryPages.append(
                HistoryPage(self._page_values['id'], self._page_values['title'], self._revisions_object_buffer))
            self._page_values = {}


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

# Parsing object
parser = xml.sax.make_parser()
parser.setContentHandler(handler)

for i, line in enumerate(
        subprocess.Popen(['bzcat'], stdin=open("files/dewiki-20211001-pages-meta-history1.xml-p1p1598.bz2"),
                         stdout=subprocess.PIPE).stdout):

    parser.feed(line)

    #if i > 1000:
        #break
for x in handler.HistoryPages:
    print(x.title)


