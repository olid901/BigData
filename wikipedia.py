import requests  # Library for parsing HTML
from bs4 import BeautifulSoup

dump_url = "https://dumps.wikimedia.org/dewiki/20211101/"
base_url = "https://dumps.wikimedia.org"
import subprocess
import xml.sax


class Revision:
    def __init__(self, contributor_id, timestamp, text_len):
        self.contributor_id = contributor_id
        self.timestamp = timestamp
        self.text_len = text_len


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
            s += f"  - contrib_id: {r.contributor_id}\n" \
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


# TODO: Refactor: Use A String to Indicate the current position in the Tree Diagram instead of booleans

# TODO: Mein Kopf tut weh
class WikiXmlHandler(xml.sax.handler.ContentHandler):
    """Content handler for Wiki XML data using SAX"""

    def __init__(self):
        xml.sax.handler.ContentHandler.__init__(self)
        self._page_buffer = None
        self._revision_buffer = None
        self._contributor_buffer = None
        self._page_values = {}
        self._contributor_values = {}
        self._revision_values = {}
        self._current_tag = None
        self._in_page = False
        self._in_revision = False
        self._in_contributor = False

        self._contributor_id_buffer = None
        self._revisions_object_buffer = []
        # actual public Data
        self.HistoryPages = []

    def characters(self, content):
        """Characters between opening and closing tags"""
        if self._in_page and not self._in_revision and not self._in_contributor:
            if self._current_tag:
                self._page_buffer.append(content)
        if self._in_revision and not self._in_contributor:
            if self._current_tag:
                # print("revision_content: ", content)
                self._revision_buffer.append(content)
        if self._in_contributor:
            # print("Contributor content: ", content)
            if self._current_tag:
                self._contributor_buffer.append(content)

    def startElement(self, name, attrs):
        """Opening tag of element"""
        # self._current_tag = name

        if name == "page":
            print("entered PAGE tag")
            self._page_buffer = []
            self._in_page = True
            self._in_revision = False
            self._in_contributor = False

        if name == "revision":
            self._in_revision = True
            self._in_contributor = False
            self._revision_buffer = []

        if name == "contributor":
            self._in_contributor = True
            self._contributor_buffer = []

        if name in ('title', 'text', 'timestamp', 'id', 'contributor'):
            self._current_tag = name

    def endElement(self, name):
        """Closing tag of element"""
        if name == self._current_tag:
            #print("End of Tag: ", name)
            if self._in_contributor:
                self._contributor_values[name] = ' '.join(self._contributor_buffer)

            elif self._in_revision:
                self._revision_values[name] = ' '.join(self._revision_buffer)

            elif self._in_page:
                self._page_values[name] = ' '.join(self._page_buffer)

        if name == 'contributor':
            self._in_contributor = False
            self._in_revision = True
            self._contributor_id_buffer = self._contributor_values['id']
        #            print("Out of CONTRIBUTOR tag")

        if name == 'revision':
            self._in_revision = False
            self._in_page = True
            self._revisions_object_buffer.append(
                Revision(self._contributor_id_buffer, self._revision_values['timestamp'],
                         len(self._revision_values['text'])))
        #            print("Out of REVISION tag")

        if name == 'page':
            self._in_page = False
            self.HistoryPages.append(
                HistoryPage(self._page_values['id'], self._page_values['title'], self._revisions_object_buffer))


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
    # print(line)
    parser.feed(line)
    if handler.HistoryPages:
        break

for x in handler.HistoryPages:
    print(x)
