import requests  # Library for parsing HTML
from bs4 import BeautifulSoup

dump_url = "https://dumps.wikimedia.org/dewiki/20211101/"
base_url = "https://dumps.wikimedia.org"
import urllib.request
import subprocess
import os
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

    #def add_revision(self, contributor_id, timestamp, text_len):
     #   self.revisions.append(Revision(contributor_id, timestamp, text_len))


'''
Tree Diagram:

        page ----------------> n revisions
        |---> id                |---> id
        |---> title             |---> text_len (after edit)
                                |---> contributor
                                       |---> id
'''

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
        self._pages = []
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
                print("revision_content: ", content)
                self._revision_buffer.append(content)
        if self._in_contributor:
            print("Contributor content: ", content)
            if self._current_tag:
                self._contributor_buffer.append(content)

    def startElement(self, name, attrs):
        """Opening tag of element"""
        #self._current_tag = name

        if name == "page":
            print("entered PAGE tag")
            self._page_buffer = []
            self._in_page = True
            self._in_revision = False
            self._in_contributor = False

        if name == "revision":
            print("endeted REVISION tag")
            self._in_revision = True
            self._in_contributor = False
            self._revision_buffer = []

        if name == "contributor":
            print("entered CONTRIBUTOR tag")
            self._in_contributor = True
            self._contributor_buffer = []

        if name in ('title', 'text', 'timestamp', 'id', 'contributor'):
            print("Current tag is now: ", name)
            self._current_tag = name

    def endElement(self, name):
        """Closing tag of element"""
        if name == self._current_tag:
            print("End of Tag: ", name)
            if self._in_contributor:
                self._in_contributor = False
                self._in_revision = True
                self._contributor_values[name] = ' '.join(self._contributor_buffer)
                print("end of CONTRIBUTOR tag, contributor_values:: ", self._page_values)
                #print("End Element in contributor: ", name)
                #self._contributor_id_buffer = self._values['id']
                #print("--- added contributor")
            elif self._in_revision:
                self._in_revision = False
                self._in_page = True
                self._revision_values[name] = ' '.join(self._revision_buffer)
                print("end of REVISION tag, revision_values:: ", self._revision_values)
                #print("revision_values: ", self._values)
                #self._revisions_object_buffer.append(Revision(self._contributor_id_buffer, self._values['timestamp'], len(self._values['text'])))
                #print("-- added revision")
            elif self._in_page:
                self._in_page = False
                self._page_values[name] = ' '.join(self._page_buffer)
                print("end of PAGE tag, page_values:: ", self._page_values)
                #self.HistoryPages.append(HistoryPage(self._values['id'], self._values['title'], self._revisions_object_buffer))
                #print("- added page")
            #self._values[name] = ' '.join(self._buffer)

        if name == 'contributor':
            self._contributor_id_buffer = self._contributor_values['id']

        if name == 'revision':
            self._revisions_object_buffer.append(Revision(self._contributor_id_buffer, self._revision_values['timestamp'], len(self._revision_values['text'])))

        if name == 'page':
            self.HistoryPages.append(HistoryPage(self._page_values['id'], self._page_values['title'], self._revisions_object_buffer))


'''
class WikiXmlHandler(xml.sax.handler.ContentHandler):
    """Content handler for Wiki XML data using SAX"""

    def __init__(self):
        xml.sax.handler.ContentHandler.__init__(self)
        self._page_buffer = None
        self._revision_buffer = None
        self._values = {}
        self._in_page = False
        self._pages = []
        self.revisions = []
        self._place = []

    def characters(self, content):
        # print(content)
        """Characters between opening and closing tags"""
        if self._in_page:
            print(" - ", content)
            self._page_buffer.append(content)

    def startElement(self, name, attrs):
        """Opening tag of element"""
        if name == "page":
            self._pages.append(WikipediaHistory)
            self._place = "page"
            self._in_page = True
            self._page_buffer = []
        if self._place == "page" and name == "title":
            print("title: ", str(attrs))

    def endElement(self, name):
        """Closing tag of element"""
        if name == "page":
            self._in_page = False
            self._values[name] = ' '.join(self._page_buffer)
            print("Put into values:")
            print(self._values)
        #if name == "revision":

        # if name == 'page':
        #   self._pages.append((self._values['title'], self._values['text']))
        # if name == 'revision':

'''
# Retrieve the html
dump_html = requests.get(dump_url).text  # Convert to a soup
soup_dump = BeautifulSoup(dump_html, 'html.parser')  # Find list elements with the class file
bz2_files = [x.get('href') for x in soup_dump.find_all('a') if
             "dewiki-20211101-pages-meta-history" in x.get('href') and x.get('href')[-4:] == ".bz2"]

# TODO: File-download: Kann ich nicht testen, da der Download immer abbricht.
# urllib.request.urlretrieve(base_url+bz2_files[0], "files/test.bz2")

file = "./files/" + "dewiki-20211001-pages-meta-history1.xml-p1p1598.bz2"

print(os.path.isfile("files/dewiki-20211001-pages-meta-history1.xml-p1p1598.bz2"))

lines = []

# Content handler for Wiki XML
handler = WikiXmlHandler()

# Parsing object
parser = xml.sax.make_parser()
parser.setContentHandler(handler)

for i, line in enumerate(
        subprocess.Popen(['bzcat'], stdin=open("files/dewiki-20211001-pages-meta-history1.xml-p1p1598.bz2"),
                         stdout=subprocess.PIPE).stdout):
    #print(line)
    parser.feed(line)
    if i > 100:
        break
# for p in handler._pages[0]:
#    print(p[0])
