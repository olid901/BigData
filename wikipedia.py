import requests# Library for parsing HTML
from bs4 import BeautifulSoup
dump_url = "https://dumps.wikimedia.org/dewiki/20211101/"
base_url = "https://dumps.wikimedia.org"
import urllib.request

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# Retrieve the html
dump_html = requests.get(dump_url).text# Convert to a soup
soup_dump = BeautifulSoup(dump_html, 'html.parser')# Find list elements with the class file
bz2_files = [x.get('href') for x in soup_dump.find_all('a') if "dewiki-20211101-pages-meta-history" in x.get('href') and x.get('href')[-4:] == ".bz2"]


# TODO: File-download: Kann ich nicht testen, da der Download immer abbricht.
#urllib.request.urlretrieve(base_url+bz2_files[0], "files/test.bz2")


