# BIG Data Projekt

Von:

- Leander Christian Prange
- Leon Nehring
- Christian Schuhmann
- Oliver Döring

---

Unsere Pipeline hat eine Laufzeit von etwa 11 Stunden (getestet auf einem AMD Ryzen 3700X mit acht Kernen, sechs parallele Prozesse)

Falls Sie die Pipeline (verständlicher Weise) nicht selbst ausführen möchten, finden Sie hier die Datenbank mit den Daten, welche die Pipeline liefert: <https://1drv.ms/u/s!AuQF7zvluJJE9QUw1EiotJcdFNqT?e=Vfohj1>

## Installation

Das Projekt läuft nur unter Linux (WSL funktioniert auch), da unsere Data-Pipeline das Programm 'bzcat' benötigt. (Könnte unter MacOS ebenfalls verfügbar sein)

Die einzige Abhängigkeit von Python, welche zusätzlich installiert werden muss, ist die Requests-Bibliothek.

## Erwartete Daten

Die Pipeline erwartet einen Wikidump in Form von bz2-komprimierten Dateien (etwa 350gb) im "files"-Ordner, sowie die Datei dewiki-20211001-all-titles-in-ns0 im Root-Verzeichnis des Repositories.
Die Dateien können hier heruntergeladen werden: <https://dumps.wikimedia.org/dewiki/20211001/>

Alternativ: <https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/dewiki/20211001/> (vermutlich schneller)

## Ausführungsreihenfolge

Die Pipeline besteht aus drei Python-Scripts, welche in folgender Reihenfolge auszuführen sind:

1. Abgeordnetenwatch.py
2. check_articles.py
3. wikipedia.py
