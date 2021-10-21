# Derivate mit dem Archives Unleashed Toolkit
Das Containerformat WARC, in dem Daten bei der Webarchivierung abgelegt werden, enthält all die unterschiedlichen
Datentypen, die eine Website ausmachen: zum Beispiel HTML- und Javascript-Dateien, aber auch Bilder oder PDF-Dateien.
Der erste Schritt vor einer Analyse ist deshalb typischerweise die Filterung und Extraktion bestimmter Daten aus der ursprünglichen
WARC-Datei und die Erzeugung von abgeleiteten Datensets. Werkzeuge wie das [Archives Unleashed Toolkit (AUT)](https://archivesunleashed.org/)
wurden speziell für diesen Schritt entwickelt.  
Die Skripte in diesem Repository nutzen das Archives Unleashed Toolkit, um Text-, Link- und Tweetdaten aus den gecrawlten
Websites zu extrahieren und als CSV-Dateien bzw. JSON-Dateien für die weitere Analyse zu speichern.

## Welche Daten können extrahiert werden?
### Tweets
* **Felder**:
  * tweet_time (%Y%m%d%H%M, UTC),
  * tweet_author,
  * retweeter (falls vorhanden),
  * tweet_text,
  * hashtags,
  * mentioned_users,
  * link_destinations,
  * action_stats (Zahl der Retweets, Replies, Favorites für den Tweet zum Crawlzeitpunkt)
* **Format**: JSONLines, jede Zeile entspricht einem Tweet
* **Befehl**: `--derivative tweet`
* **Bemerkung**: Tweets können auf verschiedene Weise archiviert werden: Daten können über die Twitter API 
  abgefragt oder Twitter-Webseiten mit Werkzeugen wie [ArchiveWeb.page](https://archiveweb.page/) über den Browser 
  archiviert werden. Bei dem letztgenannten Ansatz ruft die Nutzerin die gewünschte Twitter-Webseite im Browser auf und 
  steuert über ihre Interaktionen, was im Browser dargestellt und archiviert wird.
  Im Gegensatz zur Abfrage von Daten über die API wird dabei auch das Look-and-Feel der Webseite mit
  erfasst und kann später wiedergegeben werden. Das bedeutet jedoch auch, dass verschiedene Arten von Daten gesammelt in
  einer WARC-Datei abgelegt werden und nicht strukturiert vorliegen. Die Skripte in diesem Repository extrahieren aus den 
  WARC-Dateien strukturierte Daten zu einzelnen Tweets für die anschließende computergestützte Analysen.

### Linkgraph
* **Schema**: Crawldatum (YYYYMMDD), Quellhost, Zielhost, Häufigkeit
* **Format**: CSV
* **Befehl**: `--derivative linkgraph`
* **Bemerkungen**: Intrinsische Links (eine Domain verweist auf sich selbst) sind nicht enthalten.

### Plaintext
* **Schema**: Crawldatum (YYYYMMDD), URL, Sprachcode, Textinhalt
* **Format**: CSV
* **Befehl**: `--derivative plaintext`
* **Bemerkungen**: HTTP Header und HTML Mark-up sind nicht im Text enthalten, Boilerplate-Inhalte wie Navigation dagegen
  schon.

### Plaintext ohne Boilerplate-Inhalte
* **Schema**: Crawldatum (YYYYMMDD), URL, Sprachcode, Textinhalt
* **Format**: CSV
* **Befehl**: `--derivative noboilerplate`
* **Bemerkungen**: Entspricht dem Plaintext Derivat, allerdings werden hier Boilerplate Inhalte mit der entsprechenden 
[AUT-Funktion](https://aut.docs.archivesunleashed.org/docs/text-analysis#extract-plain-text-minus-boilerplate) entfernt.

## Wie extrahiere ich die Daten?
Das Skript unterscheidet bei den auszuwertenden Daten zwischen Zeitschnitten (`--target-instance`) und einzelnen WARC-Dateien (`--warc`).
Ein Zeitschnitt besteht aus mehreren WARC-Dateien, die in der folgenden Struktur abgelegt sind:
```shell
Eingabeverzeichnis
|- Zeitschnitt1
    |- arcs
        |- XY-1.warc.gz
        |- XY-2.warc.gz
        |- ...
|- Zeitschnitt2
    |- arcs
        |- XY-1.warc.gz
        |- XY-2.warc.gz
        |- ...
    
```
Bei Zeitschnitten wird ein Derivat pro Zeitschnitt erzeugt, in dem die Daten aus den zugehörigen WARCs zusammengefasst sind.  
Bei WARCs wird ein Derivat pro WARC-Datei erzeugt.  
Liste die Namen der Zeitschnitte bzw. WARC-Dateien, die ausgewertet sollen, untereinander in einer Textdatei auf und 
übergebe die Datei als Argument. Gebe zusätzlich an, ob es sich bei den aufgelisteten Namen um Zeitschnitte (`--target-instance`)
oder WARCs (`--warc`) handelt und welches Derivat erzeugt werden soll (`--derivative`). 
In `docker-compose.yml`:
```
    command: >-
      /spark/bin/spark-submit
      --driver-memory 5G 
      --py-files /aut/aut-0.90.2.zip 
      --jars /aut/aut-0.90.2-fatjar.jar 
      /extraction/main.py
      --derivative linkgraph
      --target-instance
      /out/TargetInstanceIDs
```
Definiere Ein- und Ausgabeverzeichnisse in `docker-compose.yml`:
```
    volumes:
      - /path/to/warcs:/in:ro
      - /path/to/store/results:/out:rw
```
Starte Extraktion im Docker Container mit `docker-compose up`.