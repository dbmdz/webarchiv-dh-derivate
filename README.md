# Derivate mit dem Archives Unleashed Toolkit
Das Containerformat WARC, in dem die Daten bei der Webarchivierung abgelegt werden, enthält viele unterschiedliche
Arten von Daten, die eine Website ausmachen: zum Beispiel HTML- und Javascript-Dateien, aber auch Bilder oder PDF-Dateien.
Der erste Schritt vor einer Analyse ist deshalb häufig die Filterung und Extraktion bestimmter Daten aus der ursprünglichen
WARC-Datei und die Erzeugung von abgeleiteten Datensets. Werkzeuge wie das [Archives Unleashed Toolkit (AUT)](https://archivesunleashed.org/)
wurden speziell für diesen Schritt entwickelt.  
Die Skripte in diesem Repository nutzen das Archives Unleashed Toolkit, um Text- und Linkdaten aus den gecrawlten
Websites zu extrahieren und als CSV-Dateien für die weitere Analyse zu speichern.

## Linkgraph
* **Schema**: Crawldatum (YYYYMMDD), Quellhost, Zielhost, Häufigkeit
* **Format**: CSV
* **Bemerkungen**: intrinsische Links sind nicht enthalten

## Plaintext
* **Schema**: Crawldatum (YYYYMMDD), URL, Sprachcode, Textinhalt
* **Format**: CSV
* **Bemerkungen**: HTTP Header und HTML Mark-up sind nicht im Text enthalten, Boilerplate-Inhalte wie Navigation dagegen
  schon

## Plaintext ohne Boilerplate-Inhalte
* **Schema**: Crawldatum (YYYYMMDD), URL, Sprachcode, Textinhalt
* **Format**: CSV
* **Bemerkungen**: Entspricht dem Plaintext Derivat, allerdings werden hier Boilerplate Inhalte mit der entsprechenden 
[AUT-Funktion](https://aut.docs.archivesunleashed.org/docs/text-analysis#extract-plain-text-minus-boilerplate) entfernt

# Nutzung mit Docker
Das Skript setzt voraus, dass die auszuwertenden Daten in der folgenden Struktur abgelegt sind:
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
Passe Ein- und Ausgabeverzeichnisse in `docker-compose.yml` an:
```
    volumes:
      - /path/to/warcs:/in:ro
      - /path/to/store/results:/out:rw
```
Liste die Namen der Zeitschnitte, die ausgewertet sollen, untereinander in einer Textdatei auf und übergebe die Datei mit dem 
Parameter `--target-instance-list`. Gebe abschließend an, welches Derivat erzeugt werden soll (`--type`). 
In `docker-compose.yml`:
```
    command: >-
      /spark/bin/spark-submit
      --driver-memory 5G 
      --py-files /aut/aut-0.90.2.zip 
      --jars /aut/aut-0.90.2-fatjar.jar 
      /extraction/main.py
      --target-instance-list /out/TargetInstanceIDs
      --type linkgraph
```
Starte Extraktion im Docker Container mit `docker-compose up`.