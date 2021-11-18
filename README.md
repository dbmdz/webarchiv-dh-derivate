# Generating derivative datasets with the Archives Unleashed Toolkit
When a website is archived, the different files that make up the website are typically saved in a WARC file. A WARC
file may therefore contain many different file types, ranging from HTML and Javascript files to images or PDF files.
Before you start analyzing an archived website, the first step is generally to extract specific 
types of data from the original WARC files, for which you can use tools like the [Archives Unleashed Toolkit](https://archivesunleashed.org/).  
The script in this repository uses the Archives Unleashed Toolkit to extract plain text, links and tweets from archived
websites and stores them as CSV or JSON files for further analysis.

## What can be extracted?
### Tweets
* **fields**:
  * tweet_time (%Y%m%d%H%M, UTC),
  * tweet_author,
  * retweeter (if available),
  * tweet_text,
  * hashtags,
  * mentioned_users,
  * link_destinations,
  * action_stats (number of retweets, replies, favorites at the time the tweet was archived)
* **format**: JSONLines, every line represents a tweet
* **command**: `--derivative tweet`
* **comment**: Tweets can be archived in different ways: You can query the Twitter API for data or you can archive Twitter 
  websites using browser-based tools like [ArchiveWeb.page](https://archiveweb.page/). In the latter case, you would
  open the website and determine what should be archived by interacting with the website in your browser (e.g. scrolling down
  to load older content).
  With this approach you are also able to capture the look-and-feel of the website and replay it at a later time. This means,
  however, that different kinds of data will be stored in a WARC file and you will not get structured data for
  individual tweets. The scripts in this repository extracts structured data for individual tweets from WARC files for
  computational analysis.

### Link graph
* **schema**: crawl date (YYYYMMDD), source host, destination host, frequency
* **format**: CSV
* **command**: `--derivative linkgraph`
* **comment**: intrinsic links (host links back to itself) are not included

### Plain text
* **schema**: crawl date (YYYYMMDD), URL, language code, plain text
* **format**: CSV
* **command**: `--derivative plaintext`
* **comment**: HTTP headers und HTML Mark-up are not included in the output, boilerplate content like navigation is

### Plain text without boilerplate content
* **schema**: crawl date (YYYYMMDD), URL, language code, plain text
* **format**: CSV
* **command**: `--derivative noboilerplate`
* **comment**: Like the plain text extract, but boilerplate content is removed using the corresponding [function in AUT](https://aut.docs.archivesunleashed.org/docs/text-analysis#extract-plain-text-minus-boilerplate)

## How do I extract data?
The script distinguishes between target instances (`--target-instance`) and single WARC files (`--warc`) as input.
A target instance consists of multiple WARC files and is organized as follows:
```shell
input directory
|- target_instance_1
    |- arcs
        |- XY-1.warc.gz
        |- XY-2.warc.gz
        |- ...
|- target_instance_2
    |- arcs
        |- XY-1.warc.gz
        |- XY-2.warc.gz
        |- ...
    
```
The script creates one output file per target instance, which contains data from all the corresponding WARC files.  
List the names of the target instances or WARC files you want to process in a text file and pass the file as an argument 
on the command line.  
Specify additionally whether the names listed are target instances (`--target-instance`) or WARCs (`--warc`) and what 
kind of data you want to extract (`--derivative`).  
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
Specify input and output directories in `docker-compose.yml`:
```
    volumes:
      - /path/to/warcs:/in:ro
      - /path/to/store/results:/out:rw
```
Start processing in the Docker container with `docker-compose up`.