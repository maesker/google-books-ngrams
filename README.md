# google-books-ngrams
A script to download and process the google-books-ngram data set


# Data set

Url: http://storage.googleapis.com/books/ngrams/books/datasetsv2.html 
License: Creative Commons Attribution 3.0 Unported License. 

File format: Each of the files below is compressed tab-separated data. In Version 2 each line has the following format:

    ngram TAB year TAB match_count TAB volume_count NEWLINE

As an example, here are the 3,000,000th and 3,000,001st lines from the a file of the English 1-grams (googlebooks-eng-all-1gram-20120701-a.gz):

    circumvallate   1978   335    91
    circumvallate   1979   261    91




# Tool

I used this tool to creade data sets for my research on data structures. Currently, it downloads and merges the V2 data set.

    usage: ngram-processing.py [-h] -n NGRAMS [-l LANGUAGE] [-m]

    Google ngrams processing

    optional arguments:
      -h, --help            show this help message and exit
      -n NGRAMS, --ngrams NGRAMS
                            Ngram to process. [1, 2, 3, 4, 5, all]
      -l LANGUAGE, --language LANGUAGE
                            The language to download, ['eng', 'chi-sim', 'fre',
                            'ger', 'heb', 'ita', 'rus', 'spa']
      -m, --merge           Merge the ngrams individual years and books to a
                            single entry
