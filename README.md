## OpenAIRE practical test for Data Engineer interview

The following script extract information from the Institutional repository of the University of Bielefeld [1] and compute statistics on the available data.

# How to run the script 

To run the script, just type the following command:
```
$ python3 OpenAIRE_python_script.py
```
# Description of txt files

The txt files contains the following information:

* n_records_per_publication_year.txt - The number of records per publication year
* n_records_per_typology.txt - The number of records per typology
* n_journal_articles_five_years_interval.txt - The number of Journal Articles produced since 1985 grouped by intervals of 5 years
* n_records_per_author_name.txt - The number of records per author
* n_records_per_ORCID.txt - The number of records per ORCID identified author 

# Libraries
Project is created in Python with the following libraries:

* Pandas: 0.25.3
* Urllib.request: 3.6
* Bs4: 4.9.3
* Numpy: 1.19.4
* Py_stringmatching: 0.4.0
* Requests: 2.24.0
* Re: 2.2.1


[1] http://pub.uni-bielefeld.de/oai
