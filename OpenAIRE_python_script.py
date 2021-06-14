#!/usr/bin/env python
# coding: utf-8

# ## Extract information from URL database and trasforming XML records in a data frame

# Import libraries for data extraction and analysis
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime
from itertools import groupby

import pandas as pd
import numpy as np
import py_stringmatching as sm

import requests
import itertools
import re
import os
# import ast

working_dir = os.getcwd()

# Define a filter for identify headers with identifier ID information

def filter_identifiers(tag):
    return (tag.name == 'identifier' and tag.parent.name == 'header')

## Get XML files for extracting information from URL database 

url_database = 'https://pub.uni-bielefeld.de/oai'
harvesting_verb = 'ListRecords'
matadata_prefix = 'oai_datacite'

identifiers_list_df = []
resource_types_list_df = []
creation_years_list_df = []
publication_years_list_df = []
authors_list_df = []

ploads = {'verb': harvesting_verb,'metadataPrefix': matadata_prefix}
r = requests.get(url_database,params=ploads)

# XML data are retrieved here
xml_data = r.content
soup = BeautifulSoup(xml_data, "xml")

# Retrieve information about information only to retrieve information about remaining n_resource 
# for knowing how many remaining items are left in the URL database

resumptionTokens_list = soup.find_all("resumptionToken")

for resumptionToken in resumptionTokens_list:
        n_resources = int(resumptionToken.attrs['completeListSize'])

n_remaining = n_resources

print("Extracting information from XML files...")

while n_remaining>0:
    
    # print(n_remaining)
    
    # Extract identifier information from the XML file and save it in a list 
    identifiers = soup.find_all(filter_identifiers)

    for identifier in identifiers:
        identifiers_list_df.append(identifier.text)
        
        
    # Extract resource type information from the XML file and save it in a list
    resource_types = soup.find_all("resourceType")

    for resource_type in resource_types:
        resource_types_list_df.append(resource_type.text)
    
    
    # Extract creation year information from the XML file and save it in a list
    dates = soup.find_all("date", dateType="Created")
    
    for date in dates:
        date_time = datetime.strptime(date.text, '%Y-%m-%dT%H:%M:%SZ')
        creation_years_list_df.append(date_time.year)
        
    # Extract publication year information from the XML file and save it in a list
    publication_years = soup.find_all("publicationYear")
    
    for publication_year in publication_years:
        publication_years_list_df.append(int(publication_year.text))
    
    # Extract authours list information from the XML file and save it in a list
    single_creators_list_infos = soup.find("creators")
    
    while single_creators_list_infos != None:
    
        author_list_info = []  
        creator_infos = single_creators_list_infos.find("creator")

        while creator_infos != None:
            
            ORCID_author = ' '
    
            for creator_info in creator_infos:
        
                if creator_info.name == "givenName":
                    givenName_author = creator_info.text
                if creator_info.name == "familyName":
                    familyName_author = creator_info.text
                if creator_info.name == "nameIdentifier":
                    ORCID_author = creator_info.text
            
            author_infos_tuple = (givenName_author, familyName_author, ORCID_author)
            author_list_info.append(author_infos_tuple)
    
            try:
                # Find next sibling to retrieve another author's information
                # If not, AttributeErrore stops the iteration
                old_creator_infos = creator_infos
                next_creator_list_infos = old_creator_infos.find_next_sibling("creator")
                creator_infos = next_creator_list_infos
            
            except AttributeError:
            
                break
            
        authors_list_df.append(author_list_info)
     
        try:
            # Find next creators information to retrieve another authors list's information
            # If not, AttributeErrore stops the iteration
            old_single_creators_list_infos = single_creators_list_infos
            next_single_creators_list_infos = old_single_creators_list_infos.find_next("creators")
            single_creators_list_infos = next_single_creators_list_infos
            
        except AttributeError:
        
            break
    
    # Prepare verbs and attributes for retrieving next XML file 
    resumptionTokens_list = soup.find_all("resumptionToken")

    for resumptionToken in resumptionTokens_list:
        resumptionToken_value = resumptionToken.text
        
    ploads = {'verb': harvesting_verb,'resumptionToken':resumptionToken}
    
    # Retrieve next XML file
    r = requests.get(url_database, params=ploads)
    xml_data = r.content
    soup = BeautifulSoup(xml_data, "xml")
        
    # Remove the number of resources added to the final list from the remaining resources 
    n_remaining -= len(identifiers)


# Total number of information which are retrieved in the final lists
print("Number of identifiers:", len(set(identifiers_list_df)))

# Transform lists in a single dataframe with all records information
data_df = {'Identifier': identifiers_list_df, 'Typology': resource_types_list_df,'Publication Year': publication_years_list_df,'Authors List': authors_list_df}
records_df = pd.DataFrame(data_df)
print("Dataframe CREATED!")


# csv_file_name = "~/Desktop/Institutional_repository_University_Bielefeld.csv"
# records_df.to_csv(csv_file_name, index=False, header=True)

# json_file_name = "~/Desktop/Institutional_repository_University_Bielefeld.json"
# records_df.to_json(json_file_name, orient="split")


# ## Analyze records data frame

# csv_file_name = "~/Desktop/Institutional_repository_University_Bielefeld.csv"
# records_df = pd.read_csv(csv_file_name)  
# records_df['Authors List'] = records_df['Authors List'].map(lambda x: ast.literal_eval(x))

# records_df.head(10)

# Get number of records per publication year by counting values in the column "Publication Year"
# and save information in a txt file
n_records_per_publication_year_txt_file = "/n_records_per_publication_year.txt"
txt_file_name = working_dir + n_records_per_publication_year_txt_file

publication_years_count = records_df['Publication Year'].value_counts()
sorted_publication_years_count = sorted(publication_years_count.items(), key=lambda item: item[0], reverse = False)

with open(txt_file_name, 'w') as f:
    
    f.write("Number of records per publication year: \r\n")
    f.write("\r\n")

    for publication_year in sorted_publication_years_count:
        f.write("Year: %s \r\n" % publication_year[0])
        f.write("Number of records: %s \r\n" % publication_year[1])
        f.write("\r\n")

print("Number of records per publication year: SAVED!")

# Get number of records per typology by counting values in the column "Typology"
# and save information in a txt file

n_records_per_typology_txt_file = "/n_records_per_typology.txt"
txt_file_name = working_dir + n_records_per_typology_txt_file

typology_counts = records_df['Typology'].value_counts()

with open(txt_file_name, 'w') as f:
    
    f.write("Number of records per publication year:\r\n")
    f.write("\r\n")
    
    for typology_count in typology_counts.items():
        f.write("Typology: %s \r\n" % typology_count[0])
        f.write("Number of records: %s \r\n" % typology_count[1])
        f.write("\r\n")

print("Number of records per typology: SAVED!")

# Get number of 'Journal_article' in produced since 195 grouped by intervals of 5 years in the following steps:
# and save information in a txt file

n_journal_articles_five_years_interval_txt_file = "/n_journal_articles_five_years_interval.txt"
txt_file_name = working_dir + n_journal_articles_five_years_interval_txt_file

# 1 - Select only Journal Articles and store these dataframe rows in a separated dataframe;
journal_articles_df = records_df[records_df["Typology"] == 'journal_article']

starting_year = 1985
years_interval = 5

today = datetime.today()
end_year = today.year

total_interval_years = []
n_journal_articles_interval_years = []

starting_year_interval = starting_year

with open(txt_file_name, 'w') as f:
    
    f.write("Number of Journal Articles produced since 1985 grouped by intervals of 5 years:\r\n")
    f.write("\r\n")

    # 2 - Select only rows with "Publication Year" value between boundary values in each interval year
    while starting_year_interval<end_year:
    
        n_journal_articles_interval_year = len(journal_articles_df.loc[(journal_articles_df["Publication Year"]>=starting_year_interval) & (journal_articles_df["Publication Year"]<starting_year_interval+years_interval), :])
        f.write("Interval year: %s - %s \r\n" % (str(starting_year_interval), str(starting_year_interval+years_interval))) 
        f.write("Number of records: %s \r\n" % n_journal_articles_interval_year)
        f.write("\r\n")
    
        starting_year_interval += years_interval

print("Number of journal articles produced since 1985 grouped by intervals of 5 years: SAVED!")

# Get information about number of records per identified ORCID and author name in the following steps:

# authors_dict is the dictionary with author name as key and number of records as value
authors_dict = {}

# ORCID_dict is the dictionary with ORCID identifier as key and number of records as value
ORCID_dict = {}

# authors_ORCID_dict is the dictionary with ORCID identifier as key and author name as value
authors_ORCID_dict = {}

# 1 - Get all authors' information from the 'Authors List' column;
for authors_list in records_df['Authors List']:
    
    # Get only the first occurence of ORCID identifier in a authors' list
    # Then is_record_in_ORCID_dict == False and other occurences are not taken as valid
    is_record_in_ORCID_dict = True
    
    for author_occurence in authors_list:
        
        initial_given_name = author_occurence[0]
        initial_family_name = author_occurence[1]
        ORCID_author = author_occurence[2]
        
        # 1.A - All the strings in the family name and given name are standardized by substituting "-" with
        # a whitespace and removing all non-textual special characters 
        lower_family_name = initial_family_name.replace("-", " ")
        lower_family_name = re.sub(r'[^\w\s]', '', lower_family_name)
        lower_family_name = lower_family_name.strip().lower()
        family_name = lower_family_name.title()
        
        lower_given_name = initial_given_name.replace("-", " ")
        lower_given_name = re.sub(r'[^\w\s]', '', lower_given_name)
        lower_given_name = lower_given_name.strip().lower()
        given_name = lower_given_name.title()
        
        author_name = (family_name ,given_name)
        
        # 1.B - Saving information in the three separate dictionaries
        if author_name in authors_dict:
            authors_dict[author_name] += 1
        else:
            authors_dict.update({author_name: 1})
        
        if ORCID_author != ' ':
            
            if is_record_in_ORCID_dict:
            
                if ORCID_author in ORCID_dict:
                    ORCID_dict[ORCID_author] += 1
                else:
                    ORCID_dict.update({ORCID_author: 1})
                    
                is_record_in_ORCID_dict = False
            
            if ORCID_author not in authors_ORCID_dict:
                authors_ORCID_dict.update({ORCID_author: [author_name]})
            else:
                list_same_ORCID = authors_ORCID_dict[ORCID_author]
                if author_name not in list_same_ORCID:
                    authors_ORCID_dict[ORCID_author].append(author_name)
                    
# print("Lenght of preliminary list of authors:", len(authors_dict))

# 1.C - Get ORCID identifier information from  save information in a txt file

sorted_ORCID_list = sorted(ORCID_dict.items(), key=lambda item: item[0], reverse = False)

n_records_per_ORCID_txt_file = "/n_records_per_ORCID.txt"
txt_file_name = working_dir + n_records_per_ORCID_txt_file

with open(txt_file_name, 'w') as f:
    
    f.write("Number of records per identified ORCID: \r\n")
    f.write("\r\n")

    for single_ORCID_author in sorted_ORCID_list:
        f.write("ORCID: %s \r\n" % single_ORCID_author[0])
        f.write("Number of records: %s \r\n" % single_ORCID_author[1])
        f.write("\r\n")

print("Number of records per identified ORCID: SAVED!")

# 2 - Merge two authors if their author name information follow some simple rules
# Dictionary authors_association_dict is the dictionary with first author name (to sostitute) as key 
# and the second author name (the final name) as value

authors_association_dict = {}
misindetified_authors_list_threshold = 2

# 2.A - Get two authors with same ORCID and if the first string (in case of more strings) 
# of the family name is the same
for ORCID, authors_list in authors_ORCID_dict.items():
    
    if len(authors_list)>=misindetified_authors_list_threshold:
                        
        list_first_surnames = []
        
        for author_first, author_second in list(itertools.combinations(authors_list, 2)):
            
            family_name_first_author = author_first[0]
            string_first_surname_first_author = family_name_first_author.split()[0]
            
            family_name_second_author = author_second[0]
            string_first_surname_second_author = family_name_second_author.split()[0]
            
            given_name_first_author = author_first[1]
            given_name_second_author = author_second[1]
            
            # The final author name is the one with the longest number of characters 
            # (i.e., more information about the author name is saved)
            
            len_strings_first_author = len(family_name_first_author) + len(given_name_first_author)
            len_strings_second_author = len(family_name_second_author) + len(given_name_second_author)
            
            if len_strings_first_author>len_strings_second_author:
                author_name_original = author_second
                author_name_final = author_first
            else:
                author_name_original = author_first
                author_name_final = author_second
            
            if string_first_surname_first_author == string_first_surname_second_author:
                authors_association_dict.update({author_name_original: author_name_final})

#  2.B - Merge authors with same ORCID in the final authors_dict
for author_original, author_final in authors_association_dict.items():
    
    new_number_records = authors_dict[author_original]+authors_dict[author_final]
    authors_dict.update({author_final: new_number_records})
    del authors_dict[author_original]
    
# print("Lenght of preliminary list of authors - after merging authors with same ORCID:", len(authors_dict))

# 2.C - Get two authors with same family name and check if they have "similar" given names
# or if one of them is the shortened version of the other one

# Group authors with the same family name
grouped_similar_family_name_authors_list = groupby(sorted(authors_dict.items()), key=lambda i:i[0][0])
same_author_names_dict = {}

raw_score_threshold = 1
minimum_threshold_single_name =  1

lev = sm.Levenshtein()

for family_name, list_similar_family_names in grouped_similar_family_name_authors_list:
    
    # Take the permutations of the lists of authors with same family name
    for first_author, second_author in list(itertools.combinations(list_similar_family_names, 2)):
        
        first_given_name = first_author[0][1]
        second_given_name = second_author[0][1]
        
        splitted_first_author_given_name = first_given_name.split()
        splitted_second_author_given_name = second_given_name.split()
        
        len_char_in_first_author_given_name = 0
        len_char_in_second_author_given_name = 0
        
        for first_author_given_name in splitted_first_author_given_name:
            len_char_in_first_author_given_name += len(first_author_given_name)
            
        for second_author_given_name in splitted_second_author_given_name:
            len_char_in_second_author_given_name += len(second_author_given_name)
                    
        is_second_short = True
        
        # Define which is the shortest and the longest surname
        if len_char_in_first_author_given_name>len_char_in_second_author_given_name:
            
            short_given_name = second_given_name
            len_short_given_name = len_char_in_second_author_given_name
            short_author_given_names = splitted_second_author_given_name
            
            long_given_name = first_given_name
            len_long_given_name = len_char_in_first_author_given_name
            long_author_given_names = splitted_first_author_given_name
        
        else:
            
            short_given_name = first_given_name
            len_short_given_name = len_char_in_first_author_given_name
            short_author_given_names = splitted_first_author_given_name
            
            long_given_name = second_given_name
            len_long_given_name = len_char_in_second_author_given_name
            long_author_given_names = splitted_second_author_given_name
            
            is_second_short = False
        
        # If ONLY one of them is composed by single characters then compare the first character of all strings with the longest family name
        # If they are the same, include in the same_author_names_dict
        if len_short_given_name<(len(short_author_given_names)*minimum_threshold_single_name+1):
            
            if len_long_given_name>=(len(long_author_given_names)*minimum_threshold_single_name+1):
                
                is_same_name = True
                
                for first_given_name, second_given_name in zip(short_author_given_names, long_author_given_names):
                    if first_given_name[0]!=second_given_name[0]:
                        is_same_name = False
                        
                if is_same_name:
                    if is_second_short:
                        same_author_names_dict.update({second_author[0]: first_author[0]})
                    else:
                        same_author_names_dict.update({first_author[0]: second_author[0]})
            
        else:
            
            # If the previous rule is not fulfilled then compare according to Levehnstein distance
            # If raw_score<=1 (i.e., if there is only mispelled character) then include in the same_author_names_dict
            raw_score = lev.get_raw_score(short_given_name, long_given_name)
            if raw_score<=raw_score_threshold:
                if is_second_short:
                    same_author_names_dict.update({second_author[0]: first_author[0]})
                else:
                    same_author_names_dict.update({first_author[0]: second_author[0]})

# 2.D - Substitue values which are even keys in the final same_author_names_dict

for author_first_name in same_author_names_dict.keys():
    
    if author_first_name in same_author_names_dict.values():
        
        author_second_name = same_author_names_dict[author_first_name]        
        author_first_name_associated = list(same_author_names_dict.keys())[list(same_author_names_dict.values()).index(author_first_name)]
        
        same_author_names_dict.update({author_first_name_associated: author_second_name})

# 2.E - Substitute all same_author_names_dict keys in the final authors_dict

for author_to_sostitute, author_final_name in same_author_names_dict.items():
    
    n_records_author_final_name = authors_dict[author_final_name]
    n_records_author_to_sostitute = authors_dict[author_to_sostitute]
    
    new_n_records_author_final_name = n_records_author_final_name+n_records_author_to_sostitute
    
    authors_dict.update({author_final_name: new_n_records_author_final_name})
    del authors_dict[author_to_sostitute]

# 2.F - Remove all NONE and 1-letter family names and save information in a txt file
sorted_authors_dict = sorted(authors_dict.items(), key=lambda item: item[0], reverse = False)

n_records_per_author_name_txt_file = "/n_records_per_author_name.txt"
txt_file_name = working_dir + n_records_per_author_name_txt_file

minimum_strings_in_authors = 1

with open(txt_file_name, 'w') as f:

    f.write("Number of records per author:\r\n")
    f.write("\r\n")

    for author_name, n_occurence in sorted_authors_dict:
    
        author_name_family_name = author_name[0]
        author_name_given_name = author_name[1]
    
    
        if len(author_name_family_name)>minimum_strings_in_authors:
            f.write("Author name: %s %s \r\n" % (author_name_family_name, author_name_given_name))
            f.write("Number of records: %s \r\n" % n_occurence)
            f.write("\r\n")

print("Number of records per author: SAVED!")
print("THE END!")
