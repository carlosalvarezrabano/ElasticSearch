#-------------------------------------------------------------------------------
# Name:        Primer Ejercicio
# Purpose:
#
# Author:      Sergio
#
# Created:     05/01/2019
# Copyright:   (c) Owner 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------
from __future__ import print_function

import json # Para poder trabajar con objetos JSON
import pprint # Para poder hacer uso de PrettyPrinter

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import datetime

def main():

    # To prit nicely
    pp = pprint.PrettyPrinter(indent=2)

    # Connnecting to localhost:9200
    es = Elasticsearch()
    index1 = "reddit-mentalhealth"


    initialQuery = es.search(
        index="reddit-mentalhealth",
        body = {
            "size":0,
            "query":{
                "query_string":{
                    "default_field":"selftext",
                    "query":"*drugs*"
                    }
                },
            "aggs": {
                "Terminos mas significativos": {
                    "significant_terms": {
                        "field":"selftext",
                        "size":25,
                        "exclude":["a", "about", "above", "after", "again", "against", "all", "also", "am", "an", "and", "another", "any", "are", "aren't", "as", "at", "back", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "even", "ever", "every", "few", "first", "five", "for", "four", "from", "further", "get", "go", "goes", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "high", "him", "himself", "his", "how", "how's", "however", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "just", "least", "less", "let's", "like", "long", "made", "make", "many", "me", "more", "most", "mustn't", "my", "myself", "never", "new", "no", "nor", "not", "now", "of", "off", "old", "on", "once", "one", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "put", "said", "same", "say", "says", "second", "see", "seen", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "since", "so", "some", "still", "such", "take", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "three", "through", "to", "too", "two", "under", "until", "up", "very", "was", "wasn't", "way", "we", "we'd", "we'll", "we're", "we've", "well", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "whether", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"],
                    }
                 }
            }
        }
    )
    print("********** Resultados consulta inicial **********\n")
    pp.pprint(initialQuery)
    print("Numero de resultados: " + str(initialQuery["hits"]["total"]) + "\n")


    queryExtended ={
        "query":{
            "query_string":{
                "default_field":"selftext",
                "query":"*drugs* OR Alcohol OR Weed OR Cocaine"
                }
            },
            "aggs": {
                "Terminos significativos":{
                    "significant_terms":{
                        "field":"selftext",
                        "size":15,
                        "exclude":["Alcohol", "Weed", "Cocaine", "a", "about", "above", "after", "again", "against", "all", "also", "am", "an", "and", "another", "any", "are", "aren't", "as", "at", "back", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "even", "ever", "every", "few", "first", "five", "for", "four", "from", "further", "get", "go", "goes", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "high", "him", "himself", "his", "how", "how's", "however", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "just", "least", "less", "let's", "like", "long", "made", "make", "many", "me", "more", "most", "mustn't", "my", "myself", "never", "new", "no", "nor", "not", "now", "of", "off", "old", "on", "once", "one", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "put", "said", "same", "say", "says", "second", "see", "seen", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "since", "so", "some", "still", "such", "take", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "three", "through", "to", "too", "two", "under", "until", "up", "very", "was", "wasn't", "way", "we", "we'd", "we'll", "we're", "we've", "well", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "whether", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"],
                        }
                    },
                }
        }

    # Scannning
    results = helpers.scan(es,
        index= index1,
        doc_type="post",
        query= queryExtended
    )

    #Function that creates a dictionary with the required data for a given result
    def fillData(hit):
        aux = {}
        aux["author"] = hit["_source"]["author"].encode("utf8")

        date = hit["_source"]["created_utc"] #Convert timestamp to date
        convertedDate = datetime.utcfromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S')

        aux["created_utc"] = convertedDate
        text = hit["_source"]["selftext"].encode("utf8")
        finalText = text.replace('\n','')
        aux["selftext"] = finalText

        return aux

    resCount = 0    #Number of results
    data = []
    for hit in results:
        data.append(fillData(hit))
        resCount = resCount + 1

    with open("mentalhealth-drugs-addition.json", 'w') as write_file:
        json.dump(data, write_file, indent=2)


    print("********** Resultados consulta extendida **********\n")
    print("Numero de resultados: " + `resCount` + "\n")

    extendedQueryGnd = es.search(
        index="reddit-mentalhealth",
        body = {
            "size":0,
            "query":{
                "query_string":{
                    "default_field":"selftext",
                    "query":"*drugs*"
                    }
                },
            "aggs": {
                "Terminos mas significativos": {
                    "significant_terms": {
                        "field":"selftext",
                        "size":15,
                        "exclude":["drugs", "drug", "recreational", "years", "life", "a", "about", "above", "after", "again", "against", "all", "also", "am", "an", "and", "another", "any", "are", "aren't", "as", "at", "back", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "even", "ever", "every", "few", "first", "five", "for", "four", "from", "further", "get", "go", "goes", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "high", "him", "himself", "his", "how", "how's", "however", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "just", "least", "less", "let's", "like", "long", "made", "make", "many", "me", "more", "most", "mustn't", "my", "myself", "never", "new", "no", "nor", "not", "now", "of", "off", "old", "on", "once", "one", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "put", "said", "same", "say", "says", "second", "see", "seen", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "since", "so", "some", "still", "such", "take", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "three", "through", "to", "too", "two", "under", "until", "up", "very", "was", "wasn't", "way", "we", "we'd", "we'll", "we're", "we've", "well", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "whether", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves","can"],
                        "gnd":{}
                    }
                 }
            }
        }
    )

    print("********** Resultados utilizando la metrica gnd **********\n")
    pp.pprint(extendedQueryGnd)


    extendedQueryChiSquare = es.search(
        index="reddit-mentalhealth",
        body = {
            "size":0,
            "query":{
                "query_string":{
                    "default_field":"selftext",
                    "query":"*drugs*"
                    }
                },
            "aggs": {
                "Terminos mas significativos": {
                    "significant_terms": {
                        "field":"selftext",
                        "size":20,
                        "exclude":["drugs", "drug", "recreational", "years", "life", "a", "about", "above", "after", "again", "against", "all", "also", "am", "an", "and", "another", "any", "are", "aren't", "as", "at", "back", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "even", "ever", "every", "few", "first", "five", "for", "four", "from", "further", "get", "go", "goes", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "high", "him", "himself", "his", "how", "how's", "however", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "just", "least", "less", "let's", "like", "long", "made", "make", "many", "me", "more", "most", "mustn't", "my", "myself", "never", "new", "no", "nor", "not", "now", "of", "off", "old", "on", "once", "one", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "put", "said", "same", "say", "says", "second", "see", "seen", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "since", "so", "some", "still", "such", "take", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "three", "through", "to", "too", "two", "under", "until", "up", "very", "was", "wasn't", "way", "we", "we'd", "we'll", "we're", "we've", "well", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "whether", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves","can"],
                        "chi_square":{}
                    }
                 }
            }
        }
    )

    print("\n********** Resultados utilizando la metrica chi square **********\n")
    pp.pprint(extendedQueryChiSquare)


    extendedQueryMutualInformation = es.search(
        index="reddit-mentalhealth",
        body = {
            "size":0,
            "query":{
                "query_string":{
                    "default_field":"selftext",
                    "query":"*drugs*"
                    }
                },
            "aggs": {
                "Terminos mas significativos": {
                    "significant_terms": {
                        "field":"selftext",
                        "size":20,
                        "exclude":["drugs", "drug", "recreational", "years", "life", "a", "about", "above", "after", "again", "against", "all", "also", "am", "an", "and", "another", "any", "are", "aren't", "as", "at", "back", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "even", "ever", "every", "few", "first", "five", "for", "four", "from", "further", "get", "go", "goes", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "high", "him", "himself", "his", "how", "how's", "however", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "just", "least", "less", "let's", "like", "long", "made", "make", "many", "me", "more", "most", "mustn't", "my", "myself", "never", "new", "no", "nor", "not", "now", "of", "off", "old", "on", "once", "one", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "put", "said", "same", "say", "says", "second", "see", "seen", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "since", "so", "some", "still", "such", "take", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "three", "through", "to", "too", "two", "under", "until", "up", "very", "was", "wasn't", "way", "we", "we'd", "we'll", "we're", "we've", "well", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "whether", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves","can"],
                        "mutual_information":{}
                    }
                 }
            }
        }
    )

    print("\n********** Resultados utilizando la metrica mutual information **********\n")
    pp.pprint(extendedQueryMutualInformation)


    extendedQueryJlh = es.search(
        index="reddit-mentalhealth",
        body = {
            "size":0,
            "query":{
                "query_string":{
                    "default_field":"selftext",
                    "query":"*drugs*"
                    }
                },
            "aggs": {
                "Terminos mas significativos": {
                    "significant_terms": {
                        "field":"selftext",
                        "size":25,
                        "exclude":["drugs", "drug", "recreational", "years", "life", "a", "about", "above", "after", "again", "against", "all", "also", "am", "an", "and", "another", "any", "are", "aren't", "as", "at", "back", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "even", "ever", "every", "few", "first", "five", "for", "four", "from", "further", "get", "go", "goes", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "high", "him", "himself", "his", "how", "how's", "however", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "just", "least", "less", "let's", "like", "long", "made", "make", "many", "me", "more", "most", "mustn't", "my", "myself", "never", "new", "no", "nor", "not", "now", "of", "off", "old", "on", "once", "one", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "put", "said", "same", "say", "says", "second", "see", "seen", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "since", "so", "some", "still", "such", "take", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "three", "through", "to", "too", "two", "under", "until", "up", "very", "was", "wasn't", "way", "we", "we'd", "we'll", "we're", "we've", "well", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "whether", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves","can"],
                        "jlh":{}
                    }
                 }
            }
        }
    )

    print("\n********** Resultados utilizando la metrica Jlh **********\n")
    pp.pprint(extendedQueryJlh)


if __name__ == '__main__':
    main()
