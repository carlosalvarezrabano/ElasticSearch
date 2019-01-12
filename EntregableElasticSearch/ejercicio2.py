#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Owner
#
# Created:     08/01/2019
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

    initialQueryMLT = es.search(
        body = {
            "size":0,
            "query":{
                "more_like_this":{
                    "fields": ["selftext"],
                    "like": "*drugs*",
                    "min_term_freq" : 1,
                    "max_query_terms" : 15
                    }
                }
            }
    )

    print("********** Resultados consulta inicial **********\n")
    pp.pprint(initialQueryMLT)
    print("Numero de resultados: " + str(initialQueryMLT["hits"]["total"]) + "\n")


    extendedQueryMLT = es.search(
        body = {
            "size":0,
            "query":{
                "more_like_this":{
                    "fields": ["selftext"],
                    "like" : [
                    {
                        "_index" : "reddit-mentalhealth",
                        "_type" : "text",
                        "_id" : "1"
                    },
                    "*drugs*","Alcohol","Weed","Cocaine"
                    ],
                    "min_term_freq" : 1,
                    "max_query_terms" : 15,
                    }
                },
            }
    )

    print("********** Resultados consulta expandida **********\n")
    pp.pprint(extendedQueryMLT)
    print("Numero de resultados: " + str(extendedQueryMLT["hits"]["total"]) + "\n")


    queryExtended ={
        "query":{
            "more_like_this":{
                "fields": ["selftext"],
                "like" : [
                {
                    "_index" : "reddit-mentalhealth",
                    "_type" : "text",
                    "_id" : "1"
                },
                "*drugs*","Alcohol","Weed","Cocaine"
                ],
                "min_term_freq" : 1,
                "max_query_terms" : 15,
            }
        },
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

    with open("mentalhealth-drugs-addition-MLT.json", 'w') as write_file:
        json.dump(data, write_file, indent=2)




if __name__ == '__main__':
    main()
