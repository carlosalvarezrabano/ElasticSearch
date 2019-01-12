#-------------------------------------------------------------------------------
# Name:        Ejercicio 3
# Author:      Alejandro
#
# Created:     08/01/2019
# Copyright:   (c) alexr 2019
#-------------------------------------------------------------------------------
# Para poder usar la función print e imprimir sin saltos de línea
from __future__ import print_function

import json # Para poder trabajar con objetos JSON
import pprint # Para poder hacer uso de PrettyPrinter
import sys # Para poder usar exit

from SPARQLWrapper import SPARQLWrapper, JSON
from elasticsearch import Elasticsearch

def main():
    # Queremos imprimir bonito
    pp = pprint.PrettyPrinter(indent=2)

    wikidataMedicationList=[]
    verificatedMedicationList=[]

    # Obtenemos la lista de medicamentos de Wikidata
    endpoint_url = "https://query.wikidata.org/sparql"

    query = """SELECT ?instance_of ?instance_ofLabel WHERE {
    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    ?instance_of wdt:P31 wd:Q12140.
    }
    LIMIT 10000"""

    def get_wikidata_results(endpoint_url, query):
        sparql = SPARQLWrapper(endpoint_url)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()

    results = get_wikidata_results(endpoint_url, query)

    # Rellenamos la lista solo con el nombre del medicamento
    for result in results["results"]["bindings"]:
        wikidataMedicationList.append(result["instance_ofLabel"]["value"])


    # Nos conectamos por defecto a localhost:9200
    es = Elasticsearch()

    # Consulta inicial para conseguir los subreddits mas relevantes
    results = es.search(
        index="reddit-mentalhealth",
        body = {
            "size": 0,
            "query": {
                "query_string": {
                    "default_field": "selftext",
                    "query": "*medicat* OR *prescri*"
                }
            },
            "aggs": {
                "Subreddits significativos": {
                    "significant_terms": {
                        "field": "subreddit",
                        "size": 20
                    }
                }
            }
        }
        )

    subreddits = results["aggregations"]["Subreddits significativos"]["buckets"]

    # Consulta por subreddit
    def queryBySubreddit(sub):
        body = {
            "size": 0,
            "query": {
                "query_string": {
                    "query": "*medicat* OR *prescri* AND " + sub,
                    "fields": ["selftext", "subreddit"]
                }
            },
            "aggs": {
                "Terminos mas significativos": {
                    "significant_terms": {
                        "field": "selftext",
                        "size": 1000
                    }
                }
            }
        }
        results = es.search(
        index = "reddit-mentalhealth",
        body = body
        )
        terms = results["aggregations"]["Terminos mas significativos"]["buckets"]
        for i in range(len(terms)):
            print("\t" + terms[i]["key"])
            if terms[i]["key"] in wikidataMedicationList:
                verificatedMedicationList.append(terms[i]["key"])


    for i in range(len(subreddits)):
        print("")
        print("SUBREDDIT ---> " + str(subreddits[i]["key"]) + "\t\t\tScore: " + str(subreddits[i]["score"]))
        print("Terminos mas signicativos:")
        queryBySubreddit(subreddits[i]["key"])
        print("")


    # Eliminamos duplicados
    verificatedMedicationList = set(verificatedMedicationList)
    print(verificatedMedicationList)

    # Lo exportamos a un archivo JSON
    with open("mentalhealth-medications.json", 'w') as write_file:
        json.dump(list(verificatedMedicationList), write_file, indent=2)

if __name__ == '__main__':
    main()