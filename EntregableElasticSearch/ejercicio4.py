# Para poder usar la funcion print e imprimir sin saltos de linea
from __future__ import print_function

import json # Para poder trabajar con objetos JSON
import pprint # Para poder hacer uso de PrettyPrinter
import sys # Para poder usar exit

from elasticsearch import Elasticsearch #servidor de ElasticSearch
from elasticsearch import helpers  #Contiene el bulk-helper

def main():
    #Nos conectamos a localhost:9200
    es = Elasticsearch()
    #Lanzamos una primera consulta para encontrar factores comorbidos relacionados
    #tanto con tendencias suicidas como con conductas autolesivas
    results_comorbidos = helpers.scan(es,
    index="reddit-mentalhealth",
    doc_type="post",
    query={
    "size": 0,
        "query": {
            "query_string": {
                "default_field": "title",
                "query": "\"self harm\" OR suicide OR suicidal OR \"kill myself\" OR \"killing myself\" OR \"end my life\""
            }
        },
        "aggs": {
            "Terminos mas significativos": {
                "significant_terms": {
                    "field": "selftext",
                    "size": 100,
                    "gnd": {}
                }
            }
        }
    })
    f=open("comorbilidad-dump.txt","w")
    #Escribimos en este fichero toda la informacion (o gran parte de ella)
    #sobre los factores comorbidos (en este documento solo aparecen los mas generales)
    for hit in results_comorbidos:
        f.write(hit["_source"]["selftext"].encode("utf8"))
    f.close()
    #Despues de haber encontrado parte de dichos factores (depresion,
    #bullying, ansiedad, trastorno por deficit de atencion con hiperactividad...),
    #lanzamos una segunda consulta para encontrar mas factores
    results = helpers.scan(es,
    index="reddit-mentalhealth",
    doc_type="post",
    query={
    "size": 0,
        "query": {
            "query_string": {
                "default_field": "title",
                "query": "pills OR alcohol OR drugs OR comorbidity OR comorb OR anxiety OR adhd OR depression OR bullying"
            }
        },
        "aggs": {
            "Terminos mas significativos": {
                "significant_terms": {
                    "field": "domain",
                    "size": 100,
                    "gnd": {}
                }
            }
        }
    })
    file=open("enfermedades-dump.txt","w")
    #En este segundo documento escribimos toda la informacion (o gran parte de ella)
    #sobre factores mas especializados o especificos sobre dichos factores, es decir,
    #en este segundo documento encontraremos un mayor abanico de enfermedades
    #relacionadas tanto con tendencias suicidas como con conductas autolesivas
    for hit in results:
        file.write(hit["_source"]["title"].encode("utf8"))
    file.close()

if __name__ == '__main__':
    main()
