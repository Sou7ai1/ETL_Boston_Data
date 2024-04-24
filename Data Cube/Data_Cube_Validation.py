from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON
import sys

g = Graph()
data_cube_path = input("Enter the path to the Data Cube: ")
g.parse(data_cube_path, format="xml")

sparql_endpoint = input("Enter the SPARQL endpoint URL: ")
sparql = SPARQLWrapper(sparql_endpoint)

constraints = [
    {
        "name": "IC-1: Unique DataSet",
        "query": """
            ASK {
                {
                    ?obs a qb:Observation .
                    FILTER NOT EXISTS { ?obs qb:dataSet [] }
                } UNION {
                    ?obs a qb:Observation ;
                         qb:dataSet ?dataset1, ?dataset2 .
                    FILTER (?dataset1 != ?dataset2)
                }
            }
        """
    },
    {
        "name": "IC-2: Unique DSD",
        "query": """
            ASK {
                {
                    ?dataset a qb:DataSet .
                    FILTER NOT EXISTS { ?dataset qb:structure [] }
                } UNION { 
                    ?dataset a qb:DataSet ;
                             qb:structure ?dsd1, ?dsd2 .
                    FILTER (?dsd1 != ?dsd2)
                }
            }
        """
    },
    {
        "name": "IC-3: DSD includes measure",
        "query": """
            ASK {
                ?dsd a qb:DataStructureDefinition .
                FILTER NOT EXISTS { ?dsd qb:component [qb:componentProperty [a qb:MeasureProperty]] }
            }
        """
    },
    {
        "name": "IC-4: Dimensions have range",
        "query": """
            ASK {
                ?dim a qb:DimensionProperty .
                FILTER NOT EXISTS { ?dim rdfs:range [] }
            }
        """
    },
    {
        "name": "IC-5: Concept dimensions have code lists",
        "query": """
            ASK {
                ?dim a qb:DimensionProperty ;
                     rdfs:range skos:Concept .
                FILTER NOT EXISTS { ?dim qb:codeList [] }
            }
        """
    },
    {
        "name": "IC-7: Slice Keys must be declared",
        "query": """
            ASK {
                ?sliceKey a qb:SliceKey .
                FILTER NOT EXISTS { [a qb:DataStructureDefinition] qb:sliceKey ?sliceKey }
            }
        """
    },
    {
        "name": "IC-8: Slice Keys consistent with DSD",
        "query": """
            ASK {
                ?slicekey a qb:SliceKey;
                          qb:componentProperty ?prop .
                ?dsd qb:sliceKey ?slicekey .
                FILTER NOT EXISTS { ?dsd qb:component [qb:componentProperty ?prop] }
            }
        """
    },

]


def validate_data_cube(g, constraints):
    for constraint in constraints:
        sparql.setQuery(constraint["query"])
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        if results["boolean"]:
            print(f"Constraint '{constraint['name']}' violated.")
        else:
            print(f"Constraint '{constraint['name']}' satisfied.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        data_cube_path = sys.argv[1]
    else:
        data_cube_path = input("Enter the path to the Data Cube: ")
    
    g.parse(data_cube_path, format="xml")
    
    if len(sys.argv) > 2:
        sparql_endpoint = sys.argv[2]
    else:
        sparql_endpoint = input("Enter the SPARQL endpoint URL: ")
    
    sparql = SPARQLWrapper(sparql_endpoint)
    
    validate_data_cube(g, constraints)
