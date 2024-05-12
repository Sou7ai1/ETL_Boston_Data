# Copyright (c) 2024 Moughel Mohamed Souhail
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.


from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, FOAF, XSD, PROV
import logging
import sys

NS = Namespace(f"https://mgh.soughail.com/ontology#")


def create_prov_data() -> Graph:
    """
    Generate provenance data graph.

    Returns:
        Graph: RDFLib graph containing provenance data.
    """
    result = Graph()
    result.bind("", NS)
    result.bind("rdf", RDF)
    result.bind("xsd", XSD)

    create_entities(result)
    create_agents(result)
    create_activities(result)

    return result


def create_entities(collector: Graph) -> None:
    """
    Create entities in the provenance data graph.

    Args:
        collector (Graph): The RDFLib graph to add entities to.
    """
    # Original datasets
    dataset_crimes = NS.KaggleCrimesDataset
    collector.add((dataset_crimes, RDF.type, PROV.Entity))
    collector.add((dataset_crimes, PROV.wasAttributedTo, NS.Kaggle))
    collector.add((dataset_crimes, PROV.atLocation, Literal(
        "https://www.kaggle.com/datasets/AnalyzeBoston/crimes-in-boston", datatype=XSD.anyURI)))

    dataset_shootings = NS.BostonDatasetShootings
    collector.add((dataset_shootings, RDF.type, PROV.Entity))
    collector.add((dataset_shootings, PROV.wasAttributedTo, NS.BostonGOV))
    collector.add((dataset_shootings, PROV.atLocation, Literal(
        "https://data.boston.gov/dataset/e63a37e1-be79-4722-89e6-9e7e2a3da6d1/resource/73c7e069-701f-4910-986d-b950f46c91a1/download/tmp8mntlmrz.csv", datatype=XSD.anyURI)))

    dataset_weather = NS.BostonDatasetWeather
    collector.add((dataset_weather, RDF.type, PROV.Entity))
    collector.add((dataset_weather, PROV.wasAttributedTo, NS.Meteo))
    collector.add((dataset_weather, PROV.atLocation, Literal(
        "https://meteostat.net/fr/place/us/boston?s=72509&t=2015-01-01/2019-01-01", datatype=XSD.anyURI)))


    dim_Crimes = NS.KaggleSpeciCrimes
    collector.add((dim_Crimes, RDF.type, PROV.Entity))
    collector.add((dim_Crimes, PROV.wasGeneratedBy, NS.ApacheAirflowActivity))
    collector.add((dim_Crimes, PROV.wasDerivedFrom, NS.KaggleCrimesDataset))
    collector.add((dim_Crimes, PROV.wasAttributedTo, NS.MoughelSouhail))

    collector.add((dim_Crimes, PROV.hadPrimarySource, NS.KaggleCrimesDataset))

    dim_Shootings = NS.BostonShootings
    collector.add((dim_Shootings, RDF.type, PROV.Entity))
    collector.add((dim_Shootings, PROV.wasGeneratedBy,
                  NS.ApacheAirflowActivity))
    collector.add((dim_Shootings, PROV.wasDerivedFrom,
                  NS.BostonDatasetShootings))
    collector.add((dim_Shootings, PROV.wasAttributedTo, NS.MoughelSouhail))
    collector.add((dim_Shootings, PROV.hadPrimarySource,
                  NS.BostonDatasetShootings))  

    dim_CrimesWeather = NS.BostonCrimesWeather
    collector.add((dim_CrimesWeather, RDF.type, PROV.Entity))
    collector.add((dim_CrimesWeather, PROV.wasGeneratedBy,
                  NS.ApacheAirflowActivity))
    collector.add(
        (dim_CrimesWeather, PROV.wasInfluencedBy, NS.KaggleCrimesDataset))
    collector.add((dim_CrimesWeather, PROV.wasInfluencedBy,
                  NS.BostonDatasetWeather))
    collector.add((dim_CrimesWeather, PROV.wasAttributedTo, NS.MoughelSouhail))
    collector.add((dim_CrimesWeather, PROV.hadPrimarySource,
                  NS.KaggleCrimesDataset)) 
    collector.add((dim_CrimesWeather, PROV.hadPrimarySource,
                  NS.BostonDatasetWeather))  


def create_agents(collector: Graph) -> None:
    """
    Create agents in the provenance data graph.

    Args:
        collector (Graph): The RDFLib graph to add agents to.
    """

    author = NS.MoughelSouhail
    collector.add((author, RDF.type, PROV.Person))
    collector.add((author, RDF.type, PROV.Agent))
    collector.add((author, PROV.actedOnBehalfOf, NS.MFF_UK))
    collector.add((author, FOAF.givenName, Literal(
        "Student Moughel Mohamed Souhail", lang="en")))
    collector.add((author, FOAF.mbox, URIRef("mailto:m.souhail03@gmail.com")))

    organization = NS.MFF_UK
    collector.add((organization, RDF.type, PROV.Organization))
    collector.add((organization, RDF.type, PROV.Agent))
    collector.add((organization, FOAF.name, Literal(
        "Matematicko-fyzikální fakulta, Univerzita Karlova", lang="cs")))
    collector.add((organization, FOAF.schoolHomepage, Literal(
        "https://www.mff.cuni.cz/", datatype=XSD.anyURI)))

    apache_airflow = NS.ApacheAirflow
    collector.add((apache_airflow, RDF.type, PROV.SoftwareAgent))
    collector.add((apache_airflow, RDF.type, PROV.Agent))  # SoftwareAgent
    collector.add((apache_airflow, PROV.actedOnBehalfOf, NS.MoughelSouhail))
    collector.add((apache_airflow, FOAF.name,
                  Literal("Apache Airflow", lang="en")))

    Tableau = NS.Tableau
    collector.add((Tableau, RDF.type, PROV.SoftwareAgent))
    collector.add((Tableau, RDF.type, PROV.Agent))  # SoftwareAgent
    collector.add((Tableau, PROV.actedOnBehalfOf, NS.MoughelSouhail))
    collector.add((Tableau, FOAF.name,
                  Literal("Tableau", lang="en")))

    kaggle = NS.Kaggle
    collector.add((kaggle, RDF.type, PROV.Organization))
    collector.add((kaggle, RDF.type, PROV.Agent))
    collector.add((kaggle, FOAF.name, Literal(
        "Kaggle, The online community of data scientists and machine learning engineers", lang="en")))
    collector.add((kaggle, FOAF.homepage, Literal(
        "https://www.kaggle.com", datatype=XSD.anyURI)))

    bostonGOV = NS.Boston
    collector.add((bostonGOV, RDF.type, PROV.Organization))
    collector.add((bostonGOV, RDF.type, PROV.Agent))
    collector.add((bostonGOV, FOAF.name, Literal(
        "ANALYZE BOSTON, BOSTON'S OPEN DATA HUB", lang="en")))
    collector.add((bostonGOV, FOAF.homepage, Literal(
        "https://data.boston.gov", datatype=XSD.anyURI)))

    Weather = NS.Meteo
    collector.add((Weather, RDF.type, PROV.Organization))
    collector.add((Weather, RDF.type, PROV.Agent))
    collector.add((Weather, FOAF.name, Literal(
        "The Weather's Record Keeper", lang="en")))
    collector.add((Weather, FOAF.homepage, Literal(
        "https://meteostat.net", datatype=XSD.anyURI)))


def create_activities(collector: Graph) -> None:
    """
    Create activities in the provenance data graph.

    Args:
        collector (Graph): The RDFLib graph to add activities to.
    """
    airflow_activity = NS.ApacheAirflowActivity
    collector.add((airflow_activity, RDF.type, PROV.Activity))
    collector.add((airflow_activity, PROV.startedAtTime, Literal(
        "2024-05-05T12:00:00", datatype=XSD.dateTime)))
    collector.add((airflow_activity, PROV.endedAtTime, Literal(
        "2024-05-05T12:01:00", datatype=XSD.dateTime)))

    usage_boston_shootings = BNode()
    collector.add((usage_boston_shootings, RDF.type, PROV.Usage))
    collector.add((usage_boston_shootings, PROV.entity,
                  NS.BostonDatasetShootings))
    collector.add((usage_boston_shootings, PROV.hadRole, NS.shootingDataUsage))
    collector.add((usage_boston_shootings, PROV.activity, airflow_activity))
    collector.add(
        (airflow_activity, PROV.qualifiedAssociation, usage_boston_shootings))

    usage_boston_weather = BNode()
    collector.add((usage_boston_weather, RDF.type, PROV.Usage))
    collector.add((usage_boston_weather, PROV.entity, NS.BostonCrimesWeather))
    collector.add((usage_boston_weather, PROV.hadRole, NS.weatherDataUsage))
    collector.add((usage_boston_weather, PROV.activity, airflow_activity))
    collector.add(
        (airflow_activity, PROV.qualifiedAssociation, usage_boston_weather))

    collector.add((airflow_activity, PROV.wasAssociatedWith, NS.ApacheAirflow))

    tableau_activity = NS.TableauActivity
    collector.add((tableau_activity, RDF.type, PROV.Activity))
    collector.add((tableau_activity, PROV.startedAtTime, Literal(
        "2024-05-05T12:00:00", datatype=XSD.dateTime)))
    collector.add((tableau_activity, PROV.endedAtTime, Literal(
        "2024-05-05T12:01:00", datatype=XSD.dateTime)))

    tableau_usage_boston_shootings = BNode()
    collector.add((tableau_usage_boston_shootings, RDF.type, PROV.Usage))
    collector.add((tableau_usage_boston_shootings,
                  PROV.entity, NS.BostonDatasetShootings))
    collector.add((tableau_usage_boston_shootings,
                  PROV.hadRole, NS.visualizationDataUsage))
    collector.add((tableau_usage_boston_shootings,
                  PROV.activity, tableau_activity))
    collector.add((tableau_activity, PROV.qualifiedAssociation,
                  tableau_usage_boston_shootings))

    # Create qualified association for the usage of Boston Crimes Weather in Tableau
    tableau_usage_boston_weather = BNode()
    collector.add((tableau_usage_boston_weather, RDF.type, PROV.Usage))
    collector.add((tableau_usage_boston_weather,
                  PROV.entity, NS.BostonCrimesWeather))
    collector.add((tableau_usage_boston_weather,
                  PROV.hadRole, NS.visualizationDataUsage))
    collector.add((tableau_usage_boston_weather,
                  PROV.activity, tableau_activity))
    collector.add((tableau_activity, PROV.qualifiedAssociation,
                  tableau_usage_boston_weather))

    collector.add((tableau_activity, PROV.wasAssociatedWith, NS.Tableau))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("Usage: python script.py <output_file_path>")
        sys.exit(1)

    prov_data = create_prov_data()
    prov_data.serialize(format="trig", destination=sys.argv[1])
