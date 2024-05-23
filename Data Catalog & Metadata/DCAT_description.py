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


from rdflib import DCTERMS, FOAF, PROV, RDFS, SKOS, BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, XSD, DCAT, NamespaceManager
import logging
import sys

# Namespaces
NDBI = Namespace("https://mgh.souhail.com/resources")
EUROVOC = Namespace("http://eurovoc.europa.eu/")
EUA = Namespace("http://publications.europa.eu/resource/authority/")
VCARD = Namespace("https://www.w3.org/TR/vcard-rdf/")
QB = Namespace("http://purl.org/linked-data/cube#")


def create_catalog_description() -> Graph:
    result = Graph()
    result.bind("ndbi", NDBI)
    result.bind("rdf", RDF)
    result.bind("xsd", XSD)
    result.bind("dcterms", DCTERMS)
    result.bind("vcard", VCARD)
    result.bind("eurovoc", EUROVOC)
    result.bind("qb", QB)

    contact_point = create_contact_point(result)
    create_catalog(result)
    create_publisher(result)
    create_dataset(result, contact_point)
    create_creator(result, contact_point)
    create_distributions(result)

    return result


def create_catalog(collector: Graph) -> None:
    catalog = NDBI.Catalog
    collector.add((catalog, RDF.type, DCAT.Catalog))
    collector.add((catalog, DCAT.dataset, NDBI.ShootingsDataset))
    collector.add((catalog, DCTERMS.publisher, NDBI.Publisher))


def create_contact_point(collector: Graph) -> BNode:
    contact_point = BNode()
    collector.add((contact_point, RDF.type, VCARD.Organization))
    collector.add((contact_point, VCARD.fn, Literal(
        "Boston Police Department", lang="en")))
    collector.add((contact_point, VCARD.hasEmail, URIRef(
        "mailto:policedashboard@boston.gov")))
    return contact_point


def create_dataset(collector: Graph, contact_point: BNode) -> None:
    shootings_dataset = NDBI.ShootingsDataset
    collector.add((shootings_dataset, RDF.type, DCAT.Dataset))
    collector.add((shootings_dataset, DCAT.distribution,
                  NDBI.ShootingsDatasetCSV))
    collector.add((shootings_dataset, DCAT.distribution,
                  NDBI.ShootingsDatasetTSV))
    collector.add((shootings_dataset, DCAT.distribution,
                  NDBI.ShootingsDatasetJSON))
    collector.add((shootings_dataset, DCAT.distribution,
                  NDBI.ShootingsDatasetXML))

    collector.add((shootings_dataset, DCTERMS.publisher, NDBI.Publisher))
    collector.add((shootings_dataset, DCTERMS.description, Literal(
        "The Shootings dashboard contains information on shooting incidents where a victim was struck by a bullet, either fatally or non-fatally; that occurred in the City of Boston and fall under Boston Police Department jurisdiction. The dashboard does not contain records for self-inflicted gunshot wounds or shootings determined to be justifiable. Information on the incident, and the demographics of victims are included. This information is updated based on analysis conducted by the Boston Regional Intelligence Center under the Boston Police Department Bureau of Intelligence and Analysis. The data is for 2015 forward, with a 7 day rolling delay to allow for analysis and data entry to occur.", lang="en")))
    collector.add((shootings_dataset, DCTERMS.accrualPeriodicity,
                  Literal("Weekly", lang="en")))
    collector.add((shootings_dataset, DCTERMS.temporal, Literal(
        "From 2015 forward, 7 day rolling delay", lang="en")))
    collector.add((shootings_dataset, DCTERMS.spatial,
                  Literal("Boston (all)", lang="en")))
    collector.add((shootings_dataset, DCTERMS.creator,
                  NDBI.BostonPoliceDepartment))
    collector.add((shootings_dataset, DCTERMS.issued,
                  Literal("2021-06-29", datatype=XSD.date)))
    collector.add((shootings_dataset, DCTERMS.modified,
                  Literal("2024-05-12", datatype=XSD.date)))
    collector.add((shootings_dataset, DCTERMS.license,
                  Literal("License not specified", lang="en")))

    collector.add((shootings_dataset, DCAT.contactPoint, contact_point))

    collector.add((shootings_dataset, DCTERMS.title,
                  Literal("Shootings", lang="en")))
    collector.add((shootings_dataset, DCAT.keyword,
                  Literal("District", lang="en")))
    collector.add((shootings_dataset, DCAT.keyword,
                  Literal("Gender", lang="en")))
    collector.add((shootings_dataset, DCAT.keyword,
                  Literal("Multiple Shootings", lang="en")))
    collector.add((shootings_dataset, DCAT.theme, EUROVOC['public_safety']))
    collector.add((shootings_dataset, DCAT.theme, EUROVOC['urban_violence']))
    collector.add((shootings_dataset, DCAT.theme, EUROVOC['law_enforcement']))
    collector.add((shootings_dataset, DCTERMS.spatial,
                  Literal("Boston, MA, USA", lang="en")))
    collector.add((shootings_dataset, DCTERMS.rights,
                  Literal("Copyright 2024, BOSTONGOV")))

    periodOfTime = BNode()
    collector.add((shootings_dataset, DCTERMS.temporal, periodOfTime))
    collector.add((periodOfTime, RDF.type, DCTERMS.PeriodOfTime))
    collector.add((periodOfTime, DCAT.startDate,
                  Literal("2024-01-01", datatype=XSD.date)))
    collector.add((periodOfTime, DCAT.endDate, Literal(
        "2022-12-31", datatype=XSD.date)))

    activity = BNode()
    collector.add((activity, RDF.type, PROV.Activity))
    collector.add((activity, PROV.startedAtTime, Literal(
        "2024-05-13T12:00:00Z", datatype=XSD.dateTime)))
    collector.add((activity, PROV.endedAtTime, Literal(
        "2024-05-13T13:00:00Z", datatype=XSD.dateTime)))

    agent = BNode()
    collector.add((agent, RDF.type, PROV.Agent))
    collector.add((agent, FOAF.name, Literal("Moughel Mohamed Souhail")))

    attribution = BNode()
    collector.add((attribution, RDF.type, PROV.Attribution))
    collector.add((attribution, PROV.agent, agent))
    collector.add((attribution, PROV.activity, activity))
    collector.add((attribution, PROV.endedAtTime, Literal(
        "2024-05-13T13:00:00Z", datatype=XSD.dateTime)))
    collector.add((shootings_dataset, PROV.qualifiedAttribution, attribution))

    collector.add((shootings_dataset, PROV.wasGeneratedBy, activity))


def create_creator(collector: Graph, contact_point: BNode) -> None:
    creator = NDBI.BostonPoliceDepartment
    collector.add((creator, RDF.type, FOAF.Organization))
    collector.add((creator, FOAF.name, Literal(
        "Boston Police Department", lang="en")))
    collector.add((creator, FOAF.mbox, URIRef(
        "mailto:policedashboard@boston.gov")))
    collector.add((creator, DCAT.contactPoint, contact_point))


def create_publisher(collector: Graph) -> None:
    publisher = NDBI.Publisher
    collector.add((publisher, RDF.type, FOAF.Organization))
    collector.add((publisher, RDFS.label, Literal(
        "Boston Government ,Department of Innovation and Technology", lang="en")))
    collector.add((publisher, FOAF.homepage,
                  URIRef("https://data.boston.gov")))


def create_distributions(collector: Graph) -> None:
    csv_distribution = NDBI.ShootingsDatasetCSV
    collector.add((csv_distribution, RDF.type, DCAT.Distribution))

    # Technical metadata
    collector.add((csv_distribution, DCAT.accessURL, URIRef(
        "https://data.boston.gov/dataset/e63a37e1-be79-4722-89e6-9e7e2a3da6d1/resource/73c7e069-701f-4910-986d-b950f46c91a1")))
    collector.add((csv_distribution, DCAT.downloadURL, URIRef(
        "https://data.boston.gov/datastore/dump/73c7e069-701f-4910-986d-b950f46c91a1?bom=True")))
    collector.add((csv_distribution, RDF.type, DCAT.Distribution))
    collector.add((csv_distribution, DCTERMS.description, Literal("CSV format distribution of shooting incidents data, suitable for data analysis and reporting.", lang="en")))
    collector.add((csv_distribution, DCAT.mediaType, URIRef(
        "https://www.iana.org/assignments/media-types/text/csv")))
    collector.add((csv_distribution, DCAT.byteSize, Literal(
        "195021", datatype=XSD.nonNegativeInteger)))

    tsv_distribution = NDBI.ShootingsDatasetTSV
    collector.add((tsv_distribution, RDF.type, DCAT.Distribution))

    # Technical metadata
    collector.add((tsv_distribution, DCAT.accessURL, URIRef(
        "https://data.boston.gov/dataset/e63a37e1-be79-4722-89e6-9e7e2a3da6d1/resource/73c7e069-701f-4910-986d-b950f46c91a1")))
    collector.add((tsv_distribution, DCAT.downloadURL, URIRef(
        "https://data.boston.gov/datastore/dump/73c7e069-701f-4910-986d-b950f46c91a1?format=tsv&bom=True")))
    collector.add((tsv_distribution, DCAT.mediaType, URIRef(
        "https://www.iana.org/assignments/media-types/text/tab-separated-values")))
    collector.add((tsv_distribution, RDF.type, DCAT.Distribution))
    collector.add((tsv_distribution, DCTERMS.description, Literal("TSV format distribution of shooting incidents data, suitable for data analysis and reporting.", lang="en")))
    collector.add((tsv_distribution, DCAT.byteSize, Literal(
        "195021", datatype=XSD.nonNegativeInteger)))

    json_distribution = NDBI.ShootingsDatasetJSON
    collector.add((json_distribution, RDF.type, DCAT.Distribution))

    # Technical metadata
    collector.add((json_distribution, DCAT.accessURL, URIRef(
        "https://data.boston.gov/dataset/e63a37e1-be79-4722-89e6-9e7e2a3da6d1/resource/73c7e069-701f-4910-986d-b950f46c91a1")))
    collector.add((json_distribution, DCAT.downloadURL, URIRef(
        "https://data.boston.gov/datastore/dump/73c7e069-701f-4910-986d-b950f46c91a1?format=json")))
    collector.add((json_distribution, DCAT.mediaType, URIRef(
        "https://www.iana.org/assignments/media-types/application/json")))
    collector.add((json_distribution, RDF.type, DCAT.Distribution))
    collector.add((json_distribution, DCTERMS.description, Literal("JSON format distribution of shooting incidents data, suitable for data analysis and reporting.", lang="en")))
    collector.add((json_distribution, DCAT.byteSize, Literal(
        "236488", datatype=XSD.nonNegativeInteger)))

    xml_distribution = NDBI.ShootingsDatasetXML
    collector.add((xml_distribution, RDF.type, DCAT.Distribution))

    # Technical metadata
    collector.add((xml_distribution, DCAT.accessURL, URIRef(
        "https://data.boston.gov/dataset/e63a37e1-be79-4722-89e6-9e7e2a3da6d1/resource/73c7e069-701f-4910-986d-b950f46c91a1")))
    collector.add((xml_distribution, DCAT.downloadURL, URIRef(
        "https://data.boston.gov/datastore/dump/73c7e069-701f-4910-986d-b950f46c91a1?format=xml")))
    collector.add((xml_distribution, DCAT.mediaType, URIRef(
        "https://www.iana.org/assignments/media-types/text/xml")))
    collector.add((xml_distribution, RDF.type, DCAT.Distribution))
    collector.add((xml_distribution, DCTERMS.description, Literal("XML format distribution of shooting incidents data, suitable for data analysis and reporting.", lang="en")))
    collector.add((xml_distribution, DCAT.byteSize, Literal(
        "706600", datatype=XSD.nonNegativeInteger)))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("Usage: python script.py <output_file_path>")
        sys.exit(1)

    data_catalog = create_catalog_description()
    data_catalog.serialize(format="turtle", destination=sys.argv[1])
