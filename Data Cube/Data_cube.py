import urllib.parse
import sys
import pandas as pd
from rdflib import Graph, BNode, Literal, Namespace, URIRef
from rdflib.namespace import RDF, QB, XSD, SKOS, DCTERMS, OWL
import logging
import urllib.parse
from sqlalchemy import create_engine

CUSTOM_PREFIX = "mgh.soughail.com"
NS = Namespace(f"https://{CUSTOM_PREFIX}/ontology#")
NSR = Namespace(f"https://{CUSTOM_PREFIX}/resources#")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
SDMX_DIMENSION = Namespace("http://purl.org/linked-data/sdmx/2009/dimension#")
SDMX_CONCEPT = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")
SDMX_CODE = Namespace("http://purl.org/linked-data/sdmx/2009/code#")


def load_csv_file_as_dataframe(file_path: str) -> pd.DataFrame:
    """
    Load a CSV file as a pandas DataFrame.

    Args:
        file_path (str): The file path of the CSV file.

    Returns:
        pd.DataFrame: The pandas DataFrame containing the data from the CSV file.
    """
    try:
        result = pd.read_csv(file_path, low_memory=False)
        logging.info("CSV file loaded successfully.")
        return result
    except Exception as e:
        logging.error(f"Error loading CSV file: {e}")
        return None


def fetch_data_to_csv(engine_url: str, sql_query: str, output_file: str) -> None:
    """
    Fetch data from a SQL database using SQLAlchemy engine and save it as a CSV file.

    Args:
        engine_url (str): The SQLAlchemy engine URL.
        sql_query (str): The SQL query to execute.
        output_file (str): The path to the output CSV file.
    """
    try:
        # Create a SQLAlchemy engine
        engine = create_engine(engine_url)

        # Connect to the database
        conn = engine.raw_connection()

        # Execute the SQL query and fetch the results into a pandas DataFrame
        df = pd.read_sql_query(sql_query, conn)

        # Save the DataFrame as a CSV file
        df.to_csv(output_file, index=False)

        # Close the connection
        conn.close()

    except Exception as e:
        logging.error(f"Error fetching data to CSV: {e}")


def create_dimensions(collector: Graph) -> list:
    """
    Create dimension properties in the RDF Graph.

    Args:
        collector (Graph): The RDF Graph to which the dimension properties will be added.

    Returns:
        list: A list containing dimension resources.
    """
    try:
        dimensions = []

        race = NS.race
        collector.add((race, RDF.type, RDFS.Property))
        collector.add((race, RDF.type, QB.DimensionProperty))
        collector.add((race, RDFS.label, Literal("race", lang="en")))
        collector.add((race, RDFS.range, SKOS.Concept))
        collector.add((race, QB.codeList, SDMX_CODE.race))
        gender = NS.gender
        collector.add((gender, RDF.type, RDFS.Property))
        collector.add((gender, RDF.type, QB.DimensionProperty))
        collector.add((gender, RDFS.label, Literal("gender", lang="en")))
        collector.add((gender, RDFS.range, XSD.string))
        collector.add((gender, QB.codeList, SDMX_CODE.gender))

        district = NS.district
        collector.add((district, RDF.type, RDFS.Property))
        collector.add((district, RDFS.label, Literal("District", lang="en")))
        collector.add((district, RDFS.range, XSD.string))
        collector.add((district, QB.codeList, SDMX_CODE.district))

        shooting_type = NS.shooting_type
        collector.add((shooting_type, RDF.type, RDFS.Property))
        collector.add((shooting_type, RDF.type, QB.DimensionProperty))
        collector.add((shooting_type, RDFS.label,
                      Literal("Shooting Type", lang="en")))
        collector.add((shooting_type, RDFS.range, XSD.integer))
        collector.add((shooting_type, QB.codeList, SDMX_CODE.shooting_type))

        return dimensions
    except Exception as e:
        logging.error(f"Error creating dimensions: {e}")
        return []


def create_measures(collector: Graph) -> list:
    """
    Create measure properties in the RDF Graph.

    Args:
        collector (Graph): The RDF Graph to which the measure properties will be added.

    Returns:
        list: A list of measure properties.
    """
    try:
        multiple_victims = NS.multiple_victims
        collector.add((multiple_victims, RDF.type, QB.MeasureProperty))
        collector.add((multiple_victims, RDFS.label,
                      Literal("Multiple Victims", lang="en")))
        collector.add((multiple_victims, RDFS.range, XSD.int))

        return [multiple_victims]

    except Exception as e:
        logging.error(f"Error creating measures: {e}")
        return []


def create_structure(graph: Graph, dimensions: list, measures: list, slice_key=None) -> BNode:
    """
    Create the structure of the Data Cube in the RDF Graph.

    Args:
        graph (Graph): The RDF Graph to which the structure will be added.
        dimensions (list): A list of dimension properties.
        measures (list): A list of measure properties.
        slice_key (URIRef, optional): The slice key for the Data Cube.

    Returns:
        BNode: The structure of the Data Cube.
    """
    try:
        structure = BNode()
        graph.add((structure, RDF.type, QB.DataStructureDefinition))

        # Add dimensions to the structure
        for index, dimension in enumerate(dimensions):
            component = BNode()
            graph.add((structure, QB.component, component))
            graph.add((component, QB.dimension, dimension))
            graph.add((component, QB.order, Literal(index + 1)))

        # Add measures to the structure
        for index, measure in enumerate(measures):
            component = BNode()
            graph.add((structure, QB.component, component))
            graph.add((component, QB.measure, measure))
            graph.add((component, QB.order, Literal(index + len(dimensions) + 1)))

        if slice_key:
            graph.add((structure, QB.sliceKey, slice_key))

        return structure

    except Exception as e:
        logging.error(f"Error creating structure: {e}")
        return None



def create_dataset(graph: Graph, structure_ref: URIRef, slice_ref: URIRef) -> URIRef:
    try:
        dataset = NSR.dataCubeInstance
        graph.add((dataset, RDF.type, QB.DataSet))
        graph.add((dataset, RDFS.label, Literal(
            "Shootings Data Cube", lang="en")))
        graph.add((dataset, QB.structure, structure_ref))
        graph.add((dataset, QB.slice, slice_ref))
        graph.add((dataset, DCTERMS.publisher,
                  Literal("Moughel Souhail", lang="en")))
        graph.add((dataset, DCTERMS.issued, Literal(
            "2024-04-19", datatype=XSD.date)))
        graph.add((dataset, DCTERMS.modified, Literal(
            "2024-04-19", datatype=XSD.date)))
        graph.add((dataset, DCTERMS.title, Literal(
            "Shootings Data Cube", lang="en")))

        return dataset
    except Exception as e:
        logging.error(f"Error creating dataset: {e}")
        return None


def as_data_cube(data: pd.DataFrame) -> Graph:
    try:
        result = Graph()

        # Bind namespaces to prefixes
        result.bind("ndbi", NS)
        result.bind("ndbi-r", NSR)
        result.bind("rdfs", RDFS)
        result.bind("rdf", RDF)
        result.bind("xsd", XSD)
        result.bind("owl", OWL)
        result.bind("sdmx-code", SDMX_CODE)
        result.bind("sdmx-dimension", SDMX_DIMENSION)

        # Define dimensions and measures
        dimensions = create_dimensions(result)
        measures = create_measures(result)

        # Create slice key
        slice_key = NSR["sliceByGender"]
        for dimension in dimensions:
            if dimension == NS.gender:
                result.add((slice_key, RDF.type, QB.SliceKey))
                result.add((slice_key, QB.componentProperty, dimension))
                break

        # Create structure
        structure = create_structure(result, dimensions, measures, slice_key)

        # Create dataset
        dataset = create_dataset(result, structure, slice_key)

        # Create slice
        create_slice(result, dataset, data)

        # Create observations
        create_observations(result, dataset, data)

        logging.info("Data Cube creation successful.")
        return result
    except Exception as e:
        logging.error(f"Error creating Data Cube: {e}")
        return None


def create_observations(collector: Graph, dataset, data: pd.DataFrame):
    """
    Create observations in the RDF Graph.

    Args:
        collector (Graph): The RDF Graph to which the observations will be added.
        dataset: The dataset of the Data Cube.
        data (pd.DataFrame): The pandas DataFrame containing the data.
    """
    try:
        for index, row in data.iterrows():
            resource = NSR["observation-" + str(index).zfill(3)]

            collector.add((resource, RDF.type, QB.Observation))
            collector.add((resource, QB.dataSet, dataset))
            collector.add((resource, NS.gender, Literal(
                row['gender'], datatype=XSD.string)))

            # Dimension: race
            collector.add((resource, NS.race, Literal(
                row['race'], datatype=XSD.string)))

            # Dimension: District
            collector.add((resource, NS.district, Literal(
                row['district'], datatype=XSD.string)))

            # Measures
            collector.add((resource, NS.multiple_victims, Literal(
                row['multiple_victims'], datatype=XSD.int)))

        logging.info("Observations created successfully.")
    except Exception as e:
        logging.error(f"Error creating observations: {e}")


def create_concept_schemes(collector: Graph) -> None:
    """
    Create concept schemes in the Data Cube.

    Args:
        collector (Graph): The Data Cube graph to add concept schemes to.
    """
    try:
        district_scheme = SDMX_CODE.district
        collector.add((district_scheme, RDF.type, SKOS.ConceptScheme))
        collector.add((district_scheme, SKOS.prefLabel,
                      Literal("District", lang="en")))
        collector.add((district_scheme, RDFS.label,
                      Literal("District", lang="en")))
        collector.add((district_scheme, SKOS.note, Literal(
            "This code list provides a list of districts in Czechia.", lang="en")))
        collector.add((district_scheme, RDFS.seeAlso, NSR.district))

        race_scheme = SDMX_CODE.race
        collector.add((race_scheme, RDF.type, SKOS.ConceptScheme))
        collector.add((race_scheme, SKOS.prefLabel,
                      Literal("race", lang="en")))
        collector.add((race_scheme, RDFS.label, Literal("race", lang="en")))
        collector.add((race_scheme, SKOS.note, Literal(
            "This code list provides a list of human races.", lang="en")))
        collector.add((race_scheme, RDFS.seeAlso, NSR.race))
    except Exception as e:
        logging.error(f"Error creating concept schemes: {e}")


def create_concept_classes(collector: Graph) -> None:
    """
    Create concept classes in the Data Cube.

    Args:
        collector (Graph): The Data Cube graph to add resource classes to.
    """
    try:
        district_class = SDMX_CODE.District
        collector.add((district_class, RDF.type, RDFS.Class))
        collector.add((district_class, RDF.type, OWL.Class))
        collector.add((district_class, RDFS.subClassOf, SKOS.Concept))
        collector.add((district_class, RDFS.label,
                      Literal("District", lang="en")))
        collector.add((district_class, SKOS.prefLabel,
                      Literal("District", lang="en")))
        collector.add((district_class, RDFS.seeAlso, SDMX_CODE.district))

        race_class = SDMX_CODE.race
        collector.add((race_class, RDF.type, RDFS.Class))
        collector.add((race_class, RDF.type, OWL.Class))
        collector.add((race_class, RDFS.subClassOf, SKOS.Concept))
        collector.add((race_class, RDFS.label, Literal("race", lang="en")))
        collector.add((race_class, SKOS.prefLabel, Literal("race", lang="en")))
        collector.add((race_class, RDFS.seeAlso, SDMX_CODE.race))
    except Exception as e:
        logging.error(f"Error creating resource classes: {e}")


def create_concepts(collector: Graph, data: pd.DataFrame) -> None:
    """
    Create concepts in the Data Cube based on input DataFrame.

    Args:
        collector (Graph): The Data Cube graph to add resources to.
        data (pd.DataFrame): The DataFrame containing the data.

    Returns:
        None.
    """
    try:
        districts = data.drop_duplicates(subset=["district"])[["district"]]
        for _, district_row in districts.iterrows():
            district_resource = NSR[f"district/{
                urllib.parse.quote(district_row['district'])}"]
            collector.add((district_resource, RDF.type, SKOS.Concept))
            collector.add((district_resource, RDF.type, SDMX_CODE.District))
            collector.add(
                (district_resource, SKOS.topConceptOf, SDMX_CODE.District))
            collector.add((district_resource, SKOS.prefLabel,
                          Literal(district_row["district"], lang="en")))
            collector.add(
                (district_resource, SKOS.inScheme, SDMX_CODE.district))

        races = data.drop_duplicates(subset=["race"])[["race"]]
        for _, race_row in races.iterrows():
            race_resource = NSR[f"race/{urllib.parse.quote(race_row['race'])}"]
            collector.add((race_resource, RDF.type, SKOS.Concept))
            collector.add((race_resource, RDF.type, SDMX_CODE.race))
            collector.add((race_resource, SKOS.topConceptOf, SDMX_CODE.race))
            collector.add((race_resource, SKOS.prefLabel,
                          Literal(race_row["race"], lang="en")))
            collector.add((race_resource, SKOS.inScheme, SDMX_CODE.race))
    except Exception as e:
        logging.error(f"Error creating resources: {e}")


def create_slice(collector: Graph, dataset: URIRef, data: pd.DataFrame) -> None:
    """
    Create a qb:Slice containing observations with female gender.

    Args:
      collector (Graph): The RDF Graph to which the slice will be added.
      dataset (URIRef): The URIRef of the dataset of the Data Cube.
      data (pd.DataFrame): The pandas DataFrame containing the data.
    """
    try:
        slice_resource = NSR["slice-females"]
        slice_key = NSR["sliceByGender"]

        # Create Slice
        collector.add((slice_resource, RDF.type, QB.Slice))
        collector.add((slice_resource, QB.dataSet, dataset))
        collector.add((slice_resource, QB.sliceStructure, slice_key))
        collector.add((slice_resource, NSR["refPeriod"], URIRef(
            "http://reference.data.gov.uk/id/gregorian-interval/2004-01-01T00:00:00/P3Y")))
        collector.add((slice_resource, SDMX_DIMENSION.sex, SDMX_CODE["sex-F"]))

        # Create SliceKey
        collector.add((slice_key, RDF.type, QB.SliceKey))
        collector.add((slice_key, RDFS.label, Literal("slice by gender")))
        collector.add((slice_key, RDFS.comment, Literal(
            "Slice by Female Shooters")))
        collector.add((slice_key, QB.componentProperty, NSR["refPeriod"]))
        collector.add((slice_key, QB.componentProperty, NSR["gender"]))

        for index, row in data.iterrows():
            if row['gender'] == "Female":
                collector.add((slice_resource, QB.observation,
                               NSR[f"observation-{index:03d}"]))

        logging.info("Slice created successfully.")
    except Exception as e:
        logging.error(f"Error creating slice: {e}")



def main(output_file_path: str):
    try:
        engine_url = ''
        sql_query = '''
            SELECT * FROM shooting
        '''
        output_file = 'Shootings.csv'
        fetch_data_to_csv(engine_url, sql_query, output_file)
        data = load_csv_file_as_dataframe(output_file)
        if data is not None:
            data_cube = as_data_cube(data)
            if data_cube is not None:
                # Modify this with your slice URIRef
                slice_ref = NSR["slice-females"]
                # Modify this with your structure URIRef
                structure_ref = NSR["structure"]
                dataset = create_dataset(data_cube, structure_ref, slice_ref)
                create_slice(data_cube, dataset, data)
                create_observations(data_cube, dataset, data)

                data_cube.serialize(
                    format="turtle", destination=output_file_path)
                logging.info("Data Cube serialization successful.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("Usage: python script.py <output_file_name>")
        sys.exit(1)

    main(sys.argv[1])
