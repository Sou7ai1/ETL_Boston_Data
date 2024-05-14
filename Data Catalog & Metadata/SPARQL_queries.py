from rdflib import Graph
g = Graph()
g.parse("Path for the file", format="turtle")

"""Query for Media Types: This query retrieves the media types of all distributions in the catalog. 
It selects distribution resources linked to a dataset through the dcat:distribution property and retrieves their media types 
using the dcat:mediaType property."
"""


query_media_types = """
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dcterms: <http://purl.org/dc/terms/>

    SELECT ?distribution ?mediaType
    WHERE {
        ?dataset a dcat:Dataset ;
                 dcat:distribution ?distribution .
        ?distribution dcat:mediaType ?mediaType .
    }
"""

"""
This query retrieves the download URLs of all distributions in the catalog. 
It also selects ?distribution resources linked to a dataset through the dcat:distribution property and retrieves 
their download URLs using the dcat:downloadURL property.
"""

query_download_urls = """
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dcterms: <http://purl.org/dc/terms/>

    SELECT ?distribution ?downloadURL
    WHERE {
        ?dataset a dcat:Dataset ;
                 dcat:distribution ?distribution .
        ?distribution dcat:downloadURL ?downloadURL .
    }
"""

results_media_types = g.query(query_media_types)
results_download_urls = g.query(query_download_urls)


print("Media Types of Distributions:")
for row in results_media_types:
    print(f"Distribution: {row.distribution}, MediaType: {row.mediaType}")

print("\nDownload URLs of Distributions:")
for row in results_download_urls:
    print(f"Distribution: {row.distribution}, DownloadURL: {row.downloadURL}")
