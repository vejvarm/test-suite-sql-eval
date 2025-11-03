import json
import aiohttp
import asyncio

class RDF4jConnector:
    repo_config_default = """
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
    @prefix rep: <http://www.openrdf.org/config/repository#>.
    @prefix sr: <http://www.openrdf.org/config/repository/sail#>.
    @prefix sail: <http://www.openrdf.org/config/sail#>.

    [] a rep:Repository ;
       rep:repositoryID "{}" ;
       rdfs:label "Spider ontop repository" ;
       rep:repositoryImpl [
          rep:repositoryType "openrdf:SailRepository" ;
          sr:sailImpl [
             sail:sailType "openrdf:NativeStore" ;
             sail:nativeStore:tripleIndexes "spoc,posc" ;
          ]
       ].
    """

    def __init__(self, base_url="http://localhost:8181/rdf4j-server", username="admin", password="admin", repo_config:str = None):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.auth = aiohttp.BasicAuth(username, password)
        self.namespaces = {}
        if repo_config is None:
            self.repo_config = self.repo_config_default

    async def create_repository(self, repository_id):
        """
        Creates a new repository in RDF4J.
        """
        endpoint_url = f"{self.base_url}/repositories/{repository_id}"
        headers = {"Content-Type": "text/turtle"}

        repository_config = self.repo_config.format(repository_id)

        async with aiohttp.ClientSession(auth=self.auth) as session:
            async with session.put(endpoint_url, headers=headers, data=repository_config) as response:
                if response.status == 204:
                    print(f"Repository '{repository_id}' created successfully.")
                else:
                    raise Exception(f"Failed to create repository: {response.status} {await response.text()}")

    async def add_data_to_named_graph(self, repository_id, graph_uri, ttl_file_path):
        """
        Adds data from a Turtle file to a specified named graph in an RDF4J repository.
        """
        endpoint_url = f"{self.base_url}/repositories/{repository_id}/rdf-graphs/service"
        params = {'graph': graph_uri}
        headers = {"Content-Type": "text/turtle"}

        async with aiohttp.ClientSession(auth=self.auth) as session:
            with open(ttl_file_path, 'rb') as ttl_file:
                async with session.post(endpoint_url, headers=headers, params=params, data=ttl_file) as response:
                    if response.status == 204:
                        print(f"Data successfully added to the named graph '{graph_uri}' in repository '{repository_id}'.")
                    else:
                        error_text = await response.text()
                        raise Exception(f"Failed to add data: {response.status} {error_text}")

    async def fetch_namespaces(self, repository_id):
        """
        Fetches namespaces from the specified RDF4J repository.
        """
        namespaces_url = f"{self.base_url}/repositories/{repository_id}/namespaces"
        async with aiohttp.ClientSession(auth=self.auth) as session:
            async with session.get(namespaces_url) as response:
                if response.status == 200:
                    text = await response.text()
                    lines = text.strip().split('\n')
                    namespaces = {}
                    for line in lines[1:]:  # Skip header line
                        prefix, namespace = line.split(',')
                        namespaces[prefix] = namespace.strip("\r")
                    self.namespaces[repository_id] = namespaces
                    print(f"Fetched namespaces for repository '{repository_id}': {namespaces}")
                else:
                    raise Exception(f'Failed to fetch namespaces: {response.status}')

    async def query_rdf4j(self, repository_id, sparql_query):
        """
        Queries a specific RDF4J repository and returns the results in JSON.
        Automatically prepends the necessary prefixes to the SPARQL query.
        """
        if repository_id not in self.namespaces:
            await self.fetch_namespaces(repository_id)

        prefixes = "\n".join([f"PREFIX {prefix}: <{uri}>" for prefix, uri in self.namespaces[repository_id].items()])
        query_with_prefixes = f"{prefixes}\n{sparql_query}"
        # with open("~/git/uT5-ssc/debug.log", "a") as f:
        #     f.write(f'{json.dumps({"p": prefixes, "q": sparql_query,"q_w_prefixes": query_with_prefixes}, indent=2)}\n')

        endpoint_url = f"{self.base_url}/repositories/{repository_id}"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/sparql-results+json"
        }
        data = {"query": query_with_prefixes}

        async with aiohttp.ClientSession(auth=self.auth) as session:
            async with session.post(endpoint_url, headers=headers, data=data) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception(f"Query failed: {response.status} {await response.text()}")

# Example Usage
async def main():
    connector = RDF4jConnector()
    repository_id = "singer"
    graph_uri = f"http://example.org/graph/{repository_id}"
    ttl_file_path = f"~/git/uT5-ssc/.cache/downloads/extracted/c702c18c8d855b7bc0a53f5b230cd5314a83d607fea4df3ad5612a557fae3dd2/Spider4SSC/database/{repository_id}/{repository_id}.ttl"  # Replace with your Turtle file path

    try:
        await connector.create_repository(repository_id)
        await connector.add_data_to_named_graph(repository_id, graph_uri, ttl_file_path)

        sparql_query = """
        SELECT ?s ?p ?o
        WHERE { ?s ?p ?o. }
        LIMIT 10
        """
        results = await connector.query_rdf4j(repository_id, sparql_query)
        print("Query Results:", results)

    except Exception as e:
        print(f"Error: {e}")

# Run the async function
# asyncio.run(main())
