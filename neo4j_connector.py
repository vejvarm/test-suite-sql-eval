import subprocess
import asyncio
import pathlib
import time
from neo4j import AsyncGraphDatabase

class Neo4jConnector:
    def __init__(self, uri="bolt://localhost:7687", username="neo4j", password="secretserver", 
                 neo4j_root: pathlib.Path = pathlib.Path("/neo4j/"), database_subfolder: str = "import/database"):
        self.uri = uri
        self.username = username
        self.password = password
        self.driver = AsyncGraphDatabase.driver(uri, auth=(username, password))
        self.database = None
        self.neo4j_root = pathlib.Path(neo4j_root)
        self.database_subfolder = database_subfolder

    def extract_prefixes_from_ttl(self, db_name: str):
        """
        Extract prefixes from a TTL file.
        """
        prefixes = {}
        ttl_file = self.neo4j_root.joinpath(self.database_subfolder).joinpath(db_name).joinpath(f"{db_name}.ttl")
        print(ttl_file)
        with ttl_file.open() as f:
            for line in f.readlines():
                if line.startswith("@prefix"):
                    line = line.removeprefix("@prefix").strip(" .\n")
                    prefix, uri = line.split(" ")
                    prefix = prefix.strip(": ") or "ROOT"
                    uri = uri.removesuffix(" .").strip(" <>")
                    prefixes[prefix] = uri
        return prefixes

    async def create_database(self, database_name):
        """
        Create a new database in the Neo4j instance.
        """
        try:
            await self.use_database(database_name)
            async with self.driver.session(database="system") as session:
                await session.run(f"CREATE DATABASE {self.database};")
            print(f"Database '{self.database}' created successfully.")
        except Exception as err:
            print(f"Database not created `{repr(err)}`")

    async def clear_database(self, database_name):
        """
        Clear all objects from Neo4j database.
        """
        cmd = "\n".join(["MATCH (n) DETACH DELETE n;",
                                "call n10s.graphconfig.init;",
                                "call n10s.nsprefixes.removeAll();"])

        try:
            await self.use_database(database_name)
            async with self.driver.session(database=self.database) as session:
                await session.run(cmd)
            print(f"Database '{self.database}' wiped successfully.")
        except Exception as err:
            print(f"Database not wiped `{repr(err)}`")

    

    async def drop_database(self, database_name):
        """
        Drop an existing database in the Neo4j instance.
        !!! Not implemented in DozerDB 5.20 !!!
        """
        database_name = database_name.replace("_", "")
        async with self.driver.session(database="system") as session:
            await session.run(f"DROP DATABASE {database_name};")
        print(f"Database '{database_name}' dropped successfully.")

    async def wipe_databases(self):
            try:
                # Stop the Neo4j server
                print("Stopping Neo4j docker server")
                subprocess.run(["docker", "stop", "neo4j_server"], check=True)

                # Remove the database folder contents as superuser
                print("Removing database folders from `/neo4j/data/`")
                subprocess.run("sudo rm -rf /neo4j/data/*", shell=True, check=True)

                print("Starting Neo4j docker server")
                subprocess.run(["docker", "start", "neo4j_server"], check=True)

                print("Waiting 60 seconds to initialize the server.")
                time.sleep(60)

                print("Databases wiped and server restarted successfully.")
            except subprocess.CalledProcessError as e:
                print(f"An error occurred: {e}")

    async def use_database(self, database_name):
        """
        Switch the context to a specific database for subsequent operations.
        """
        self.database = database_name.replace("_", "")

    async def init_database(self, prefixes, db_name: str):
        """
        Initializes the Neo4j database by clearing it, adding namespaces, and loading data from a TTL file.
        """
        commands = [
            "CREATE CONSTRAINT n10s_unique_uri FOR (r:Resource) REQUIRE r.uri IS UNIQUE;",
            "MATCH (n) DETACH DELETE n;",
            "CALL n10s.graphconfig.init;",
            "CALL n10s.nsprefixes.removeAll();"
        ]

        # Add namespace prefixes
        for prefix, uri in prefixes.items():
            commands.append(f'CALL n10s.nsprefixes.add("{prefix}", "{uri}");')

        # Import TTL file
        path_to_ttl = f"file:///{self.database_subfolder}/{db_name}/{db_name}.ttl"
        print(path_to_ttl)
        commands.append(f'CALL n10s.rdf.import.fetch("{path_to_ttl}", "Turtle");')

        async with self.driver.session(database=self.database) as session:
            for command in commands:
                try:
                    await session.run(command)
                except Exception as err:
                    print(f"Command `{command}` not executed ({repr(err)})")

    # async def query_neo4j(self, db_name: str, query: str):
    #     """
    #     Executes a Cypher query and returns the results.
    #     """
    #     if db_name is not None:
    #         await self.use_database(db_name)
    #     async with self.driver.session(database=self.database) as session:
    #         result = await session.run(query)
    #         result = await result.values()
    #         return [record for record in result]

    async def query_neo4j(self, db_name: str, query: str):
        async with AsyncGraphDatabase.driver(self.uri, auth=(self.username, self.password)) as driver:
            if db_name is not None:
                await self.use_database(db_name)
            async with driver.session(database=self.database) as session:
                result = await session.run(query)
                result = await result.values()
                return [record for record in result]

    async def close(self):
        """
        Close the Neo4j driver connection.
        """
        await self.driver.close()

# Example Usage
async def main():
    connector = Neo4jConnector(username="neo4j", password="secretserver")
    db_name = "concert_singer"

    try:
        # Create a new database
        await connector.create_database(db_name)
        
        # Add data to the database
        prefixes = connector.extract_prefixes_from_ttl(db_name)
        await connector.init_database(prefixes, db_name)

        # Query the database
        query = "MATCH (t1:ROOT__singer) RETURN t1.singer__name AS t1_name, t1.singer__country AS t1_country, t1.singer__age AS t1_age ORDER BY t1.singer__age DESC"
        results = await connector.query_neo4j(db_name, query)
        print("Query Results:", results)

    finally:
        # Ensure the driver is properly closed
        await connector.close()

# Uncomment the following line to run the example
# asyncio.run(main())
