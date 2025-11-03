import json
import os
import pathlib
import re
import sqlite3
import asyncio
import threading
from typing import Tuple, Any, List, Set
from itertools import product
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import tqdm
import random
import time
import pickle as pkl
import subprocess
from itertools import chain

from neo4j.time import DateTime, Date, Time
from rdflib import Graph

from .parse import get_all_preds_for_execution, remove_distinct
from .rdf4j_connector import RDF4jConnector
from .neo4j_connector import Neo4jConnector
from .postgres_connector import PostgresConnector

NEO4J_DB_ROOT = os.getenv("NEO4J_DB_ROOT", "/neo4j")
NEO4J_OVERRIDE_KG = os.getenv("NEO4J_OVERRIDE_KG", None)

threadLock = threading.Lock()
TIMEOUT = 60
EXEC_TMP_DIR = os.path.join(os.path.dirname(__file__), "tmp")
RDF4J = RDF4jConnector()
NEO4J = Neo4jConnector(neo4j_root=NEO4J_DB_ROOT)
POSTGRES = PostgresConnector()


DATATYPES = {
    float: ["http://www.w3.org/2001/XMLSchema#decimal", "http://www.w3.org/2001/XMLSchema#float", "http://www.w3.org/2001/XMLSchema#double"],
    int: ["http://www.w3.org/2001/XMLSchema#integer", "http://www.w3.org/2001/XMLSchema#long", "http://www.w3.org/2001/XMLSchema#int"],
    bool: ["http://www.w3.org/2001/XMLSchema#boolean"],
    str: ["http://www.w3.org/2001/XMLSchema#string"],
    "dateTime": ["http://www.w3.org/2001/XMLSchema#dateTime"],
    "date": ["http://www.w3.org/2001/XMLSchema#date"],
    "time": ["http://www.w3.org/2001/XMLSchema#time"],
}

def permute_tuple(element: Tuple, perm: Tuple) -> Tuple:
    assert len(element) == len(perm)
    return tuple([element[i] for i in perm])


def unorder_row(row: Tuple) -> Tuple:
    return tuple(sorted(row, key=lambda x: str(x) + str(type(x))))


# unorder each row in the table
# [result_1 and result_2 has the same bag of unordered row]
# is a necessary condition of
# [result_1 and result_2 are equivalent in denotation]
def quick_rej(result1: List[Tuple], result2: List[Tuple], order_matters: bool) -> bool:
    s1 = [unorder_row(row) for row in result1]
    s2 = [unorder_row(row) for row in result2]
    if order_matters:
        return s1 == s2
    else:
        return set(s1) == set(s2)


# return whether two bag of relations are equivalent
def multiset_eq(l1: List, l2: List) -> bool:
    if len(l1) != len(l2):
        return False
    d = defaultdict(int)
    for e in l1:
        d[e] = d[e] + 1
    for e in l2:
        d[e] = d[e] - 1
        if d[e] < 0:
            return False
    return True


def get_constraint_permutation(tab1_sets_by_columns: List[Set], result2: List[Tuple]):
    num_cols = len(result2[0])
    perm_constraints = [{i for i in range(num_cols)} for _ in range(num_cols)]
    if num_cols <= 3:
        return product(*perm_constraints)

    # we sample 20 rows and constrain the space of permutations
    for _ in range(20):
        random_tab2_row = random.choice(result2)

        for tab1_col in range(num_cols):
            for tab2_col in set(perm_constraints[tab1_col]):
                if random_tab2_row[tab2_col] not in tab1_sets_by_columns[tab1_col]:
                    perm_constraints[tab1_col].remove(tab2_col)
    return product(*perm_constraints)


# check whether two denotations are correct
def result_eq(result1: List[Tuple], result2: List[Tuple], order_matters: bool) -> bool:
    if len(result1) == 0 and len(result2) == 0:
        return True

    # if length is not the same, then they are definitely different bag of rows
    if len(result1) != len(result2):
        return False

    num_cols = len(result1[0])

    # if the results do not have the same number of columns, they are different
    if len(result2[0]) != num_cols:
        return False

    # unorder each row and compare whether the denotation is the same
    # this can already find most pair of denotations that are different
    if not quick_rej(result1, result2, order_matters):
        return False

    # the rest of the problem is in fact more complicated than one might think
    # we want to find a permutation of column order and a permutation of row order,
    # s.t. result_1 is the same as result_2
    # we return true if we can find such column & row permutations
    # and false if we cannot
    tab1_sets_by_columns = [{row[i] for row in result1} for i in range(num_cols)]

    # on a high level, we enumerate all possible column permutations that might make result_1 == result_2
    # we decrease the size of the column permutation space by the function get_constraint_permutation
    # if one of the permutation make result_1, result_2 equivalent, then they are equivalent
    for perm in get_constraint_permutation(tab1_sets_by_columns, result2):
        if len(perm) != len(set(perm)):
            continue
        if num_cols == 1:
            result2_perm = result2
        else:
            result2_perm = [permute_tuple(element, perm) for element in result2]
        if order_matters:
            if result1 == result2_perm:
                return True
        else:
            # in fact the first condition must hold if the second condition holds
            # but the first is way more efficient implementation-wise
            # and we use it to quickly reject impossible candidates
            if set(result1) == set(result2_perm) and multiset_eq(result1, result2_perm):
                return True
    return False


def replace_cur_year(query: str) -> str:
    return re.sub(
        "YEAR\s*\(\s*CURDATE\s*\(\s*\)\s*\)\s*", "2020", query, flags=re.IGNORECASE
    )


# get the database cursor for a sqlite database path
def get_cursor_from_path(sqlite_path: str):
    try:
        if not os.path.exists(sqlite_path):
            print("Openning a new connection %s" % sqlite_path)
        connection = sqlite3.connect(sqlite_path)
    except Exception as e:
        print(sqlite_path)
        raise e
    connection.text_factory = lambda b: b.decode(errors="ignore")
    cursor = connection.cursor()
    return cursor


async def exec_on_db_(sqlite_path: str, query: str) -> Tuple[str, Any]:
    query = replace_cur_year(query)
    cursor = get_cursor_from_path(sqlite_path)
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        cursor.connection.close()
        return "result", result
    except Exception as e:
        cursor.close()
        cursor.connection.close()
        return "exception", e


def convert_rdflib_value(value, datatype, var_name):
    """ Convert a value from rdflib to a Python/Cypher datatype"""
    if value == "0" and "aggregation" in var_name:
        return None  # Special case for null entries
    if datatype in DATATYPES[float]:
        return float(value)
    if datatype in DATATYPES[int]:
        return int(value)
    if datatype in DATATYPES[bool]:
        return value.lower() == 'true'
    if datatype in DATATYPES[str]:
        return str(value)
    if datatype in DATATYPES["dateTime"]:
        return DateTime.fromisoformat(value)
    if datatype in DATATYPES["date"]:
        return Date.fromisoformat(value)
    if datatype in DATATYPES["time"]:
        return Time.fromisoformat(value)
    # Add more datatype conversions as needed
    return value


def _transform_rdflib_result(rdflib_json):
    rdflib_data = json.loads(rdflib_json)
    bindings = rdflib_data["results"]["bindings"]
    vars = rdflib_data["head"]["vars"]
    transformed_results = []

    for binding in bindings:
        transformed_entry = {}
        for var in vars:
            if var not in binding.keys():
                transformed_entry[var] = None
                continue
            data = binding[var]
            value = data["value"]
            datatype = data.get("datatype")
            if datatype:
                transformed_entry[var] = convert_rdflib_value(value, datatype, var)
            else:
                # for example uri does not have a datatype, but we can just take it as a string
                transformed_entry[var] = value
        transformed_results.append(transformed_entry)

    values_only = [tuple(e.values()) for e in transformed_results]

    return values_only


def get_graph_from_path(kg_path: str):
    path_to_dataset = kg_path
    graph = Graph(store="Oxigraph")
    graph.parse(path_to_dataset, format="ttl")

    return graph


def exec_sparql_sync(kg_path: str, query: str):
    graph = Graph()
    graph.parse(kg_path, format="ttl")
    result = graph.query(query)
    graph.close()
    return result

async def exec_sparql_on_db_(kg_path: str, query: str) -> Tuple[str, Any]:
    try:
        repo_name = pathlib.Path(kg_path).stem
        result = await RDF4J.query_rdf4j(repo_name, query)
        result_serialized = json.dumps(result)
        return "result", _transform_rdflib_result(result_serialized)
    except Exception as e:
        return "exception", e


async def exec_cypher_on_db_(kg_path: str, query: str) -> Tuple[str, Any]:
    try:
        if NEO4J_OVERRIDE_KG is None:
            repo_name = pathlib.Path(kg_path).stem
        else:
            repo_name = NEO4J_OVERRIDE_KG
        result = await NEO4J.query_neo4j(repo_name, query)
        res_out = [tuple(record) for record in result]
        # deb = pathlib.Path("~/git/uT5-ssc/debug_exec_cypher.log").resolve()
        # with deb.open("a") as f:
        #     f.write(f'{json.dumps({"repo": repo_name, "res": res_out}, indent=2)}\n')
        return "result", res_out
    except Exception as e:
        return "exception", e

# async def exec_sparql_on_db_(kg_path: str, query: str) -> Tuple[str, Any]:
#     query = replace_cur_year(query)
#     graph = get_graph_from_path(kg_path)
#     try:
#         result = graph.query(query)
#         result_serialized = result.serialize(format='json').decode('utf-8')
#         graph.close()
#         return "result", _transform_rdflib_result(result_serialized)
#     except Exception as e:
#         graph.close()
#         del graph
#         return "exception", e

async def exec_on_db(
    db_path: str, query: str, process_id: str = "", timeout: int = TIMEOUT, lang: str = "sql"
) -> Tuple[str, Any]:
    try:
        if "postgresql" in lang:
            return await asyncio.wait_for(POSTGRES.query_postgresql(query), timeout)
        elif "sql" in lang: 
            return await asyncio.wait_for(exec_on_db_(db_path, query), timeout)
        elif "sparql" in lang:
            return await asyncio.wait_for(exec_sparql_on_db_(db_path, query), timeout)
        elif "cypher" in lang:
            return await asyncio.wait_for(exec_cypher_on_db_(db_path, query), timeout)
        else:
            raise NotImplementedError("`lang` must be one of `sql`, `sparql` or `cypher`.")
    except asyncio.TimeoutError:
        return ('exception', TimeoutError)
    except Exception as e:
        return ("exception", e)


# postprocess the model predictions to avoid execution errors
# e.g. removing spaces between ">" and "="
def postprocess(query: str) -> str:
    query = query.replace("> =", ">=").replace("< =", "<=").replace("! =", "!=")
    return query


# approximate whether p_str and g_str are semantically equivalent
# db is the database path
# we are going to evaluate whether they are equivalent in all the databases
# that are in the same directory as db
# 0 if denotationally equivalent
# 1 otherwise
# the meaning of each auxillary argument can be seen in the parser definition in evaluation.py
def eval_exec_match(
    db: str,
    p_str: str,
    g_str: str,
    plug_value: bool,
    keep_distinct: bool,
    progress_bar_for_each_datapoint: bool,
    lang = "sql",  # can be postgresql, sql, sparql or cypher
) -> int:
    # post-process the prediction.
    # e.g. removing spaces between ">" and "="
    p_str, g_str = postprocess(p_str), postprocess(g_str)
    if not keep_distinct:
        try:
            # if sqlparse can't parse p_str, we should not even try to execute it
            p_str = remove_distinct(p_str, lang)
        except Exception as e:
            return 0
        g_str = remove_distinct(g_str, lang)

    # DEBUG: WORKS NOW !!!    
    deb = pathlib.Path("~/git/uT5-ssc/debug.log").resolve()
    with deb.open("a") as f:
        f.write(f'{json.dumps({"p_str": p_str, "g_str": g_str}, indent=2)}\n')

    # we decide whether two denotations are equivalent based on "bag semantics"
    # https://courses.cs.washington.edu/courses/cse444/10sp/lectures/lecture16.pdf
    # if there is order by in query, then we assume order of the rows matter
    # order by might also be used to find the max/min instead of sorting,
    # but in that case the result mostly only contains one row and hence order_matters does not make a difference
    order_matters = "order by" in g_str.lower()

    # based on the language, we choose the database types to load
    ext_map = {"postgresql": ".sqlite",
               "sql": ".sqlite",
               "sparql": ".ttl",
               "cypher": ".ttl"}

    # find all databases in the same directory
    db_dir = os.path.dirname(db)
    db_paths = [
        os.path.join(db_dir, basename)
        for basename in os.listdir(db_dir)
        if basename.endswith(ext_map[lang])
    ]

    preds = [p_str]
    # if plug in value (i.e. we do not consider value prediction correctness)
    # enumerate all ways to plug in values in the gold query to the model predictions
    # otherwise, we only evaluate the predicted query with its own value prediction
    if plug_value:
        _, preds = get_all_preds_for_execution(g_str, p_str)
        # we did not add this line in our EMNLP work
        # this reduces "false negatives" when value is substituted
        preds = chain([p_str], preds)

    for pred in preds:

        pred_passes = 1
        # compare the gold and predicted denotations on each database in the directory
        # wrap with progress bar if required
        if progress_bar_for_each_datapoint:
            ranger = tqdm.tqdm(db_paths)
        else:
            ranger = db_paths
        
        for db_path in ranger:
            async def run_queries():
                return await asyncio.gather(
                    exec_on_db(db_path, g_str, lang=lang),
                    exec_on_db(db_path, pred, lang=lang),
                    return_exceptions=True  # Capture exceptions
                )
            g_result, p_result = asyncio.run(run_queries())
            g_flag, g_denotation = g_result
            p_flag, p_denotation = p_result
            # g_flag, g_denotation = asyncio.run(exec_on_db(db_path, g_str, lang=lang))
            # p_flag, p_denotation = asyncio.run(exec_on_db(db_path, pred, lang=lang))

            # we should expect the gold to be succesfully executed on the database
            assert (
                g_flag != "exception"
            ), f"gold query {g_str} has error {g_denotation} on database file {db_path}"

            # wrong if execution fails
            if p_flag == "exception":
                pred_passes = 0

            # if denotations are not equivalent, the prediction must be wrong
            elif not result_eq(g_denotation, p_denotation, order_matters=order_matters):
                pred_passes = 0
            if pred_passes == 0:
                break

        # the model prediction has the same denotation as the gold for all databases
        if pred_passes == 1:
            return 1

    # none of the predictions passed
    return 0
