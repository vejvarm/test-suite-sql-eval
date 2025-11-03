import json
import os
import re
import sqlite3
import asyncio
import multiprocessing
from typing import Tuple, Any, List, Set
from itertools import product, chain
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor  # Use ProcessPoolExecutor instead of ThreadPoolExecutor
import tqdm
import random
import time
import pickle as pkl
import subprocess

from neo4j.time import DateTime, Date, Time
from rdflib import Graph

from .parse import get_all_preds_for_execution, remove_distinct

# threadLock = threading.Lock()  # Not needed or not effective across processes
TIMEOUT = 60
EXEC_TMP_DIR = os.path.join(os.path.dirname(__file__), "tmp")

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

def quick_rej(result1: List[Tuple], result2: List[Tuple], order_matters: bool) -> bool:
    s1 = [unorder_row(row) for row in result1]
    s2 = [unorder_row(row) for row in result2]
    if order_matters:
        return s1 == s2
    else:
        return set(s1) == set(s2)

def multiset_eq(l1: List, l2: List) -> bool:
    if len(l1) != len(l2):
        return False
    d = defaultdict(int)
    for e in l1:
        d[e] += 1
    for e in l2:
        d[e] -= 1
        if d[e] < 0:
            return False
    return True

def get_constraint_permutation(tab1_sets_by_columns: List[Set], result2: List[Tuple]):
    num_cols = len(result2[0])
    perm_constraints = [{i for i in range(num_cols)} for _ in range(num_cols)]
    if num_cols <= 3:
        return list(product(*perm_constraints))

    # we sample 20 rows and constrain the space of permutations
    for _ in range(20):
        random_tab2_row = random.choice(result2)

        for tab1_col in range(num_cols):
            for tab2_col in set(perm_constraints[tab1_col]):
                if random_tab2_row[tab2_col] not in tab1_sets_by_columns[tab1_col]:
                    perm_constraints[tab1_col].remove(tab2_col)
    return list(product(*perm_constraints))

def result_eq(result1: List[Tuple], result2: List[Tuple], order_matters: bool) -> bool:
    if len(result1) == 0 and len(result2) == 0:
        return True
    if len(result1) != len(result2):
        return False
    num_cols = len(result1[0])
    if len(result2[0]) != num_cols:
        return False
    if not quick_rej(result1, result2, order_matters):
        return False

    tab1_sets_by_columns = [{row[i] for row in result1} for i in range(num_cols)]
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
            if set(result1) == set(result2_perm) and multiset_eq(result1, result2_perm):
                return True
    return False

def replace_cur_year(query: str) -> str:
    return re.sub(
        "YEAR\s*\(\s*CURDATE\s*\(\s*\)\s*\)\s*", "2020", query, flags=re.IGNORECASE
    )

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
    if value == "0" and "aggregation" in var_name:
        return None
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
                transformed_entry[var] = value
        transformed_results.append(transformed_entry)

    values_only = [tuple(e.values()) for e in transformed_results]
    return values_only

def exec_sparql_sync(kg_path: str, query: str):
    graph = Graph()
    graph.parse(kg_path, format="ttl")
    result = graph.query(query)
    graph.close()
    return result

async def exec_sparql_on_db_(kg_path: str, query: str) -> Tuple[str, Any]:
    loop = asyncio.get_event_loop()
    try:
        # Use ProcessPoolExecutor for multiprocessing
        with ProcessPoolExecutor() as executor:
            result = await loop.run_in_executor(executor, exec_sparql_sync, kg_path, query)
        result_serialized = result.serialize(format='json').decode('utf-8')
        print("\tdone")
        return "result", _transform_rdflib_result(result_serialized)
    except Exception as e:
        return "exception", e

async def exec_on_db(
    db_path: str, query: str, process_id: str = "", timeout: int = TIMEOUT, lang: str = "sql"
) -> Tuple[str, Any]:
    try:
        if "sql" in lang: 
            return await asyncio.wait_for(exec_on_db_(db_path, query), timeout)
        elif "sparql" in lang:
            print("executing sparql")
            return await asyncio.wait_for(exec_sparql_on_db_(db_path, query), timeout)
    except asyncio.TimeoutError:
        return ('exception', TimeoutError)
    except Exception as e:
        return ("exception", e)

def postprocess(query: str) -> str:
    query = query.replace("> =", ">=").replace("< =", "<=").replace("! =", "!=")
    return query

def eval_exec_match(
    db: str,
    p_str: str,
    g_str: str,
    plug_value: bool,
    keep_distinct: bool,
    progress_bar_for_each_datapoint: bool,
    lang = "sql",
) -> int:
    p_str, g_str = postprocess(p_str), postprocess(g_str)
    if not keep_distinct:
        try:
            # if sqlparse can't parse p_str, we should not even try to execute it
            p_str = remove_distinct(p_str, lang)
        except Exception as e:
            return 0
        g_str = remove_distinct(g_str, lang)

    order_matters = "order by" in g_str.lower()

    ext_map = {"sql": ".sqlite",
               "sparql": ".ttl",
               "cypher": ".ttl"}

    db_dir = os.path.dirname(db)
    db_paths = [
        os.path.join(db_dir, basename)
        for basename in os.listdir(db_dir)
        if basename.endswith(ext_map[lang])
    ]

    preds = [p_str]
    if plug_value:
        _, preds_from_gold = get_all_preds_for_execution(g_str, p_str)
        preds = list(chain([p_str], preds_from_gold))

    for pred in preds:
        pred_passes = 1
        ranger = tqdm.tqdm(db_paths) if progress_bar_for_each_datapoint else db_paths
        for db_path in ranger:
            g_flag, g_denotation = asyncio.run(exec_on_db(db_path, g_str, lang=lang))
            p_flag, p_denotation = asyncio.run(exec_on_db(db_path, pred, lang=lang))

            assert g_flag != "exception", f"gold query {g_str} has error {g_denotation} on database file {db_path}"

            if p_flag == "exception":
                pred_passes = 0
            elif not result_eq(g_denotation, p_denotation, order_matters=order_matters):
                pred_passes = 0

            if pred_passes == 0:
                break

        if pred_passes == 1:
            return 1

    return 0
