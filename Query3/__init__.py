import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
    
    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0 or len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = "La note moyenne des films par genre\n"
    try:
            logging.info("Test de connexion avec pyodbc...")
            with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT genre, SUM(averageRating)/COUNT(*) FROM [dbo].[tTitles] as T, [dbo].[tGenres] as G WHERE T.tconst = G.tconst AND averageRating >= 0 GROUP BY genre")

                rows = cursor.fetchall()
                for row in rows:
                    dataString += f"SQL: genre={row[0]}, note moyenne ={row[1]}\n"


    except:
        errorMessage = "Erreur de connexion a la base SQL"
    
        
    
    if name:
        nameMessage = f"Hello, {name}!\n"
    else:
        nameMessage = "Le parametre 'name' n'a pas ete fourni lors de l'appel.\n"
    
    if errorMessage != "":
        return func.HttpResponse(dataString + nameMessage + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString + nameMessage + " Connexions réussies a Neo4j et SQL!")
