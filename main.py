import json
from sqlalchemy.engine import url as sa_url
from sqlalchemy import create_engine
import segment_materializer as sm

with open('config.json', 'r') as jsonFile:
    jsonString = jsonFile.read()

config = json.loads(jsonString)
server = config['server']
schemas = config['schemas']

address = sa_url.URL(
    drivername='postgresql',
    username=server['username'],
    password=server['password'],
    host=server['address'],
    port=server['port'],
    database=server['database']
)
engine = create_engine(address)

conn = engine.connect()

for schema in schemas:
    sm.materialize(conn, engine, schema['name'], schema['users'], schema['properties'])
