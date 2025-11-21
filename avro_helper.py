from fastavro import parse_schema, schemaless_writer, schemaless_reader
import json
import io

def load_schema(path):
    with open(path, "r") as f:
        schema = json.load(f)
    return parse_schema(schema)

def serialize_avro(schema, record):
    bytes_writer = io.BytesIO()
    schemaless_writer(bytes_writer, schema, record)
    return bytes_writer.getvalue()

def deserialize_avro(schema, data):
    bytes_reader = io.BytesIO(data)
    return schemaless_reader(bytes_reader, schema)
