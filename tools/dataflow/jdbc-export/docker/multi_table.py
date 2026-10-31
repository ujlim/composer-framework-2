# Author: 임의진 (LIM UI JIN)
# Updated: 2026-10-31
"""Pure validation for the opt-in multi-table BigQuery manifest (version 1)."""
import json
import re

ID = re.compile(r"[a-z][a-z0-9_]{0,62}\Z")
TABLE = re.compile(r"[a-z][a-z0-9-]{4,61}[a-z0-9]:[A-Za-z_][A-Za-z0-9_]{0,1023}\.[A-Za-z_][A-Za-z0-9_]{0,1023}\Z")
FIELD = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,299}\Z")
TYPES = {"STRING", "BYTES", "INTEGER", "INT64", "FLOAT", "FLOAT64", "BOOLEAN", "BOOL", "TIMESTAMP", "DATE", "TIME", "DATETIME", "NUMERIC", "BIGNUMERIC", "GEOGRAPHY", "JSON", "RECORD", "STRUCT"}


def _keys(value, required, optional=()):
    if not isinstance(value, dict) or not set(required) <= value.keys() or value.keys() - set(required) - set(optional):
        raise ValueError("Invalid multi-table document keys")


def _json(text):
    def unique_keys(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError("Non-standard JSON constant")

    try:
        return json.loads(text, object_pairs_hook=unique_keys, parse_constant=invalid_constant)
    except (ValueError, TypeError):
        raise ValueError("Invalid multi-table JSON document") from None


def gcs_uri(uri):
    if not isinstance(uri, str) or not uri.startswith("gs://") or any(c in uri for c in "\\%?#") or any(c.isspace() or not c.isprintable() for c in uri):
        raise ValueError("Invalid multi-table GCS URI")
    parts = uri[5:].split("/")
    if len(parts) < 2 or any(p in {"", ".", ".."} for p in parts):
        raise ValueError("Invalid multi-table GCS URI")
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]", parts[0]):
        raise ValueError("Invalid multi-table GCS bucket")
    return uri


def validate_schema(schema):
    """Return a canonical fields object; reject duplicate names case-insensitively."""
    if isinstance(schema, list):
        schema = {"fields": schema}
    _keys(schema, {"fields"})

    def fields(items, depth=0):
        if not isinstance(items, list) or not items or depth > 15:
            raise ValueError("Invalid BigQuery schema fields")
        names = set()
        for field in items:
            _keys(field, {"name", "type"}, {"mode", "description", "fields", "precision", "scale", "maxLength"})
            name, kind = field["name"], field["type"]
            if not isinstance(name, str) or not FIELD.fullmatch(name) or name.lower() in names:
                raise ValueError("Invalid or duplicate BigQuery schema field")
            names.add(name.lower())
            if "description" in field and not isinstance(field["description"], str):
                raise ValueError("Invalid BigQuery schema description")
            for attribute in ("precision", "scale", "maxLength"):
                if attribute in field and (type(field[attribute]) is not int or field[attribute] < (0 if attribute == "scale" else 1)):
                    raise ValueError("Invalid BigQuery schema numeric attribute")
            if not isinstance(kind, str) or kind not in TYPES or not isinstance(field.get("mode", "NULLABLE"), str) or field.get("mode", "NULLABLE") not in {"NULLABLE", "REQUIRED", "REPEATED"}:
                raise ValueError("Invalid BigQuery schema type or mode")
            if kind in {"RECORD", "STRUCT"}:
                fields(field.get("fields"), depth + 1)
            elif "fields" in field:
                raise ValueError("Only RECORD fields may have child fields")
    fields(schema["fields"])
    return schema


def load_tables(manifest_uri, read_text):
    """Preflight all assets before constructing a pipeline or fetching credentials.

    Assets must be sibling/descendant objects of this run's manifest directory.
    Only SQL/schema/destination data belong here, never connection credentials.
    """
    prefix = gcs_uri(manifest_uri).rsplit("/", 1)[0] + "/"
    manifest = _json(read_text(manifest_uri))
    _keys(manifest, {"version", "tables"})
    if type(manifest["version"]) is not int or manifest["version"] != 1:
        raise ValueError("Unsupported multi-table manifest version")
    items = manifest["tables"]
    if not isinstance(items, list) or not items:
        raise ValueError("Multi-table manifest requires at least one table")
    ids, destinations, result = set(), set(), []
    for item in items:
        _keys(item, {"id", "query_uri", "schema_uri", "destination"})
        name = item["id"]
        if not isinstance(name, str) or not ID.fullmatch(name) or name in ids:
            raise ValueError("Invalid or duplicate multi-table id")
        ids.add(name)
        for key in ("query_uri", "schema_uri"):
            if not gcs_uri(item[key]).startswith(prefix) or item[key] == manifest_uri:
                raise ValueError("Multi-table asset must be below manifest directory")
        dest = item["destination"]
        _keys(dest, {"table"}, {"write_disposition", "create_disposition"})
        table = dest["table"]
        if not isinstance(table, str) or not TABLE.fullmatch(table) or table.lower() in destinations:
            raise ValueError("Invalid or duplicate multi-table destination")
        destinations.add(table.lower())
        if not isinstance(dest.get("write_disposition", "WRITE_APPEND"), str) or dest.get("write_disposition", "WRITE_APPEND") not in {"WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"}:
            raise ValueError("Invalid BigQuery write disposition")
        if not isinstance(dest.get("create_disposition", "CREATE_IF_NEEDED"), str) or dest.get("create_disposition", "CREATE_IF_NEEDED") not in {"CREATE_IF_NEEDED", "CREATE_NEVER"}:
            raise ValueError("Invalid BigQuery create disposition")
        schema = validate_schema(_json(read_text(item["schema_uri"])))
        query = read_text(item["query_uri"])
        if not isinstance(query, str) or not query.strip() or "{{" in query or "{%" in query:
            raise ValueError("Multi-table SQL must be nonempty and fully rendered")
        result.append({"id": name, "query": query, "schema": schema, "destination": dest})
    return result
