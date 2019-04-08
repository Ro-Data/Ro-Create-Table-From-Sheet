#!/usr/bin/env python

from __future__ import print_function, unicode_literals

import os
import io
import re
import sys
import json

import dateutil.parser
import snowflake.connector
import pygsheets

DEFAULT_DB_CONFIG_FILENAME = os.path.abspath('db.json')
DEFAULT_SERVICE_ACCOUNT_FILE = os.path.abspath('service-account.json')

DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'
]


def read_db_config(filename=None):
    """Read and return a JSON object from `filename`."""
    filename = filename or DEFAULT_DB_CONFIG_FILENAME
    with open(filename, 'r') as infile:
        return json.load(infile)


def chop_at_blank(row):
    """Chop `row` off at its first empty element."""
    result = []
    for item in row:
        if item == '':
            break
        result.append(item)
    return result


def drop_empty_rows(rows):
    """Return `rows` with all empty rows removed."""
    return [row for row in rows if any(val.strip() for val in row)]


def _read_worksheet(sheet_id, worksheet_id=None, service_account_file=None,
                    scopes=None):
    service_account_file = service_account_file or DEFAULT_SERVICE_ACCOUNT_FILE
    scopes = scopes or DEFAULT_SCOPES
    api = pygsheets.authorize(service_account_file=service_account_file,
                              scopes=scopes)
    sheet = api.open_by_key(sheet_id)
    worksheet_id = worksheet_id or 0
    if isinstance(worksheet_id, int):
        worksheet = sheet[worksheet_id]
    elif isinstance(worksheet_id, str):
        worksheet = sheet.worksheet_by_title(worksheet_id)
    else:
        raise Exception('Invalid ID for worksheet: {!r}'.format(worksheet_id))
    title = worksheet.title
    rows = list(worksheet)
    headers = chop_at_blank(rows[0])
    data = drop_empty_rows(rows[1:])
    return {'title': title, 'headers': headers, 'data': data}


def headers_to_keys(headers):
    """Convert row headers to object keys."""
    regex = re.compile(r'[^a-z0-9_]+')
    return [regex.sub('_', header.lower()) for header in headers]


def apply_coercions_1(obj, coercions):
    """Return `obj` with `coercions` applied."""
    result = {}
    for key, val in obj.items():
        target = coercions.get(key)
        if target in ('int', 'integer'):
            val = re.sub(r'[,$]', '', val)
            val = int(val) if val else None
        elif target == 'float':
            val = re.sub(r'[,$]', '', val)
            val = float(val) if val else None
        elif target == 'date':
            val = dateutil.parser.parse(val) if val.strip() else None
            val = val.strftime('%Y-%m-%d')
        elif target in ('datetime', 'timestamp'):
            val = dateutil.parser.parse(val) if val.strip() else None
            val = val.strftime('%Y-%m-%d %H:%M:%S')
        elif target is not None:
            print('Unknown coercion target {!r}'.format(target),
                  file=sys.stderr)
        result[key] = val
    return result


def apply_coercions(data, coercions):
    """Return `data` with `coercions` applied to each object."""
    return [apply_coercions_1(obj, coercions) for obj in data]


def read_worksheet(sheet_id, worksheet_id=None, coercions=None,
                   service_account_file=None, scopes=None):
    """Read a worksheet and return a dict.

    The dict will have two keys: `title` (the title of the worksheet) and
    `data` (a list of dicts, one for each row, mapping column names to values).

    The `sheet_id` should be the ID as used by Google Sheets, not the title.
    The `worksheet_id` can be either an integer (the ordinal position of the
    worksheet) or a string (its title).
    """
    objects = []
    payload = _read_worksheet(sheet_id, worksheet_id=worksheet_id,
                              service_account_file=service_account_file,
                              scopes=scopes)
    headers = payload['headers']
    keys = headers_to_keys(headers)
    for row in payload['data']:
        objects.append(dict(zip(keys, row)))
    if coercions:
        objects = apply_coercions(objects, coercions)
    return {'title': payload['title'], 'data': objects}


def build_create_table(schema, table):
    """Return the CREATE TABLE statement as a string."""
    return """CREATE OR REPLACE TABLE {}.{} (
        source string,
        imported_at timestamp_tz,
        data variant
    );
    """.format(schema, table)


def build_insert_rows(schema, table, payload):
    """Return the INSERT INTO statement as a string."""
    out = io.StringIO()

    out.write('INSERT INTO {}.{}\n'.format(schema, table))
    out.write('SELECT column1, column2, parse_json(column3)\n')
    out.write('FROM VALUES\n')

    title = payload['title']
    data = payload['data']
    count = len(data)
    for i, obj in enumerate(data):
        out.write("('{}', current_timestamp, '{}')".format(
            title, json.dumps(obj)
        ))
        if i != count - 1:
            out.write(',')
        out.write('\n')

    return out.getvalue()


def load_sheet(schema, table, sheet_id, worksheet=None, coercions=None,
               service_account_file=None, config_file=None,
               verbose=False, dry_run=False):
    """Load ``schema.table`` from `sheet_id`."""
    if isinstance(coercions, str):
        coercions = json.loads(coercions)
    config_file = config_file or DEFAULT_DB_CONFIG_FILENAME
    config = read_db_config(config_file)
    payload = read_worksheet(sheet_id, worksheet_id=worksheet,
                             service_account_file=service_account_file,
                             coercions=coercions)
    create_table = build_create_table(schema, table)
    insert_rows = build_insert_rows(schema, table, payload)
    with snowflake.connector.connect(**config) as connection:
        cursor = connection.cursor()
        for statement in create_table, insert_rows:
            if verbose:
                print(statement)
            if not dry_run:
                cursor.execute(statement)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--schema', required=True)
    parser.add_argument('--table', required=True)
    parser.add_argument('--sheet', required=True)
    parser.add_argument('--worksheet')
    parser.add_argument('--coercions')
    parser.add_argument('--db-config')
    parser.add_argument('--service-account-file')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    load_sheet(args.schema, args.table, args.sheet,
               worksheet=args.worksheet,
               coercions=args.coercions,
               service_account_file=args.service_account_file,
               config_file=args.db_config,
               verbose=args.verbose,
               dry_run=args.dry_run)
