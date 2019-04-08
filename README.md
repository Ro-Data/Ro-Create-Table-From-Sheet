# Import Semi-Structured Data from Google Sheets to Snowflake

## Requirements

- [`snowflake-connector-python`](https://pypi.org/project/snowflake-connector-python/)
- [`python-dateutil`](https://pypi.org/project/python-dateutil/)
- [`pygsheets`](https://pypi.org/project/pygsheets/)

## Background

Suppose you have a Google Sheet, accessible via a [service account][], that
looks something like this:

| id | code | date       | cost   |
| -- | ---- | ---------- | ------ |
| 1  | abc  | 03/01/2019 | 100.00 |
| 2  | xyz  | 04/01/2019 | 200.00 |

And you want to import this data into your data warehouse on a regular basis.
Further suppose that this sheet's structure is likely to change frequently, with
fields being added and removed, so you don't want to use a rigid schema.

On Snowflake, one possibility is to take advantage of [`variant`][variant], a
[semi-structured data type][semi-structured] data type.

[service account]: https://cloud.google.com/iam/docs/service-accounts
[variant]: https://docs.snowflake.net/manuals/sql-reference/data-types-semistructured.html
[semi-structured]: https://docs.snowflake.net/manuals/user-guide/semistructured-concepts.html

Using this module, the result of importing the sheet above would look like this:

| source           | imported_at | data                                                           |
| -----------------| ----------- | -------------------------------------------------------------- |
| [worksheet name] | [timestamp] | {"id": 1, "code": "abc", "date": "2019-03-01", "cost": 100.00} |
| [worksheet name] | [timestamp] | {"id": 2, "code": "xyz", "date": "2019-04-01", "cost": 200.00} |

## Usage

To get set up:

- Copy `db.json.example` to `db.json`
- Edit `db.json` to contain your Snowflake connection information and
  credentials
- Download your Google service account file
- Find the ID of a sheet you'd like to import (and to which your service account
  has access)

You can then invoke this script:

    python create_table_from_sheet.py
        --schema [destination_schema] --table [destination_table]
        --sheet [sheet_id]
        --service-account-file [path_to_service_file]
        --db-config [path_to_db_config_file]

If omitted, `./service-account.json` and `./db.json` are used as the default
values for the service account file and DB config file respectively.

However, this will import `id`, `date`, and `cost` as strings containing the
contents reflected in the sheet. You can use `--coercions` to specify that they
should be interpreted specially:

    python create_table_from_sheet.py
        # ... same as above ...
        --coercions '{"id": "int", "date": "date", "cost": "float"}'

This says that the column `id` should be interpreted as an integer, `date` as a
date, and `cost` as a float.

By default, the first worksheet is imported, but you can specify a worksheet by
name with the `--worksheet` argument.

There are also options `--verbose` (which will print the SQL generated) and
`--dry-run` (which will read the sheet and generate the SQL, but not execute it).

## Limitations

This script replaces the full table in the database every time it is run, so if
historical information is removed from the sheet, it will be removed from the
database too.
