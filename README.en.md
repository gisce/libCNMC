# libCNMC

[Català](README.md) · [Castellano](README.es.md) · English

Python library and command-line tools for preparing electricity distribution
inventories, investment plans, and regulatory reporting forms. The record
models can be used without an ERP connection.

## Installation

From the repository root, install the package in a Python 3.11 virtual
environment. This example creates the environment alongside the repository.

```bash
python3.11 -m venv ../libcnmc-py311
. ../libcnmc-py311/bin/activate
python -m pip install .
```

The testing section shows how to install the package with Python 2.7 in Docker.
To check which copy of the package an existing environment loads, run this
command with its interpreter from outside the repository.

```bash
python -c 'import libcnmc; print(libcnmc.__file__)'
```

## Using the models

Import the models for forms F1 through F8 of resolutions 4771, 4131, and
4666 from `libcnmc.models`. The models for resolution 4667 are in
`libcnmc.models.res_4667`.

This example creates two versions of a dispatch centre record (F6) with
sample data. The `valor_inversion` field is converted to an integer.

```python
from libcnmc.models import F6Res4131

data = {
    "identificador": "D1",
    "cini": "I2900000",
    "denominacion": "Despatx de mostra",
    "fecha_aps": "15/06/2015",
    "fecha_baja": "",
    "valor_inversion": "1000",
    "estado": 1,
}
record = F6Res4131(**data)
updated = F6Res4131(**dict(data, valor_inversion="1250"))

print(record.dump())
print(record.dump("list"))
print(record.diff(updated))  # {'valor_inversion': (1000, 1250)}
```

`dump()` returns JSON text, and `dump("list")` returns values in schema order.
In JSON, `Decimal` values are represented as strings. `diff()` compares all
fields by default. The `==` operator uses each form’s comparison rules.

## Command line

To generate files with `cnmc`, use a Python 2.7 environment with access to
GISCE ERP/OpenERP. From the repository root, install the package, the OOOP
client, and the dependencies used by the forms.

```bash
python -m pip install --upgrade 'pip<21' 'setuptools<45' wheel
python -m pip install . 'psycopg2-binary<2.9' 'pandas<0.25' \
  https://github.com/gisce/ooop/archive/xmlrpc_transaction.zip
python -m pip install --no-deps workalendar pyCalverter lunardate pyluach
cnmc --help
cnmc cir_8_2021_fb3_3 --help
```

Each command’s help lists the options it accepts. For form B3.3, use `--year`
for the reporting year, `--codi-r1` for the distributor code, and `--output`
for the file path. The `--server`, `--port`, `--database`, `--user`, and
`--password` options configure the ERP connection.

## Tests

GitHub Actions runs the Mamba suite with coverage on Python 2.7 and 3.11 when
a PR targeting `master` has the `to be merged` label. The suite is in
`spec/` and can run without an ERP connection. [Test workflow](.github/workflows/tests.yml)

### Python 3.11

With the virtual environment activated, run these commands from the repository
root.

```bash
python -m pip install . 'mamba>=0.11.3' psycopg2-binary pandas workalendar expects
mamba --enable-coverage
```

To run just the model tests, use `mamba spec/models`.

### Python 2.7

Run the tests in a container from the repository root. The container mounts
the repository read-only and tests a temporary copy.

```bash
docker run --rm -i -v "$PWD:/work:ro" -w /tmp python:2.7 bash -es <<'BASH'
cp -R /work libcnmc
cd libcnmc
python -m pip install --upgrade 'pip<21' 'setuptools<45' wheel
python -m pip install . 'psycopg2-binary<2.9' 'pandas<0.25' 'mamba<0.11' mock expects
python -m pip install --no-deps workalendar pyCalverter lunardate pyluach
mamba --enable-coverage
BASH
```

## Regulatory references

| Scope | Module | Reference |
| --- | --- | --- |
| Regulatory reporting | `libcnmc/cir_8_2021/` | [Circular 8/2021](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2021-21003) |
| Regulatory reporting | `libcnmc/cir_4_2015/` | [Circular 4/2015](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2015-8624) |
| Regulatory reporting | `libcnmc/cir_4_2014/` | [Circular 4/2014](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2014-8511) |
| Electricity social tariff | `libcnmc/cir_3_2015/` | [Circular 3/2015](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2015-8985) |
| Audited inventory | `libcnmc/res_4603/` | [BOE-A-2014-4603](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2014-4603) |
| Audited inventory | `libcnmc/res_4771/` | [BOE-A-2015-4771](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2015-4771) |
| Audited inventory | `libcnmc/res_4131/` | [BOE-A-2016-4131](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2016-4131) |
| Audited inventory | `libcnmc/res_4666/` | [BOE-A-2017-4666](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2017-4666) |
| Investment plans | `libcnmc/res_4667/` | [BOE-A-2017-4667](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2017-4667) |
| Investment audits | `libcnmc/audit_6181/` | [BOE-A-2019-6181](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2019-6181) |
