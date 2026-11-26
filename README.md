# libCNMC

Català · [Castellano](README.es.md) · [English](README.en.md)

Llibreria i eines de línia d’ordres per preparar fitxers d’inventari, plans
d’inversió i formularis d’informació reguladora de la distribució elèctrica.
Els models de registre es poden utilitzar sense connexió a l’ERP.

## Instal·lació

Des de l’arrel del repositori, instal·la el paquet en un entorn virtual amb
Python 3.11. L’exemple crea l’entorn al costat del repositori.

```bash
python3.11 -m venv ../libcnmc-py311
. ../libcnmc-py311/bin/activate
python -m pip install .
```

La secció de proves mostra com instal·lar el paquet amb Python 2.7 dins de
Docker. Si vols comprovar quina còpia del paquet carrega un entorn existent,
executa aquesta ordre amb el seu intèrpret des de fora del repositori.

```bash
python -c 'import libcnmc; print(libcnmc.__file__)'
```

## Ús dels models

Els models dels formularis F1 a F8 de les resolucions 4771, 4131 i 4666
s’importen des de `libcnmc.models`. Els de la resolució 4667 són a
`libcnmc.models.res_4667`.

Aquest exemple crea dues versions d’un registre de despatx (F6) amb dades
fictícies. El camp `valor_inversion` es converteix a enter.

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

`dump()` retorna text JSON i `dump("list")` retorna els valors en l’ordre de
l’esquema. En JSON, els valors `Decimal` es representen com a cadenes.
`diff()` compara tots els camps per defecte. L’operador `==` aplica el criteri
de comparació de cada formulari.

## Línia d’ordres

Per generar fitxers amb `cnmc`, utilitza un entorn Python 2.7 amb accés a
GISCE ERP/OpenERP. Des de l’arrel del repositori, instal·la el paquet, el
client OOOP i les dependències dels formularis.

```bash
python -m pip install --upgrade 'pip<21' 'setuptools<45' wheel
python -m pip install . 'psycopg2-binary<2.9' 'pandas<0.25' \
  https://github.com/gisce/ooop/archive/xmlrpc_transaction.zip
python -m pip install --no-deps workalendar pyCalverter lunardate pyluach
cnmc --help
cnmc cir_8_2021_fb3_3 --help
```

L’ajuda de cada ordre indica els paràmetres que admet. Per al formulari B3.3,
indica l’any amb `--year`, el codi de la distribuïdora amb `--codi-r1` i la
ruta del fitxer amb `--output`. Les opcions `--server`, `--port`,
`--database`, `--user` i `--password` configuren la connexió a l’ERP.

## Proves

GitHub Actions executa la suite Mamba amb cobertura en Python 2.7 i 3.11
quan un PR cap a `master` té l’etiqueta `to be merged`. La suite es troba a
`spec/` i es pot executar sense connexió a l’ERP. [Workflow de proves](.github/workflows/tests.yml)

### Python 3.11

Amb l’entorn virtual activat, executa aquestes ordres des de l’arrel del
repositori.

```bash
python -m pip install . 'mamba>=0.11.3' psycopg2-binary pandas workalendar expects
mamba --enable-coverage
```

Per provar només els models, executa `mamba spec/models`.

### Python 2.7

Des de l’arrel del repositori, executa les proves en un contenidor. El
contenidor munta el repositori en mode de només lectura i treballa sobre
una còpia temporal.

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

## Referències normatives

| Àmbit | Mòdul | Referència |
| --- | --- | --- |
| Informació reguladora | `libcnmc/cir_8_2021/` | [Circular 8/2021](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2021-21003) |
| Informació reguladora | `libcnmc/cir_4_2015/` | [Circular 4/2015](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2015-8624) |
| Informació reguladora | `libcnmc/cir_4_2014/` | [Circular 4/2014](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2014-8511) |
| Bo social | `libcnmc/cir_3_2015/` | [Circular 3/2015](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2015-8985) |
| Inventari auditat | `libcnmc/res_4603/` | [BOE-A-2014-4603](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2014-4603) |
| Inventari auditat | `libcnmc/res_4771/` | [BOE-A-2015-4771](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2015-4771) |
| Inventari auditat | `libcnmc/res_4131/` | [BOE-A-2016-4131](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2016-4131) |
| Inventari auditat | `libcnmc/res_4666/` | [BOE-A-2017-4666](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2017-4666) |
| Plans d’inversió | `libcnmc/res_4667/` | [BOE-A-2017-4667](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2017-4667) |
| Auditoria d’inversions | `libcnmc/audit_6181/` | [BOE-A-2019-6181](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2019-6181) |
