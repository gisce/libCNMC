# libCNMC

[Català](README.md) · Castellano · [English](README.en.md)

Biblioteca y herramientas de línea de comandos para preparar ficheros de
inventario, planes de inversión y formularios de información regulatoria de
la distribución eléctrica. Los modelos de registro se pueden utilizar sin
conexión al ERP.

## Instalación

Desde la raíz del repositorio, instala el paquete en un entorno virtual con
Python 3.11. El ejemplo crea el entorno junto al repositorio.

```bash
python3.11 -m venv ../libcnmc-py311
. ../libcnmc-py311/bin/activate
python -m pip install .
```

La sección de pruebas muestra cómo instalar el paquete con Python 2.7 en
Docker. Para comprobar qué copia del paquete carga un entorno existente,
ejecuta este comando con su intérprete desde fuera del repositorio.

```bash
python -c 'import libcnmc; print(libcnmc.__file__)'
```

## Uso de los modelos

Los modelos de los formularios F1 a F8 de las resoluciones 4771, 4131 y
4666 se importan desde `libcnmc.models`. Los de la resolución 4667 están en
`libcnmc.models.res_4667`.

Este ejemplo crea dos versiones de un registro de despacho (F6) con datos
ficticios. El campo `valor_inversion` se convierte en un entero.

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

`dump()` devuelve texto JSON y `dump("list")` devuelve los valores en el
orden del esquema. En JSON, los valores `Decimal` se representan como
cadenas. `diff()` compara todos los campos por defecto. El operador `==`
aplica el criterio de comparación de cada formulario.

## Línea de comandos

Para generar ficheros con `cnmc`, utiliza un entorno Python 2.7 con acceso a
GISCE ERP/OpenERP. Desde la raíz del repositorio, instala el paquete, el
cliente OOOP y las dependencias de los formularios.

```bash
python -m pip install --upgrade 'pip<21' 'setuptools<45' wheel
python -m pip install . 'psycopg2-binary<2.9' 'pandas<0.25' \
  https://github.com/gisce/ooop/archive/xmlrpc_transaction.zip
python -m pip install --no-deps workalendar pyCalverter lunardate pyluach
cnmc --help
cnmc cir_8_2021_fb3_3 --help
```

La ayuda de cada comando indica los parámetros que admite. Para el formulario
B3.3, indica el año con `--year`, el código de la distribuidora con
`--codi-r1` y la ruta del fichero con `--output`. Las opciones `--server`,
`--port`, `--database`, `--user` y `--password` configuran la conexión al ERP.

## Pruebas

GitHub Actions ejecuta la suite Mamba con cobertura en Python 2.7 y 3.11
cuando un PR hacia `master` tiene la etiqueta `to be merged`. La suite está
en `spec/` y se puede ejecutar sin conexión al ERP. [Workflow de pruebas](.github/workflows/tests.yml)

### Python 3.11

Con el entorno virtual activado, ejecuta estos comandos desde la raíz del
repositorio.

```bash
python -m pip install . 'mamba>=0.11.3' psycopg2-binary pandas workalendar expects
mamba --enable-coverage
```

Para probar solo los modelos, ejecuta `mamba spec/models`.

### Python 2.7

Desde la raíz del repositorio, ejecuta las pruebas en un contenedor. El
contenedor monta el repositorio en modo de solo lectura y trabaja sobre
una copia temporal.

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

## Referencias normativas

| Ámbito | Módulo | Referencia |
| --- | --- | --- |
| Información regulatoria | `libcnmc/cir_8_2021/` | [Circular 8/2021](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2021-21003) |
| Información regulatoria | `libcnmc/cir_4_2015/` | [Circular 4/2015](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2015-8624) |
| Información regulatoria | `libcnmc/cir_4_2014/` | [Circular 4/2014](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2014-8511) |
| Bono social | `libcnmc/cir_3_2015/` | [Circular 3/2015](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2015-8985) |
| Inventario auditado | `libcnmc/res_4603/` | [BOE-A-2014-4603](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2014-4603) |
| Inventario auditado | `libcnmc/res_4771/` | [BOE-A-2015-4771](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2015-4771) |
| Inventario auditado | `libcnmc/res_4131/` | [BOE-A-2016-4131](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2016-4131) |
| Inventario auditado | `libcnmc/res_4666/` | [BOE-A-2017-4666](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2017-4666) |
| Planes de inversión | `libcnmc/res_4667/` | [BOE-A-2017-4667](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2017-4667) |
| Auditoría de inversiones | `libcnmc/audit_6181/` | [BOE-A-2019-6181](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2019-6181) |
