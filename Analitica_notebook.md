{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "bb0d5039-3d17-40a1-a707-38e7c45a6bf1",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Trabajo en Clase"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "72f4f66d-444c-4c40-837d-01c60c26fbeb",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "### Sesiones 1 y 2 · Práctica en Databricks"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "06ba4917-1556-4472-9f96-ad6dba3c3a5a",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Arquitecturas ETL y Modelos de IA Aplicados · EAM · 2026-II\n",
    "Robert García Rey\n",
    "\n",
    "En esta práctica crearemos un notebook desde cero para conocer el entorno de Databricks, cargar archivos y trabajar con SQL y PySpark. Consultaremos tablas, aplicaremos operaciones Delta y crearemos una vista y una función."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "4f3f2ccc-abb4-40bb-9ee0-405fb7e79dfc",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "#### Desarrollo"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "e3cb7a51-7f95-4b50-b13c-f0a82573b942",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Establece analitica.data como el contexto de trabajo para ejecutar las siguientes consultas SQL sobre sus tablas."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270480671,
     "inputWidgets": {},
     "nuid": "f3cad9c9-6b66-463a-af2e-03c910335db4",
     "showTitle": false,
     "startTime": 1792270479651,
     "submitTime": 1792270479290,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr></tr></thead><tbody></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 3
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "USE analitica.data"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "2d03916a-e543-4dcb-a3b6-3192d6c8d11f",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Crea o reemplaza la tabla tabla_simple con dos columnas: id de tipo entero y nombre de tipo texto. La tabla utiliza el formato Delta y, si ya existe, reemplaza su contenido y estructura."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270483074,
     "inputWidgets": {},
     "nuid": "cffa0396-bbe8-41a9-bbe4-32c19ff624a8",
     "showTitle": false,
     "startTime": 1792270480844,
     "submitTime": 1792270479295,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.databricks.empty-table+json": {
       "directive_name": "CreateTable"
      }
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE TABLE tabla_simple(\n",
    "    id INT,\n",
    "    nombre STRING\n",
    ")\n",
    "\n",
    "USING DELTA"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "d9686413-dccb-4310-95cd-07662a886725",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Inserta tres registros en la tabla tabla_simple, con los IDs 1, 2 y 3, y los nombres Ana, Carlos y María."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270489127,
     "inputWidgets": {},
     "nuid": "8ea03f5f-d9c4-4932-9b31-f69935911811",
     "showTitle": false,
     "startTime": 1792270483112,
     "submitTime": 1792270479300,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>num_affected_rows</th><th>num_inserted_rows</th></tr></thead><tbody><tr><td>3</td><td>3</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         3,
         3
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 5
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "num_affected_rows",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "num_inserted_rows",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "INSERT INTO tabla_simple (id, nombre)\n",
    "VALUES\n",
    "    (1, 'Ana'),\n",
    "    (2, 'Carlos'),\n",
    "    (3, 'María');"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "6b67f664-3331-4004-8435-b236781a7c0b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Consulta y muestra todos los registros y columnas de la tabla tabla_simple."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270491124,
     "inputWidgets": {},
     "nuid": "4aa42070-c3ca-4eed-96d1-3337f7d4c37a",
     "showTitle": false,
     "startTime": 1792270489183,
     "submitTime": 1792270479307,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>nombre</th></tr></thead><tbody><tr><td>1</td><td>Ana</td></tr><tr><td>2</td><td>Carlos</td></tr><tr><td>3</td><td>María</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         "Ana"
        ],
        [
         2,
         "Carlos"
        ],
        [
         3,
         "María"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 6
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "nombre",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT * \n",
    "FROM tabla_simple;"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "75370420-1295-4109-9f3f-f1621d6902f0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Importa las funciones de PySpark con el alias F y guarda en la variable ruta la ubicación /volumenes/analitica/data/landing, donde se encuentran o se almacenarán archivos."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270491432,
     "inputWidgets": {},
     "nuid": "fd86c6c3-637f-4fd3-8232-5b5ce01a64b6",
     "showTitle": false,
     "startTime": 1792270491174,
     "submitTime": 1792270479311,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql import functions as F\n",
    "ruta= \"/volumenes/analitica/data/landing\""
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "29e2b22f-56b4-47c9-837c-5064e4ac667b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Crea o reemplaza la función fn_mayor_que_10, que recibe un número decimal y devuelve TRUE si es mayor que 10 o FALSE si es menor o igual a 10."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270492940,
     "inputWidgets": {},
     "nuid": "a7254de9-cf4c-4b6f-8273-97f5eebe1c7a",
     "showTitle": false,
     "startTime": 1792270491519,
     "submitTime": 1792270479316,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr></tr></thead><tbody></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 8
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "%sql\n",
    "CREATE OR REPLACE FUNCTION analitica.data.fn_mayor_que_10(x DOUBLE)\n",
    "RETURNS BOOLEAN\n",
    "RETURN x > 10;\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a3f693fc-877a-4a88-a2f9-2d362942e901",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Consulta todos los registros de tabla_simple y devuelve únicamente las filas si la función fn_mayor_que_10(11) es verdadera. Como 11 es mayor que 10, se muestran todos los registros de la tabla."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270494650,
     "inputWidgets": {},
     "nuid": "61cc8ed2-bd94-4fc8-9302-d61878fc518f",
     "showTitle": false,
     "startTime": 1792270492968,
     "submitTime": 1792270479321,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>nombre</th></tr></thead><tbody><tr><td>1</td><td>Ana</td></tr><tr><td>2</td><td>Carlos</td></tr><tr><td>3</td><td>María</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         "Ana"
        ],
        [
         2,
         "Carlos"
        ],
        [
         3,
         "María"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 9
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "nombre",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT * FROM tabla_simple \n",
    "WHERE analitica.data.fn_mayor_que_10(11)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "fd4b609d-8741-4b3d-bfa1-8f49755feab7",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Lee el archivo data_demo.csv desde la ruta indicada, detecta automáticamente los tipos de datos y utiliza la primera fila como encabezado. Luego muestra las primeras 10 filas del DataFrame df."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270496867,
     "inputWidgets": {},
     "nuid": "f2f3cd60-5769-4c86-aedd-13560439f3c3",
     "showTitle": false,
     "startTime": 1792270494671,
     "submitTime": 1792270479326,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>rooms</th><th>garages</th><th>useful_area</th><th>value</th><th>interior_quality</th><th>time_on_market</th><th>has_outlier</th></tr></thead><tbody><tr><td>3.0</td><td>1.0</td><td>105.0</td><td>1038640.0</td><td>2</td><td>31.7870789438591</td><td>0</td></tr><tr><td>3.0</td><td>2.0</td><td>76.0</td><td>606405.0</td><td>4</td><td>209.46884172046086</td><td>0</td></tr><tr><td>3.0</td><td>1.0</td><td>123.0</td><td>1534500.0</td><td>1</td><td>38.0</td><td>0</td></tr><tr><td>3.0</td><td>2.0</td><td>180.0</td><td>1131950.0</td><td>5</td><td>154.0</td><td>0</td></tr><tr><td>3.0</td><td>1.0</td><td>67.0</td><td>452672.0</td><td>4</td><td>15.0</td><td>0</td></tr><tr><td>3.0</td><td>3.0</td><td>198.0</td><td>3116290.0</td><td>3</td><td>306.3557294709182</td><td>0</td></tr><tr><td>4.0</td><td>2.0</td><td>245.0</td><td>1202560.0</td><td>3</td><td>24.824464122611424</td><td>0</td></tr><tr><td>2.0</td><td>2.0</td><td>66.0</td><td>419282.0</td><td>5</td><td>24.0</td><td>0</td></tr><tr><td>2.0</td><td>1.0</td><td>114.0</td><td>1830260.0</td><td>1</td><td>251.39533835163672</td><td>0</td></tr><tr><td>2.0</td><td>1.0</td><td>56.0</td><td>274005.0</td><td>5</td><td>93.0</td><td>0</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         3.0,
         1.0,
         105.0,
         1038640.0,
         2,
         31.7870789438591,
         0
        ],
        [
         3.0,
         2.0,
         76.0,
         606405.0,
         4,
         209.46884172046086,
         0
        ],
        [
         3.0,
         1.0,
         123.0,
         1534500.0,
         1,
         38.0,
         0
        ],
        [
         3.0,
         2.0,
         180.0,
         1131950.0,
         5,
         154.0,
         0
        ],
        [
         3.0,
         1.0,
         67.0,
         452672.0,
         4,
         15.0,
         0
        ],
        [
         3.0,
         3.0,
         198.0,
         3116290.0,
         3,
         306.3557294709182,
         0
        ],
        [
         4.0,
         2.0,
         245.0,
         1202560.0,
         3,
         24.824464122611424,
         0
        ],
        [
         2.0,
         2.0,
         66.0,
         419282.0,
         5,
         24.0,
         0
        ],
        [
         2.0,
         1.0,
         114.0,
         1830260.0,
         1,
         251.39533835163672,
         0
        ],
        [
         2.0,
         1.0,
         56.0,
         274005.0,
         5,
         93.0,
         0
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "rooms",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "garages",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "useful_area",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "value",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "interior_quality",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "time_on_market",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "has_outlier",
         "type": "\"integer\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "path_data_demo = \"/Volumes/analitica/data/landing/data_demo.csv\"\n",
    "\n",
    "df = spark.read.csv(\n",
    "    path=path_data_demo,\n",
    "    header=True,\n",
    "    inferSchema=True\n",
    ")\n",
    "\n",
    "display(df.limit(10))\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "89a94d90-d20a-4671-9afa-3aa643845f75",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Guarda el DataFrame df como una tabla Delta llamada inmuebles. Si la tabla ya existe, sobrescribe sus datos actuales."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270499357,
     "inputWidgets": {},
     "nuid": "d135ab00-8608-464e-9e5b-4fa244cc891a",
     "showTitle": false,
     "startTime": 1792270496904,
     "submitTime": 1792270479332,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"inmuebles\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "9724016d-a54d-4f23-ad9e-8d70777f253d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Carga la tabla inmuebles en el DataFrame df_inmuebles y muestra sus primeras 3 filas."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270500662,
     "inputWidgets": {},
     "nuid": "1f708609-1837-4865-9080-785523c6ece9",
     "showTitle": false,
     "startTime": 1792270499379,
     "submitTime": 1792270479336,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>rooms</th><th>garages</th><th>useful_area</th><th>value</th><th>interior_quality</th><th>time_on_market</th><th>has_outlier</th></tr></thead><tbody><tr><td>3.0</td><td>1.0</td><td>105.0</td><td>1038640.0</td><td>2</td><td>31.7870789438591</td><td>0</td></tr><tr><td>3.0</td><td>2.0</td><td>76.0</td><td>606405.0</td><td>4</td><td>209.46884172046086</td><td>0</td></tr><tr><td>3.0</td><td>1.0</td><td>123.0</td><td>1534500.0</td><td>1</td><td>38.0</td><td>0</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         3.0,
         1.0,
         105.0,
         1038640.0,
         2,
         31.7870789438591,
         0
        ],
        [
         3.0,
         2.0,
         76.0,
         606405.0,
         4,
         209.46884172046086,
         0
        ],
        [
         3.0,
         1.0,
         123.0,
         1534500.0,
         1,
         38.0,
         0
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "rooms",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "garages",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "useful_area",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "value",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "interior_quality",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "time_on_market",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "has_outlier",
         "type": "\"integer\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "df_inmuebles = spark.table(\"inmuebles\")\n",
    "display(df_inmuebles.limit(3))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "258280a7-6edb-49d9-aeeb-5e10580d14c0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Consulta y muestra las primeras 5 filas de la tabla inmuebles con todas sus columnas."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270502268,
     "inputWidgets": {},
     "nuid": "08361730-efec-4a00-9048-26191be0eb97",
     "showTitle": false,
     "startTime": 1792270500703,
     "submitTime": 1792270479341,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>rooms</th><th>garages</th><th>useful_area</th><th>value</th><th>interior_quality</th><th>time_on_market</th><th>has_outlier</th></tr></thead><tbody><tr><td>3.0</td><td>1.0</td><td>105.0</td><td>1038640.0</td><td>2</td><td>31.7870789438591</td><td>0</td></tr><tr><td>3.0</td><td>2.0</td><td>76.0</td><td>606405.0</td><td>4</td><td>209.46884172046086</td><td>0</td></tr><tr><td>3.0</td><td>1.0</td><td>123.0</td><td>1534500.0</td><td>1</td><td>38.0</td><td>0</td></tr><tr><td>3.0</td><td>2.0</td><td>180.0</td><td>1131950.0</td><td>5</td><td>154.0</td><td>0</td></tr><tr><td>3.0</td><td>1.0</td><td>67.0</td><td>452672.0</td><td>4</td><td>15.0</td><td>0</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         3.0,
         1.0,
         105.0,
         1038640.0,
         2,
         31.7870789438591,
         0
        ],
        [
         3.0,
         2.0,
         76.0,
         606405.0,
         4,
         209.46884172046086,
         0
        ],
        [
         3.0,
         1.0,
         123.0,
         1534500.0,
         1,
         38.0,
         0
        ],
        [
         3.0,
         2.0,
         180.0,
         1131950.0,
         5,
         154.0,
         0
        ],
        [
         3.0,
         1.0,
         67.0,
         452672.0,
         4,
         15.0,
         0
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 13
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "rooms",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "garages",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "useful_area",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "value",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "interior_quality",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "time_on_market",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "has_outlier",
         "type": "\"integer\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT * FROM inmuebles LIMIT 5"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "1cad0ce3-e6cd-4599-aada-231bec8b7f6f",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Lee el archivo spark_intro.csv, toma la primera fila como encabezado, infiere automáticamente los tipos de datos y muestra las primeras 10 filas en el DataFrame df_pedidos."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270503968,
     "inputWidgets": {},
     "nuid": "8074992a-ddd3-4c35-b150-ee2565688f2a",
     "showTitle": false,
     "startTime": 1792270502296,
     "submitTime": 1792270479346,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>order_id</th><th>order_date</th><th>country</th><th>product</th><th>category</th><th>units_sold</th><th>unit_price</th></tr></thead><tbody><tr><td>1</td><td>2024-01-02</td><td>Spain</td><td>Laptop</td><td>Electronics</td><td>2</td><td>1200</td></tr><tr><td>2</td><td>2024-01-03</td><td>Spain</td><td>Mouse</td><td>Accessories</td><td>5</td><td>25</td></tr><tr><td>3</td><td>2024-01-05</td><td>France</td><td>Laptop</td><td>Electronics</td><td>1</td><td>1150</td></tr><tr><td>4</td><td>2024-01-06</td><td>Germany</td><td>Keyboard</td><td>Accessories</td><td>3</td><td>45</td></tr><tr><td>5</td><td>2024-01-07</td><td>Spain</td><td>Monitor</td><td>Electronics</td><td>2</td><td>300</td></tr><tr><td>6</td><td>2024-01-08</td><td>France</td><td>Mouse</td><td>Accessories</td><td>10</td><td>23</td></tr><tr><td>7</td><td>2024-01-10</td><td>Germany</td><td>Laptop</td><td>Electronics</td><td>1</td><td>1300</td></tr><tr><td>8</td><td>2024-01-11</td><td>Spain</td><td>Keyboard</td><td>Accessories</td><td>4</td><td>42</td></tr><tr><td>9</td><td>2024-01-12</td><td>France</td><td>Monitor</td><td>Electronics</td><td>1</td><td>280</td></tr><tr><td>10</td><td>2024-01-14</td><td>Germany</td><td>Mouse</td><td>Accessories</td><td>6</td><td>27</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         "2024-01-02",
         "Spain",
         "Laptop",
         "Electronics",
         2,
         1200
        ],
        [
         2,
         "2024-01-03",
         "Spain",
         "Mouse",
         "Accessories",
         5,
         25
        ],
        [
         3,
         "2024-01-05",
         "France",
         "Laptop",
         "Electronics",
         1,
         1150
        ],
        [
         4,
         "2024-01-06",
         "Germany",
         "Keyboard",
         "Accessories",
         3,
         45
        ],
        [
         5,
         "2024-01-07",
         "Spain",
         "Monitor",
         "Electronics",
         2,
         300
        ],
        [
         6,
         "2024-01-08",
         "France",
         "Mouse",
         "Accessories",
         10,
         23
        ],
        [
         7,
         "2024-01-10",
         "Germany",
         "Laptop",
         "Electronics",
         1,
         1300
        ],
        [
         8,
         "2024-01-11",
         "Spain",
         "Keyboard",
         "Accessories",
         4,
         42
        ],
        [
         9,
         "2024-01-12",
         "France",
         "Monitor",
         "Electronics",
         1,
         280
        ],
        [
         10,
         "2024-01-14",
         "Germany",
         "Mouse",
         "Accessories",
         6,
         27
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "order_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{\"__detected_date_formats\": \"yyyy-M-d\"}",
         "name": "order_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "country",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "units_sold",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "unit_price",
         "type": "\"integer\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "path_data_demo = \"/Volumes/analitica/data/landing/spark_intro.csv\"\n",
    "\n",
    "df_pedidos = spark.read.csv(\n",
    "    path=path_data_demo,\n",
    "    header=True,\n",
    "    inferSchema=True\n",
    ")\n",
    "\n",
    "display(df_pedidos.limit(10))\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "7f7a0d79-299f-4bbe-946d-3efaa858cad5",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Importa las funciones de PySpark con el alias F para utilizarlas en operaciones de transformación y análisis de datos."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270504283,
     "inputWidgets": {},
     "nuid": "6da12d10-8fb2-4664-80e8-e7b89b69590a",
     "showTitle": false,
     "startTime": 1792270503996,
     "submitTime": 1792270479351,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql import functions as F"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "f828f959-df0e-4ac4-9d0e-a3e05d3ef0ed",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Selecciona las columnas order_id, country, product y units_sold del DataFrame df_pedidos y filtra los registros para mostrar únicamente los pedidos de España."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270504677,
     "inputWidgets": {},
     "nuid": "7423685a-a1e2-4aa5-8d00-03799d4112b4",
     "showTitle": false,
     "startTime": 1792270504305,
     "submitTime": 1792270479355,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "execute_result",
     "data": {
      "text/plain": [
       "DataFrame[order_id: int, country: string, product: string, units_sold: int]"
      ]
     },
     "execution_count": 16,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "df_pedidos.select(\"order_id\",\"country\",\"product\",\"units_sold\").filter(F.col(\"country\")== \"Spain\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "ba6d7bd7-4822-4693-a83c-468270fdd9e8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Crea la columna importe multiplicando las unidades vendidas (units_sold) por el precio unitario (unit_price) y muestra el DataFrame completo con los importes calculados."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270505492,
     "inputWidgets": {},
     "nuid": "93663671-dc7e-47d6-83da-bc5258ce3d10",
     "showTitle": false,
     "startTime": 1792270504740,
     "submitTime": 1792270479360,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>order_id</th><th>order_date</th><th>country</th><th>product</th><th>category</th><th>units_sold</th><th>unit_price</th><th>importe</th></tr></thead><tbody><tr><td>1</td><td>2024-01-02</td><td>Spain</td><td>Laptop</td><td>Electronics</td><td>2</td><td>1200</td><td>2400</td></tr><tr><td>2</td><td>2024-01-03</td><td>Spain</td><td>Mouse</td><td>Accessories</td><td>5</td><td>25</td><td>125</td></tr><tr><td>3</td><td>2024-01-05</td><td>France</td><td>Laptop</td><td>Electronics</td><td>1</td><td>1150</td><td>1150</td></tr><tr><td>4</td><td>2024-01-06</td><td>Germany</td><td>Keyboard</td><td>Accessories</td><td>3</td><td>45</td><td>135</td></tr><tr><td>5</td><td>2024-01-07</td><td>Spain</td><td>Monitor</td><td>Electronics</td><td>2</td><td>300</td><td>600</td></tr><tr><td>6</td><td>2024-01-08</td><td>France</td><td>Mouse</td><td>Accessories</td><td>10</td><td>23</td><td>230</td></tr><tr><td>7</td><td>2024-01-10</td><td>Germany</td><td>Laptop</td><td>Electronics</td><td>1</td><td>1300</td><td>1300</td></tr><tr><td>8</td><td>2024-01-11</td><td>Spain</td><td>Keyboard</td><td>Accessories</td><td>4</td><td>42</td><td>168</td></tr><tr><td>9</td><td>2024-01-12</td><td>France</td><td>Monitor</td><td>Electronics</td><td>1</td><td>280</td><td>280</td></tr><tr><td>10</td><td>2024-01-14</td><td>Germany</td><td>Mouse</td><td>Accessories</td><td>6</td><td>27</td><td>162</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         "2024-01-02",
         "Spain",
         "Laptop",
         "Electronics",
         2,
         1200,
         2400
        ],
        [
         2,
         "2024-01-03",
         "Spain",
         "Mouse",
         "Accessories",
         5,
         25,
         125
        ],
        [
         3,
         "2024-01-05",
         "France",
         "Laptop",
         "Electronics",
         1,
         1150,
         1150
        ],
        [
         4,
         "2024-01-06",
         "Germany",
         "Keyboard",
         "Accessories",
         3,
         45,
         135
        ],
        [
         5,
         "2024-01-07",
         "Spain",
         "Monitor",
         "Electronics",
         2,
         300,
         600
        ],
        [
         6,
         "2024-01-08",
         "France",
         "Mouse",
         "Accessories",
         10,
         23,
         230
        ],
        [
         7,
         "2024-01-10",
         "Germany",
         "Laptop",
         "Electronics",
         1,
         1300,
         1300
        ],
        [
         8,
         "2024-01-11",
         "Spain",
         "Keyboard",
         "Accessories",
         4,
         42,
         168
        ],
        [
         9,
         "2024-01-12",
         "France",
         "Monitor",
         "Electronics",
         1,
         280,
         280
        ],
        [
         10,
         "2024-01-14",
         "Germany",
         "Mouse",
         "Accessories",
         6,
         27,
         162
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "order_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{\"__detected_date_formats\": \"yyyy-M-d\"}",
         "name": "order_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "country",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "units_sold",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "unit_price",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "importe",
         "type": "\"integer\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "df_importes = df_pedidos.withColumn(\n",
    "    \"importe\",\n",
    "    F.col(\"units_sold\") * F.col(\"unit_price\")\n",
    ")\n",
    "\n",
    "display(df_importes)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b6a14169-a15a-453f-8a45-a5384254ae27",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Calcula la suma de todos los valores de la columna importe para obtener las ventas totales y muestra el resultado con el nombre ventas_totales."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270506182,
     "inputWidgets": {},
     "nuid": "52a2f6da-b93f-4db6-83a8-c9b4df65b5c3",
     "showTitle": false,
     "startTime": 1792270505529,
     "submitTime": 1792270479365,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------+\n|ventas_totales|\n+--------------+\n|          6550|\n+--------------+\n\n"
     ]
    }
   ],
   "source": [
    "df_importes.agg(\n",
    "    F.sum(\"importe\").alias(\"ventas_totales\")\n",
    ").show()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b4753122-d78b-44e3-bcfd-79dff28e8c93",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Agrupa los pedidos por país, suma los importes de cada grupo para calcular las ventas totales por país y muestra los resultados."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270506880,
     "inputWidgets": {},
     "nuid": "e30a152d-b636-474a-9dab-a6f286ebde83",
     "showTitle": false,
     "startTime": 1792270506232,
     "submitTime": 1792270479370,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------+--------------+\n|country|ventas_totales|\n+-------+--------------+\n|  Spain|          3293|\n| France|          1660|\n|Germany|          1597|\n+-------+--------------+\n\n"
     ]
    }
   ],
   "source": [
    "df_importes.groupBy(\"country\").agg(\n",
    "    F.sum(\"importe\").alias(\"ventas_totales\")\n",
    ").show()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "e6e38c79-eb9b-4039-b8cd-6973ffc46f5e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Agrupa los pedidos por producto, calcula las ventas totales de cada uno y ordena los resultados de mayor a menor según las ventas."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270507592,
     "inputWidgets": {},
     "nuid": "d0e40529-00a1-440b-bb12-ae87b3b9364d",
     "showTitle": false,
     "startTime": 1792270506912,
     "submitTime": 1792270479376,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------+--------------+\n| product|ventas_totales|\n+--------+--------------+\n|  Laptop|          4850|\n| Monitor|           880|\n|   Mouse|           517|\n|Keyboard|           303|\n+--------+--------------+\n\n"
     ]
    }
   ],
   "source": [
    "df_importes.groupBy(\"product\").agg(\n",
    "    F.sum(\"importe\").alias(\"ventas_totales\")\n",
    ").orderBy(\n",
    "    F.desc(\"ventas_totales\")\n",
    ").show()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "4af6266f-b5da-498f-9fc0-868130e9e494",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Agrupa los pedidos por país, calcula el total de unidades vendidas y el importe total redondeado a dos decimales, y muestra los resultados ordenados de mayor a menor importe total."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270508484,
     "inputWidgets": {},
     "nuid": "bcbf9383-a111-41c7-8bfb-9dfbc0394016",
     "showTitle": false,
     "startTime": 1792270507617,
     "submitTime": 1792270479380,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>country</th><th>unidades</th><th>importe_total</th></tr></thead><tbody><tr><td>Spain</td><td>13</td><td>3293</td></tr><tr><td>France</td><td>12</td><td>1660</td></tr><tr><td>Germany</td><td>10</td><td>1597</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Spain",
         13,
         3293
        ],
        [
         "France",
         12,
         1660
        ],
        [
         "Germany",
         10,
         1597
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "country",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "unidades",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "importe_total",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "resumen_pais = df_importes.groupBy(\"country\").agg(\n",
    "    F.sum(\"units_sold\").alias(\"unidades\"),\n",
    "    F.round(F.sum(\"importe\"), 2).alias(\"importe_total\")\n",
    ")\n",
    "\n",
    "display(resumen_pais.orderBy(F.desc(\"importe_total\")))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "5a5c20e0-9002-48b0-941b-c61789b74632",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Lee el archivo dim_spark_intro.csv definiendo las columnas product, product_family y supplier como texto. Si encuentra datos con formato inválido, detiene la lectura y muestra el DataFrame df_productos."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270509489,
     "inputWidgets": {},
     "nuid": "fd239187-ef2f-40f8-8b5a-85b3cd329ab6",
     "showTitle": false,
     "startTime": 1792270508511,
     "submitTime": 1792270479386,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>product</th><th>product_family</th><th>supplier</th></tr></thead><tbody><tr><td>Laptop</td><td>Computers</td><td>TechCorp</td></tr><tr><td>Mouse</td><td>Peripherals</td><td>InputMakers</td></tr><tr><td>Keyboard</td><td>Peripherals</td><td>InputMakers</td></tr><tr><td>Monitor</td><td>Displays</td><td>ScreenWorld</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Laptop",
         "Computers",
         "TechCorp"
        ],
        [
         "Mouse",
         "Peripherals",
         "InputMakers"
        ],
        [
         "Keyboard",
         "Peripherals",
         "InputMakers"
        ],
        [
         "Monitor",
         "Displays",
         "ScreenWorld"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "product",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_family",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "supplier",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "path_file = \"/Volumes/analitica/data/landing/dim_spark_intro.csv\"\n",
    "\n",
    "\n",
    "df_productos = spark.read.csv(\n",
    "    path=path_file,\n",
    "    header=True,\n",
    "    schema= \"product STRING, product_family STRING, supplier STRING\",\n",
    "    mode=\"FAILFAST\"\n",
    ")\n",
    "\n",
    "display(df_productos)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "f1c05acf-25cb-482b-83b9-da6e74743cb8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Agrupa los pedidos por producto, cuenta cuántas veces aparece cada uno y muestra únicamente los productos que aparecen más de una vez."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270510292,
     "inputWidgets": {},
     "nuid": "062964a7-49dd-46ec-8b89-073613fcdf83",
     "showTitle": false,
     "startTime": 1792270509510,
     "submitTime": 1792270479391,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>product</th><th>count</th></tr></thead><tbody><tr><td>Mouse</td><td>3</td></tr><tr><td>Monitor</td><td>2</td></tr><tr><td>Keyboard</td><td>2</td></tr><tr><td>Laptop</td><td>3</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Mouse",
         3
        ],
        [
         "Monitor",
         2
        ],
        [
         "Keyboard",
         2
        ],
        [
         "Laptop",
         3
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "product",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "count",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "display(\n",
    "    df_pedidos\n",
    "    .groupBy(\"product\")\n",
    "    .count()\n",
    "    .filter(\"count > 1\")\n",
    ")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "57e65e06-4a50-47d7-9f1f-126f956294d0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Combina df_importes con df_productos mediante la columna product, conservando todos los pedidos y agregando la información de familia y proveedor cuando exista coincidencia. Muestra el DataFrame resultante df_enriquecido."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270511303,
     "inputWidgets": {},
     "nuid": "c7f9e538-0005-4477-9b08-f77085c242e2",
     "showTitle": false,
     "startTime": 1792270510312,
     "submitTime": 1792270479397,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>product</th><th>order_id</th><th>order_date</th><th>country</th><th>category</th><th>units_sold</th><th>unit_price</th><th>importe</th><th>product_family</th><th>supplier</th></tr></thead><tbody><tr><td>Laptop</td><td>1</td><td>2024-01-02</td><td>Spain</td><td>Electronics</td><td>2</td><td>1200</td><td>2400</td><td>Computers</td><td>TechCorp</td></tr><tr><td>Mouse</td><td>2</td><td>2024-01-03</td><td>Spain</td><td>Accessories</td><td>5</td><td>25</td><td>125</td><td>Peripherals</td><td>InputMakers</td></tr><tr><td>Laptop</td><td>3</td><td>2024-01-05</td><td>France</td><td>Electronics</td><td>1</td><td>1150</td><td>1150</td><td>Computers</td><td>TechCorp</td></tr><tr><td>Keyboard</td><td>4</td><td>2024-01-06</td><td>Germany</td><td>Accessories</td><td>3</td><td>45</td><td>135</td><td>Peripherals</td><td>InputMakers</td></tr><tr><td>Monitor</td><td>5</td><td>2024-01-07</td><td>Spain</td><td>Electronics</td><td>2</td><td>300</td><td>600</td><td>Displays</td><td>ScreenWorld</td></tr><tr><td>Mouse</td><td>6</td><td>2024-01-08</td><td>France</td><td>Accessories</td><td>10</td><td>23</td><td>230</td><td>Peripherals</td><td>InputMakers</td></tr><tr><td>Laptop</td><td>7</td><td>2024-01-10</td><td>Germany</td><td>Electronics</td><td>1</td><td>1300</td><td>1300</td><td>Computers</td><td>TechCorp</td></tr><tr><td>Keyboard</td><td>8</td><td>2024-01-11</td><td>Spain</td><td>Accessories</td><td>4</td><td>42</td><td>168</td><td>Peripherals</td><td>InputMakers</td></tr><tr><td>Monitor</td><td>9</td><td>2024-01-12</td><td>France</td><td>Electronics</td><td>1</td><td>280</td><td>280</td><td>Displays</td><td>ScreenWorld</td></tr><tr><td>Mouse</td><td>10</td><td>2024-01-14</td><td>Germany</td><td>Accessories</td><td>6</td><td>27</td><td>162</td><td>Peripherals</td><td>InputMakers</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Laptop",
         1,
         "2024-01-02",
         "Spain",
         "Electronics",
         2,
         1200,
         2400,
         "Computers",
         "TechCorp"
        ],
        [
         "Mouse",
         2,
         "2024-01-03",
         "Spain",
         "Accessories",
         5,
         25,
         125,
         "Peripherals",
         "InputMakers"
        ],
        [
         "Laptop",
         3,
         "2024-01-05",
         "France",
         "Electronics",
         1,
         1150,
         1150,
         "Computers",
         "TechCorp"
        ],
        [
         "Keyboard",
         4,
         "2024-01-06",
         "Germany",
         "Accessories",
         3,
         45,
         135,
         "Peripherals",
         "InputMakers"
        ],
        [
         "Monitor",
         5,
         "2024-01-07",
         "Spain",
         "Electronics",
         2,
         300,
         600,
         "Displays",
         "ScreenWorld"
        ],
        [
         "Mouse",
         6,
         "2024-01-08",
         "France",
         "Accessories",
         10,
         23,
         230,
         "Peripherals",
         "InputMakers"
        ],
        [
         "Laptop",
         7,
         "2024-01-10",
         "Germany",
         "Electronics",
         1,
         1300,
         1300,
         "Computers",
         "TechCorp"
        ],
        [
         "Keyboard",
         8,
         "2024-01-11",
         "Spain",
         "Accessories",
         4,
         42,
         168,
         "Peripherals",
         "InputMakers"
        ],
        [
         "Monitor",
         9,
         "2024-01-12",
         "France",
         "Electronics",
         1,
         280,
         280,
         "Displays",
         "ScreenWorld"
        ],
        [
         "Mouse",
         10,
         "2024-01-14",
         "Germany",
         "Accessories",
         6,
         27,
         162,
         "Peripherals",
         "InputMakers"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "product",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "order_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{\"__detected_date_formats\": \"yyyy-M-d\"}",
         "name": "order_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "country",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "units_sold",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "unit_price",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "importe",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "product_family",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "supplier",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "df_enriquecido = df_importes.join(df_productos,\"product\", \"left\")\n",
    "\n",
    "display(df_enriquecido)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "05cc8330-f93e-47f0-bc2c-4e737a290df2",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Cuenta los registros antes y después de la unión y verifica si ambas cantidades son iguales; si no coinciden, indica que se debe revisar la llave product por posibles duplicados."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270513513,
     "inputWidgets": {},
     "nuid": "f0bb658b-1c79-4984-8508-1a3132f025e7",
     "showTitle": false,
     "startTime": 1792270511330,
     "submitTime": 1792270479402,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "pedidos antes:  10\npedidos despues:  10\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/plain": [
       "True"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "text/plain": [
       "'revisar la llave por producto'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "print(\"pedidos antes: \", df_pedidos.count())\n",
    "print(\"pedidos despues: \", df_enriquecido.count())\n",
    "display(df_enriquecido.count()== df_pedidos.count(),\"revisar la llave por producto\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "2aa89fe2-f151-44d1-af61-466f07bef473",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Guarda el DataFrame df_enriquecido como una tabla Delta llamada pedidos_enriquecido, sobrescribiendo los datos existentes si la tabla ya existe."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270516223,
     "inputWidgets": {},
     "nuid": "3aa8421a-e2db-4258-89f6-4aba3eef8c3c",
     "showTitle": false,
     "startTime": 1792270513531,
     "submitTime": 1792270479407,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "df_enriquecido.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"pedidos_enriquecido\")\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "de3ca6c8-8cc0-4dcd-b56d-9c3fba287664",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Lee el archivo ventas_2025.csv definiendo las columnas id, fecha, producto, cantidad y precio con sus tipos de datos correspondientes. Si encuentra datos inválidos, detiene la lectura y muestra el DataFrame df_ventas."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270517219,
     "inputWidgets": {},
     "nuid": "e1f0e908-47fc-4872-a481-0ada7854582b",
     "showTitle": false,
     "startTime": 1792270516241,
     "submitTime": 1792270479413,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>fecha</th><th>producto</th><th>cantidad</th><th>precio</th></tr></thead><tbody><tr><td>1</td><td>2025-01-03</td><td>Teclado</td><td>20</td><td>25.99</td></tr><tr><td>2</td><td>2025-01-03</td><td>Webcam</td><td>1</td><td>39.99</td></tr><tr><td>3</td><td>2025-01-03</td><td>Auriculares</td><td>18</td><td>29.99</td></tr><tr><td>4</td><td>2025-01-04</td><td>Webcam</td><td>10</td><td>39.99</td></tr><tr><td>5</td><td>2025-01-04</td><td>Mouse</td><td>2</td><td>15.5</td></tr><tr><td>6</td><td>2025-01-05</td><td>Altavoces</td><td>13</td><td>45.0</td></tr><tr><td>7</td><td>2025-01-08</td><td>Teclado</td><td>11</td><td>25.99</td></tr><tr><td>8</td><td>2025-01-10</td><td>Auriculares</td><td>5</td><td>29.99</td></tr><tr><td>9</td><td>2025-01-11</td><td>Monitor</td><td>2</td><td>199.99</td></tr><tr><td>10</td><td>2025-01-12</td><td>Microfono</td><td>20</td><td>59.99</td></tr><tr><td>11</td><td>2025-01-12</td><td>USB</td><td>8</td><td>9.99</td></tr><tr><td>12</td><td>2025-01-12</td><td>Webcam</td><td>16</td><td>39.99</td></tr><tr><td>13</td><td>2025-01-13</td><td>USB</td><td>6</td><td>9.99</td></tr><tr><td>14</td><td>2025-01-14</td><td>Altavoces</td><td>20</td><td>45.0</td></tr><tr><td>15</td><td>2025-01-15</td><td>Auriculares</td><td>17</td><td>29.99</td></tr><tr><td>16</td><td>2025-01-18</td><td>USB</td><td>16</td><td>9.99</td></tr><tr><td>17</td><td>2025-01-18</td><td>Impresora</td><td>13</td><td>89.9</td></tr><tr><td>18</td><td>2025-01-19</td><td>USB</td><td>12</td><td>9.99</td></tr><tr><td>19</td><td>2025-01-24</td><td>Mouse</td><td>8</td><td>15.5</td></tr><tr><td>20</td><td>2025-01-26</td><td>Webcam</td><td>7</td><td>39.99</td></tr><tr><td>21</td><td>2025-01-27</td><td>Altavoces</td><td>4</td><td>45.0</td></tr><tr><td>22</td><td>2025-01-27</td><td>Disco Duro</td><td>12</td><td>79.99</td></tr><tr><td>23</td><td>2025-01-28</td><td>Impresora</td><td>10</td><td>89.9</td></tr><tr><td>24</td><td>2025-01-28</td><td>Webcam</td><td>15</td><td>39.99</td></tr><tr><td>25</td><td>2025-01-31</td><td>Webcam</td><td>8</td><td>39.99</td></tr><tr><td>26</td><td>2025-01-31</td><td>Auriculares</td><td>18</td><td>29.99</td></tr><tr><td>27</td><td>2025-02-01</td><td>Disco Duro</td><td>14</td><td>79.99</td></tr><tr><td>28</td><td>2025-02-01</td><td>USB</td><td>9</td><td>9.99</td></tr><tr><td>29</td><td>2025-02-01</td><td>Altavoces</td><td>20</td><td>45.0</td></tr><tr><td>30</td><td>2025-02-02</td><td>Teclado</td><td>17</td><td>25.99</td></tr><tr><td>31</td><td>2025-02-02</td><td>Mouse</td><td>13</td><td>15.5</td></tr><tr><td>32</td><td>2025-02-06</td><td>Altavoces</td><td>7</td><td>45.0</td></tr><tr><td>33</td><td>2025-02-07</td><td>USB</td><td>14</td><td>9.99</td></tr><tr><td>34</td><td>2025-02-08</td><td>Teclado</td><td>14</td><td>25.99</td></tr><tr><td>35</td><td>2025-02-09</td><td>Impresora</td><td>17</td><td>89.9</td></tr><tr><td>36</td><td>2025-02-09</td><td>Monitor</td><td>12</td><td>199.99</td></tr><tr><td>37</td><td>2025-02-09</td><td>Altavoces</td><td>20</td><td>45.0</td></tr><tr><td>38</td><td>2025-02-10</td><td>Monitor</td><td>8</td><td>199.99</td></tr><tr><td>39</td><td>2025-02-12</td><td>Impresora</td><td>19</td><td>89.9</td></tr><tr><td>40</td><td>2025-02-12</td><td>Webcam</td><td>19</td><td>39.99</td></tr><tr><td>41</td><td>2025-02-12</td><td>Mouse</td><td>5</td><td>15.5</td></tr><tr><td>42</td><td>2025-02-12</td><td>Disco Duro</td><td>10</td><td>79.99</td></tr><tr><td>43</td><td>2025-02-13</td><td>Microfono</td><td>5</td><td>59.99</td></tr><tr><td>44</td><td>2025-02-15</td><td>Webcam</td><td>18</td><td>39.99</td></tr><tr><td>45</td><td>2025-02-15</td><td>Teclado</td><td>2</td><td>25.99</td></tr><tr><td>46</td><td>2025-02-16</td><td>Auriculares</td><td>6</td><td>29.99</td></tr><tr><td>47</td><td>2025-02-18</td><td>USB</td><td>6</td><td>9.99</td></tr><tr><td>48</td><td>2025-02-18</td><td>Mouse</td><td>2</td><td>15.5</td></tr><tr><td>49</td><td>2025-02-19</td><td>Impresora</td><td>3</td><td>89.9</td></tr><tr><td>50</td><td>2025-02-20</td><td>Microfono</td><td>11</td><td>59.99</td></tr><tr><td>51</td><td>2025-02-23</td><td>Auriculares</td><td>10</td><td>29.99</td></tr><tr><td>52</td><td>2025-02-25</td><td>Auriculares</td><td>8</td><td>29.99</td></tr><tr><td>53</td><td>2025-02-26</td><td>Microfono</td><td>15</td><td>59.99</td></tr><tr><td>54</td><td>2025-02-26</td><td>Altavoces</td><td>10</td><td>45.0</td></tr><tr><td>55</td><td>2025-02-27</td><td>Teclado</td><td>5</td><td>25.99</td></tr><tr><td>56</td><td>2025-02-28</td><td>Teclado</td><td>18</td><td>25.99</td></tr><tr><td>57</td><td>2025-02-28</td><td>Microfono</td><td>12</td><td>59.99</td></tr><tr><td>58</td><td>2025-02-28</td><td>Disco Duro</td><td>18</td><td>79.99</td></tr><tr><td>59</td><td>2025-03-03</td><td>Teclado</td><td>18</td><td>25.99</td></tr><tr><td>60</td><td>2025-03-03</td><td>Monitor</td><td>16</td><td>199.99</td></tr><tr><td>61</td><td>2025-03-05</td><td>Impresora</td><td>2</td><td>89.9</td></tr><tr><td>62</td><td>2025-03-06</td><td>Microfono</td><td>8</td><td>59.99</td></tr><tr><td>63</td><td>2025-03-07</td><td>Mouse</td><td>18</td><td>15.5</td></tr><tr><td>64</td><td>2025-03-08</td><td>Microfono</td><td>2</td><td>59.99</td></tr><tr><td>65</td><td>2025-03-10</td><td>Teclado</td><td>2</td><td>25.99</td></tr><tr><td>66</td><td>2025-03-10</td><td>Disco Duro</td><td>19</td><td>79.99</td></tr><tr><td>67</td><td>2025-03-12</td><td>Mouse</td><td>5</td><td>15.5</td></tr><tr><td>68</td><td>2025-03-12</td><td>Impresora</td><td>3</td><td>89.9</td></tr><tr><td>69</td><td>2025-03-12</td><td>Impresora</td><td>1</td><td>89.9</td></tr><tr><td>70</td><td>2025-03-13</td><td>Auriculares</td><td>10</td><td>29.99</td></tr><tr><td>71</td><td>2025-03-14</td><td>Webcam</td><td>12</td><td>39.99</td></tr><tr><td>72</td><td>2025-03-15</td><td>Monitor</td><td>12</td><td>199.99</td></tr><tr><td>73</td><td>2025-03-20</td><td>Altavoces</td><td>20</td><td>45.0</td></tr><tr><td>74</td><td>2025-03-20</td><td>Microfono</td><td>13</td><td>59.99</td></tr><tr><td>75</td><td>2025-03-21</td><td>USB</td><td>6</td><td>9.99</td></tr><tr><td>76</td><td>2025-03-21</td><td>USB</td><td>3</td><td>9.99</td></tr><tr><td>77</td><td>2025-03-22</td><td>Monitor</td><td>3</td><td>199.99</td></tr><tr><td>78</td><td>2025-03-23</td><td>USB</td><td>5</td><td>9.99</td></tr><tr><td>79</td><td>2025-03-23</td><td>Altavoces</td><td>16</td><td>45.0</td></tr><tr><td>80</td><td>2025-03-24</td><td>Webcam</td><td>12</td><td>39.99</td></tr><tr><td>81</td><td>2025-03-26</td><td>Webcam</td><td>6</td><td>39.99</td></tr><tr><td>82</td><td>2025-03-26</td><td>Altavoces</td><td>3</td><td>45.0</td></tr><tr><td>83</td><td>2025-03-26</td><td>Teclado</td><td>13</td><td>25.99</td></tr><tr><td>84</td><td>2025-03-27</td><td>Microfono</td><td>6</td><td>59.99</td></tr><tr><td>85</td><td>2025-03-28</td><td>Disco Duro</td><td>4</td><td>79.99</td></tr><tr><td>86</td><td>2025-03-28</td><td>Impresora</td><td>12</td><td>89.9</td></tr><tr><td>87</td><td>2025-03-29</td><td>Mouse</td><td>4</td><td>15.5</td></tr><tr><td>88</td><td>2025-03-29</td><td>Altavoces</td><td>4</td><td>45.0</td></tr><tr><td>89</td><td>2025-03-30</td><td>Microfono</td><td>1</td><td>59.99</td></tr><tr><td>90</td><td>2025-03-30</td><td>USB</td><td>2</td><td>9.99</td></tr><tr><td>91</td><td>2025-04-01</td><td>Impresora</td><td>5</td><td>89.9</td></tr><tr><td>92</td><td>2025-04-02</td><td>Monitor</td><td>4</td><td>199.99</td></tr><tr><td>93</td><td>2025-04-02</td><td>Webcam</td><td>9</td><td>39.99</td></tr><tr><td>94</td><td>2025-04-02</td><td>Webcam</td><td>4</td><td>39.99</td></tr><tr><td>95</td><td>2025-04-03</td><td>USB</td><td>12</td><td>9.99</td></tr><tr><td>96</td><td>2025-04-03</td><td>Mouse</td><td>5</td><td>15.5</td></tr><tr><td>97</td><td>2025-04-05</td><td>Monitor</td><td>7</td><td>199.99</td></tr><tr><td>98</td><td>2025-04-06</td><td>Mouse</td><td>10</td><td>15.5</td></tr><tr><td>99</td><td>2025-04-06</td><td>Teclado</td><td>10</td><td>25.99</td></tr><tr><td>100</td><td>2025-04-06</td><td>Mouse</td><td>16</td><td>15.5</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         "2025-01-03",
         "Teclado",
         20,
         25.99
        ],
        [
         2,
         "2025-01-03",
         "Webcam",
         1,
         39.99
        ],
        [
         3,
         "2025-01-03",
         "Auriculares",
         18,
         29.99
        ],
        [
         4,
         "2025-01-04",
         "Webcam",
         10,
         39.99
        ],
        [
         5,
         "2025-01-04",
         "Mouse",
         2,
         15.5
        ],
        [
         6,
         "2025-01-05",
         "Altavoces",
         13,
         45.0
        ],
        [
         7,
         "2025-01-08",
         "Teclado",
         11,
         25.99
        ],
        [
         8,
         "2025-01-10",
         "Auriculares",
         5,
         29.99
        ],
        [
         9,
         "2025-01-11",
         "Monitor",
         2,
         199.99
        ],
        [
         10,
         "2025-01-12",
         "Microfono",
         20,
         59.99
        ],
        [
         11,
         "2025-01-12",
         "USB",
         8,
         9.99
        ],
        [
         12,
         "2025-01-12",
         "Webcam",
         16,
         39.99
        ],
        [
         13,
         "2025-01-13",
         "USB",
         6,
         9.99
        ],
        [
         14,
         "2025-01-14",
         "Altavoces",
         20,
         45.0
        ],
        [
         15,
         "2025-01-15",
         "Auriculares",
         17,
         29.99
        ],
        [
         16,
         "2025-01-18",
         "USB",
         16,
         9.99
        ],
        [
         17,
         "2025-01-18",
         "Impresora",
         13,
         89.9
        ],
        [
         18,
         "2025-01-19",
         "USB",
         12,
         9.99
        ],
        [
         19,
         "2025-01-24",
         "Mouse",
         8,
         15.5
        ],
        [
         20,
         "2025-01-26",
         "Webcam",
         7,
         39.99
        ],
        [
         21,
         "2025-01-27",
         "Altavoces",
         4,
         45.0
        ],
        [
         22,
         "2025-01-27",
         "Disco Duro",
         12,
         79.99
        ],
        [
         23,
         "2025-01-28",
         "Impresora",
         10,
         89.9
        ],
        [
         24,
         "2025-01-28",
         "Webcam",
         15,
         39.99
        ],
        [
         25,
         "2025-01-31",
         "Webcam",
         8,
         39.99
        ],
        [
         26,
         "2025-01-31",
         "Auriculares",
         18,
         29.99
        ],
        [
         27,
         "2025-02-01",
         "Disco Duro",
         14,
         79.99
        ],
        [
         28,
         "2025-02-01",
         "USB",
         9,
         9.99
        ],
        [
         29,
         "2025-02-01",
         "Altavoces",
         20,
         45.0
        ],
        [
         30,
         "2025-02-02",
         "Teclado",
         17,
         25.99
        ],
        [
         31,
         "2025-02-02",
         "Mouse",
         13,
         15.5
        ],
        [
         32,
         "2025-02-06",
         "Altavoces",
         7,
         45.0
        ],
        [
         33,
         "2025-02-07",
         "USB",
         14,
         9.99
        ],
        [
         34,
         "2025-02-08",
         "Teclado",
         14,
         25.99
        ],
        [
         35,
         "2025-02-09",
         "Impresora",
         17,
         89.9
        ],
        [
         36,
         "2025-02-09",
         "Monitor",
         12,
         199.99
        ],
        [
         37,
         "2025-02-09",
         "Altavoces",
         20,
         45.0
        ],
        [
         38,
         "2025-02-10",
         "Monitor",
         8,
         199.99
        ],
        [
         39,
         "2025-02-12",
         "Impresora",
         19,
         89.9
        ],
        [
         40,
         "2025-02-12",
         "Webcam",
         19,
         39.99
        ],
        [
         41,
         "2025-02-12",
         "Mouse",
         5,
         15.5
        ],
        [
         42,
         "2025-02-12",
         "Disco Duro",
         10,
         79.99
        ],
        [
         43,
         "2025-02-13",
         "Microfono",
         5,
         59.99
        ],
        [
         44,
         "2025-02-15",
         "Webcam",
         18,
         39.99
        ],
        [
         45,
         "2025-02-15",
         "Teclado",
         2,
         25.99
        ],
        [
         46,
         "2025-02-16",
         "Auriculares",
         6,
         29.99
        ],
        [
         47,
         "2025-02-18",
         "USB",
         6,
         9.99
        ],
        [
         48,
         "2025-02-18",
         "Mouse",
         2,
         15.5
        ],
        [
         49,
         "2025-02-19",
         "Impresora",
         3,
         89.9
        ],
        [
         50,
         "2025-02-20",
         "Microfono",
         11,
         59.99
        ],
        [
         51,
         "2025-02-23",
         "Auriculares",
         10,
         29.99
        ],
        [
         52,
         "2025-02-25",
         "Auriculares",
         8,
         29.99
        ],
        [
         53,
         "2025-02-26",
         "Microfono",
         15,
         59.99
        ],
        [
         54,
         "2025-02-26",
         "Altavoces",
         10,
         45.0
        ],
        [
         55,
         "2025-02-27",
         "Teclado",
         5,
         25.99
        ],
        [
         56,
         "2025-02-28",
         "Teclado",
         18,
         25.99
        ],
        [
         57,
         "2025-02-28",
         "Microfono",
         12,
         59.99
        ],
        [
         58,
         "2025-02-28",
         "Disco Duro",
         18,
         79.99
        ],
        [
         59,
         "2025-03-03",
         "Teclado",
         18,
         25.99
        ],
        [
         60,
         "2025-03-03",
         "Monitor",
         16,
         199.99
        ],
        [
         61,
         "2025-03-05",
         "Impresora",
         2,
         89.9
        ],
        [
         62,
         "2025-03-06",
         "Microfono",
         8,
         59.99
        ],
        [
         63,
         "2025-03-07",
         "Mouse",
         18,
         15.5
        ],
        [
         64,
         "2025-03-08",
         "Microfono",
         2,
         59.99
        ],
        [
         65,
         "2025-03-10",
         "Teclado",
         2,
         25.99
        ],
        [
         66,
         "2025-03-10",
         "Disco Duro",
         19,
         79.99
        ],
        [
         67,
         "2025-03-12",
         "Mouse",
         5,
         15.5
        ],
        [
         68,
         "2025-03-12",
         "Impresora",
         3,
         89.9
        ],
        [
         69,
         "2025-03-12",
         "Impresora",
         1,
         89.9
        ],
        [
         70,
         "2025-03-13",
         "Auriculares",
         10,
         29.99
        ],
        [
         71,
         "2025-03-14",
         "Webcam",
         12,
         39.99
        ],
        [
         72,
         "2025-03-15",
         "Monitor",
         12,
         199.99
        ],
        [
         73,
         "2025-03-20",
         "Altavoces",
         20,
         45.0
        ],
        [
         74,
         "2025-03-20",
         "Microfono",
         13,
         59.99
        ],
        [
         75,
         "2025-03-21",
         "USB",
         6,
         9.99
        ],
        [
         76,
         "2025-03-21",
         "USB",
         3,
         9.99
        ],
        [
         77,
         "2025-03-22",
         "Monitor",
         3,
         199.99
        ],
        [
         78,
         "2025-03-23",
         "USB",
         5,
         9.99
        ],
        [
         79,
         "2025-03-23",
         "Altavoces",
         16,
         45.0
        ],
        [
         80,
         "2025-03-24",
         "Webcam",
         12,
         39.99
        ],
        [
         81,
         "2025-03-26",
         "Webcam",
         6,
         39.99
        ],
        [
         82,
         "2025-03-26",
         "Altavoces",
         3,
         45.0
        ],
        [
         83,
         "2025-03-26",
         "Teclado",
         13,
         25.99
        ],
        [
         84,
         "2025-03-27",
         "Microfono",
         6,
         59.99
        ],
        [
         85,
         "2025-03-28",
         "Disco Duro",
         4,
         79.99
        ],
        [
         86,
         "2025-03-28",
         "Impresora",
         12,
         89.9
        ],
        [
         87,
         "2025-03-29",
         "Mouse",
         4,
         15.5
        ],
        [
         88,
         "2025-03-29",
         "Altavoces",
         4,
         45.0
        ],
        [
         89,
         "2025-03-30",
         "Microfono",
         1,
         59.99
        ],
        [
         90,
         "2025-03-30",
         "USB",
         2,
         9.99
        ],
        [
         91,
         "2025-04-01",
         "Impresora",
         5,
         89.9
        ],
        [
         92,
         "2025-04-02",
         "Monitor",
         4,
         199.99
        ],
        [
         93,
         "2025-04-02",
         "Webcam",
         9,
         39.99
        ],
        [
         94,
         "2025-04-02",
         "Webcam",
         4,
         39.99
        ],
        [
         95,
         "2025-04-03",
         "USB",
         12,
         9.99
        ],
        [
         96,
         "2025-04-03",
         "Mouse",
         5,
         15.5
        ],
        [
         97,
         "2025-04-05",
         "Monitor",
         7,
         199.99
        ],
        [
         98,
         "2025-04-06",
         "Mouse",
         10,
         15.5
        ],
        [
         99,
         "2025-04-06",
         "Teclado",
         10,
         25.99
        ],
        [
         100,
         "2025-04-06",
         "Mouse",
         16,
         15.5
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "fecha",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "producto",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "cantidad",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "precio",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "path_file = \"/Volumes/analitica/data/landing/ventas_2025.csv\"\n",
    "\n",
    "\n",
    "\n",
    "df_ventas = spark.read.csv(\n",
    "    path=path_file,\n",
    "    header=True,\n",
    "    schema= \"id INT, fecha DATE, producto STRING, cantidad INT, precio DOUBLE\",\n",
    "    mode=\"FAILFAST\"\n",
    ")\n",
    "\n",
    "display(df_ventas)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "d1e75042-7f7b-4961-8e65-e0e615001eca",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Verifica el esquema y la cantidad de ventas, identifica IDs duplicados y detecta registros inválidos por valores nulos, precios no numéricos, cantidades no positivas o precios negativos. Si encuentra registros inválidos, detiene la ejecución con un error."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270519322,
     "inputWidgets": {},
     "nuid": "725ae7df-3115-4c51-876d-e0d5691b0f0a",
     "showTitle": false,
     "startTime": 1792270517243,
     "submitTime": 1792270479420,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n |-- id: integer (nullable = true)\n |-- fecha: date (nullable = true)\n |-- producto: string (nullable = true)\n |-- cantidad: integer (nullable = true)\n |-- precio: double (nullable = true)\n\nRegistros de venta: 100\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>count</th></tr></thead><tbody></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "count",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>fecha</th><th>producto</th><th>cantidad</th><th>precio</th></tr></thead><tbody></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "fecha",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "producto",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "cantidad",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "precio",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "df_ventas.printSchema()\n",
    "print(\"Registros de venta:\", df_ventas.count())\n",
    "\n",
    "display(df_ventas.groupBy(\"id\").count().filter(\"count > 1\"))\n",
    "\n",
    "df_invalidos = df_ventas.filter(\n",
    "    F.col(\"id\").isNull() | F.col(\"fecha\").isNull() |\n",
    "    F.col(\"producto\").isNull() | F.col(\"cantidad\").isNull() |\n",
    "    F.col(\"precio\").isNull() | F.isnan(\"precio\") |\n",
    "    (F.col(\"cantidad\") <= 0) | (F.col(\"precio\") < 0)\n",
    ")\n",
    "display(df_invalidos)\n",
    "assert df_invalidos.count() == 0, \"Revisar los registros antes de continuar.\""
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "2e3d9e14-8832-4663-b718-aa2824cf4381",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Guarda el DataFrame df_ventas como una tabla Delta llamada ventas, sobrescribiendo los datos existentes si la tabla ya existe."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270521626,
     "inputWidgets": {},
     "nuid": "029abbe1-8ddc-4a7e-9bc5-164237d7ec15",
     "showTitle": false,
     "startTime": 1792270519347,
     "submitTime": 1792270479426,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "df_ventas.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"ventas\")\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b2c80b09-ae41-48d0-bbbd-17ec13222c65",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Muestra los detalles técnicos de la tabla ventas, como su formato, ubicación de almacenamiento, tamaño, número de archivos y propiedades de Delta."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270522632,
     "inputWidgets": {},
     "nuid": "76dc6abc-cb5b-4c65-951d-0b8b07622a29",
     "showTitle": false,
     "startTime": 1792270521648,
     "submitTime": 1792270479430,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>format</th><th>id</th><th>name</th><th>description</th><th>location</th><th>createdAt</th><th>lastModified</th><th>partitionColumns</th><th>clusteringColumns</th><th>numFiles</th><th>sizeInBytes</th><th>properties</th><th>minReaderVersion</th><th>minWriterVersion</th><th>tableFeatures</th><th>statistics</th><th>clusterByAuto</th></tr></thead><tbody><tr><td>delta</td><td>170135fc-926d-4830-9b9c-b3281b498c9d</td><td>analitica.data.ventas</td><td>null</td><td></td><td>2026-10-17T20:20:21.411Z</td><td>2026-10-17T20:55:22.000Z</td><td>List()</td><td>List()</td><td>1</td><td>2392</td><td>Map(delta.parquet.compression.codec -> zstd, delta.parquet.format.version.afe.internal -> 2.12.0, delta.enableDeletionVectors -> true, delta.parquet.format.version -> 2.12.0, io.unitycatalog.tableId -> ff9ba7c2-6a42-4d74-8201-2feab1b8d499)</td><td>3</td><td>7</td><td>List(appendOnly, deletionVectors, invariants)</td><td>Map(numRowsDeletedByDeletionVectors -> 0, numDeletionVectors -> 0)</td><td>false</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "delta",
         "170135fc-926d-4830-9b9c-b3281b498c9d",
         "analitica.data.ventas",
         null,
         "",
         "2026-10-17T20:20:21.411Z",
         "2026-10-17T20:55:22.000Z",
         [],
         [],
         1,
         2392,
         {
          "delta.enableDeletionVectors": "true",
          "delta.parquet.compression.codec": "zstd",
          "delta.parquet.format.version": "2.12.0",
          "delta.parquet.format.version.afe.internal": "2.12.0",
          "io.unitycatalog.tableId": "ff9ba7c2-6a42-4d74-8201-2feab1b8d499"
         },
         3,
         7,
         [
          "appendOnly",
          "deletionVectors",
          "invariants"
         ],
         {
          "numDeletionVectors": 0,
          "numRowsDeletedByDeletionVectors": 0
         },
         false
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 30
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "format",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "description",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "location",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "createdAt",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "lastModified",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "partitionColumns",
         "type": "{\"containsNull\":true,\"elementType\":\"string\",\"type\":\"array\"}"
        },
        {
         "metadata": "{}",
         "name": "clusteringColumns",
         "type": "{\"containsNull\":true,\"elementType\":\"string\",\"type\":\"array\"}"
        },
        {
         "metadata": "{}",
         "name": "numFiles",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "sizeInBytes",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "properties",
         "type": "{\"keyType\":\"string\",\"type\":\"map\",\"valueContainsNull\":true,\"valueType\":\"string\"}"
        },
        {
         "metadata": "{}",
         "name": "minReaderVersion",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "minWriterVersion",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "tableFeatures",
         "type": "{\"containsNull\":true,\"elementType\":\"string\",\"type\":\"array\"}"
        },
        {
         "metadata": "{}",
         "name": "statistics",
         "type": "{\"keyType\":\"string\",\"type\":\"map\",\"valueContainsNull\":true,\"valueType\":\"long\"}"
        },
        {
         "metadata": "{}",
         "name": "clusterByAuto",
         "type": "\"boolean\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "DESCRIBE DETAIL ventas;"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "8a6376b4-0634-48d5-ad0c-c57137d1d3b8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Agrupa las ventas por producto, calcula las unidades vendidas y el importe total redondeado a dos decimales, y ordena los productos de mayor a menor importe total."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270524148,
     "inputWidgets": {},
     "nuid": "eacb3866-b7bb-4e34-b99d-edc994a44a19",
     "showTitle": false,
     "startTime": 1792270522662,
     "submitTime": 1792270479436,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>producto</th><th>unidades</th><th>importe_total</th></tr></thead><tbody><tr><td>Monitor</td><td>64</td><td>12799.36</td></tr><tr><td>Impresora</td><td>85</td><td>7641.5</td></tr><tr><td>Altavoces</td><td>137</td><td>6165.0</td></tr><tr><td>Disco Duro</td><td>77</td><td>6159.23</td></tr><tr><td>Microfono</td><td>93</td><td>5579.07</td></tr><tr><td>Webcam</td><td>137</td><td>5478.63</td></tr><tr><td>Teclado</td><td>130</td><td>3378.7</td></tr><tr><td>Auriculares</td><td>92</td><td>2759.08</td></tr><tr><td>Mouse</td><td>88</td><td>1364.0</td></tr><tr><td>USB</td><td>99</td><td>989.01</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Monitor",
         64,
         12799.36
        ],
        [
         "Impresora",
         85,
         7641.5
        ],
        [
         "Altavoces",
         137,
         6165.0
        ],
        [
         "Disco Duro",
         77,
         6159.23
        ],
        [
         "Microfono",
         93,
         5579.07
        ],
        [
         "Webcam",
         137,
         5478.63
        ],
        [
         "Teclado",
         130,
         3378.7
        ],
        [
         "Auriculares",
         92,
         2759.08
        ],
        [
         "Mouse",
         88,
         1364.0
        ],
        [
         "USB",
         99,
         989.01
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 31
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "producto",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "unidades",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "importe_total",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "text/plain": [
       "Databricks visualization. Run in Databricks to view."
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1.subcommand+json": {
       "baseErrorDetails": null,
       "bindings": {},
       "collapsed": false,
       "command": "%sql WITH q AS (SELECT producto, \nsum(cantidad) as unidades,\nround(SUM(cantidad * precio),2) AS importe_total\nFROM ventas\nGROUP BY producto\nORDER BY importe_total DESC) SELECT `producto`,SUM(`importe_total`) `column_eec8cd0958`,`producto` FROM q GROUP BY `producto`",
       "commandTitle": "Visualization 1",
       "commandType": "auto",
       "commandVersion": 0,
       "commentThread": [],
       "commentsVisible": false,
       "contentSha256Hex": null,
       "customPlotOptions": {
        "redashChart": [
         {
          "key": "type",
          "value": "CHART"
         },
         {
          "key": "options",
          "value": {
           "alignYAxesAtZero": true,
           "coefficient": 1,
           "columnConfigurationMap": {
            "series": {
             "column": "producto",
             "id": "column_eec8cd0959"
            },
            "x": {
             "column": "producto",
             "id": "column_eec8cd0961"
            },
            "y": [
             {
              "column": "importe_total",
              "id": "column_eec8cd0958",
              "transform": "SUM"
             }
            ]
           },
           "dateTimeFormat": "DD/MM/YYYY HH:mm",
           "direction": {
            "type": "counterclockwise"
           },
           "error_y": {
            "type": "data",
            "visible": true
           },
           "globalSeriesType": "column",
           "isAggregationOn": true,
           "legend": {
            "traceorder": "normal"
           },
           "missingValuesAsZero": true,
           "numberFormat": "0,0.[00000]",
           "percentFormat": "0[.]00%",
           "series": {
            "error_y": {
             "type": "data",
             "visible": true
            },
            "stacking": null
           },
           "seriesOptions": {
            "column_eec8cd0958": {
             "name": "importe_total",
             "type": "column",
             "yAxis": 0
            }
           },
           "showDataLabels": false,
           "sizemode": "diameter",
           "sortX": true,
           "sortY": true,
           "swappedAxes": false,
           "textFormat": "",
           "useAggregationsUi": true,
           "valuesOptions": {},
           "version": 2,
           "xAxis": {
            "labels": {
             "enabled": true
            },
            "type": "-"
           },
           "yAxis": [
            {
             "type": "-"
            },
            {
             "opposite": true,
             "type": "-"
            }
           ]
          }
         }
        ]
       },
       "datasetPreviewNameToCmdIdMap": {},
       "diffDeletes": [],
       "diffInserts": [],
       "displayType": "redashChart",
       "error": null,
       "errorDetails": null,
       "errorSummary": null,
       "errorTraceType": null,
       "finishTime": 0,
       "globalVars": {},
       "guid": "",
       "height": "auto",
       "hideCommandCode": false,
       "hideCommandResult": false,
       "iPythonMetadata": null,
       "inputWidgets": {},
       "isLockedInExamMode": false,
       "latestAssumeRoleInfo": null,
       "latestUser": "a user",
       "latestUserId": null,
       "listResultMetadata": null,
       "metadata": {
        "byteLimit": 2048000,
        "implicitDf": true,
        "rowLimit": 10000
       },
       "nuid": "e0cca491-bda7-4a25-af4b-c610bf92d47f",
       "origId": 0,
       "parentHierarchy": [],
       "pivotAggregation": null,
       "pivotColumns": null,
       "position": 31.0,
       "resultDbfsErrorMessage": null,
       "resultDbfsStatus": "INLINED_IN_TREE",
       "results": null,
       "showCommandTitle": false,
       "startTime": 0,
       "state": "finished",
       "streamStates": {},
       "subcommandOptions": {
        "queryPlan": {
         "groups": [
          {
           "column": "producto",
           "type": "column"
          },
          {
           "column": "producto",
           "type": "column"
          }
         ],
         "selects": [
          {
           "column": "producto",
           "type": "column"
          },
          {
           "alias": "column_eec8cd0958",
           "args": [
            {
             "column": "importe_total",
             "type": "column"
            }
           ],
           "function": "SUM",
           "type": "function"
          },
          {
           "column": "producto",
           "type": "column"
          }
         ]
        }
       },
       "submitTime": 0,
       "subtype": "tableResultSubCmd.visualization",
       "tableResultIndex": 0,
       "tableResultSettingsMap": {},
       "useConsistentColors": false,
       "version": "CommandV1",
       "width": "auto",
       "workflows": [],
       "xColumns": null,
       "yColumns": null
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT producto, \n",
    "sum(cantidad) as unidades,\n",
    "round(SUM(cantidad * precio),2) AS importe_total\n",
    "FROM ventas\n",
    "GROUP BY producto\n",
    "ORDER BY importe_total DESC"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "c8edb395-b4f3-42fb-9312-177a59dcae0e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Muestra el historial de cambios de la tabla Delta ventas, incluyendo versiones, fechas, operaciones realizadas y usuarios responsables cuando esa información está disponible."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270526642,
     "inputWidgets": {},
     "nuid": "ca32457b-56cc-41fd-b1a1-52b3de0306b5",
     "showTitle": false,
     "startTime": 1792270524183,
     "submitTime": 1792270479441,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>version</th><th>timestamp</th><th>userId</th><th>userName</th><th>operation</th><th>operationParameters</th><th>job</th><th>notebook</th><th>queryHistoryStatementId</th><th>clusterId</th><th>readVersion</th><th>isolationLevel</th><th>isBlindAppend</th><th>operationMetrics</th><th>userMetadata</th><th>engineInfo</th></tr></thead><tbody><tr><td>1</td><td>2026-10-17T20:55:22.000Z</td><td>77538316313781</td><td>andressabogal315@gmail.com</td><td>CREATE OR REPLACE TABLE AS SELECT</td><td>Map(isV1SaveAsTableOverwrite -> true, partitionBy -> [], clusterBy -> [], description -> null, isManaged -> true, properties -> {\"delta.parquet.compression.codec\":\"zstd\",\"delta.parquet.format.version.afe.internal\":\"2.12.0\",\"delta.enableDeletionVectors\":\"true\",\"delta.parquet.format.version\":\"2.12.0\",\"io.unitycatalog.tableId\":\"ff9ba7c2-6a42-4d74-8201-2feab1b8d499\"}, statsOnLoad -> true)</td><td>null</td><td>List(3412372199736854)</td><td>fba06089-a31e-41e8-a53b-62e6655da839</td><td>1017-190227-syaso9ee-v2n</td><td>0</td><td>WriteSerializable</td><td>false</td><td>Map(numFiles -> 1, numRemovedFiles -> 1, numRemovedBytes -> 2392, numDeletionVectorsRemoved -> 0, numOutputRows -> 100, numOutputBytes -> 2392)</td><td>null</td><td>Databricks-Runtime/19.9.x-aarch64-photon-scala2.13</td></tr><tr><td>0</td><td>2026-10-17T20:20:23.000Z</td><td>77538316313781</td><td>andressabogal315@gmail.com</td><td>CREATE OR REPLACE TABLE AS SELECT</td><td>Map(isV1SaveAsTableOverwrite -> true, partitionBy -> [], clusterBy -> [], description -> null, isManaged -> true, properties -> {\"delta.parquet.compression.codec\":\"zstd\",\"delta.parquet.format.version.afe.internal\":\"2.12.0\",\"delta.enableDeletionVectors\":\"true\",\"delta.parquet.format.version\":\"2.12.0\",\"io.unitycatalog.tableId\":\"ff9ba7c2-6a42-4d74-8201-2feab1b8d499\"}, statsOnLoad -> true)</td><td>null</td><td>List(3412372199736854)</td><td>92e6eb9a-97f7-4f00-81da-09c64d9be088</td><td>1017-190227-syaso9ee-v2n</td><td>null</td><td>WriteSerializable</td><td>false</td><td>Map(numFiles -> 1, numRemovedFiles -> 0, numRemovedBytes -> 0, numDeletionVectorsRemoved -> 0, numOutputRows -> 100, numOutputBytes -> 2392)</td><td>null</td><td>Databricks-Runtime/19.9.x-aarch64-photon-scala2.13</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         "2026-10-17T20:55:22.000Z",
         "77538316313781",
         "andressabogal315@gmail.com",
         "CREATE OR REPLACE TABLE AS SELECT",
         {
          "clusterBy": "[]",
          "description": null,
          "isManaged": "true",
          "isV1SaveAsTableOverwrite": "true",
          "partitionBy": "[]",
          "properties": "{\"delta.parquet.compression.codec\":\"zstd\",\"delta.parquet.format.version.afe.internal\":\"2.12.0\",\"delta.enableDeletionVectors\":\"true\",\"delta.parquet.format.version\":\"2.12.0\",\"io.unitycatalog.tableId\":\"ff9ba7c2-6a42-4d74-8201-2feab1b8d499\"}",
          "statsOnLoad": "true"
         },
         null,
         [
          "3412372199736854"
         ],
         "fba06089-a31e-41e8-a53b-62e6655da839",
         "1017-190227-syaso9ee-v2n",
         0,
         "WriteSerializable",
         false,
         {
          "numDeletionVectorsRemoved": "0",
          "numFiles": "1",
          "numOutputBytes": "2392",
          "numOutputRows": "100",
          "numRemovedBytes": "2392",
          "numRemovedFiles": "1"
         },
         null,
         "Databricks-Runtime/19.9.x-aarch64-photon-scala2.13"
        ],
        [
         0,
         "2026-10-17T20:20:23.000Z",
         "77538316313781",
         "andressabogal315@gmail.com",
         "CREATE OR REPLACE TABLE AS SELECT",
         {
          "clusterBy": "[]",
          "description": null,
          "isManaged": "true",
          "isV1SaveAsTableOverwrite": "true",
          "partitionBy": "[]",
          "properties": "{\"delta.parquet.compression.codec\":\"zstd\",\"delta.parquet.format.version.afe.internal\":\"2.12.0\",\"delta.enableDeletionVectors\":\"true\",\"delta.parquet.format.version\":\"2.12.0\",\"io.unitycatalog.tableId\":\"ff9ba7c2-6a42-4d74-8201-2feab1b8d499\"}",
          "statsOnLoad": "true"
         },
         null,
         [
          "3412372199736854"
         ],
         "92e6eb9a-97f7-4f00-81da-09c64d9be088",
         "1017-190227-syaso9ee-v2n",
         null,
         "WriteSerializable",
         false,
         {
          "numDeletionVectorsRemoved": "0",
          "numFiles": "1",
          "numOutputBytes": "2392",
          "numOutputRows": "100",
          "numRemovedBytes": "0",
          "numRemovedFiles": "0"
         },
         null,
         "Databricks-Runtime/19.9.x-aarch64-photon-scala2.13"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 32
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "version",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "userId",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "userName",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "operation",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "operationParameters",
         "type": "{\"keyType\":\"string\",\"type\":\"map\",\"valueContainsNull\":true,\"valueType\":\"string\"}"
        },
        {
         "metadata": "{}",
         "name": "job",
         "type": "{\"fields\":[{\"metadata\":{},\"name\":\"jobId\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"jobName\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"jobRunId\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"runId\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"jobOwnerId\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"triggerType\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"workspaceId\",\"nullable\":true,\"type\":\"string\"}],\"type\":\"struct\"}"
        },
        {
         "metadata": "{}",
         "name": "notebook",
         "type": "{\"fields\":[{\"metadata\":{},\"name\":\"notebookId\",\"nullable\":true,\"type\":\"string\"}],\"type\":\"struct\"}"
        },
        {
         "metadata": "{}",
         "name": "queryHistoryStatementId",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "clusterId",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "readVersion",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "isolationLevel",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "isBlindAppend",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "operationMetrics",
         "type": "{\"keyType\":\"string\",\"type\":\"map\",\"valueContainsNull\":true,\"valueType\":\"string\"}"
        },
        {
         "metadata": "{}",
         "name": "userMetadata",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "engineInfo",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "DESCRIBE HISTORY ventas;"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "abe8c203-5546-499d-841c-6a6ffa230afa",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Define Spain como país seleccionado y muestra del DataFrame df_enriquecido únicamente los registros correspondientes a ese país."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270527641,
     "inputWidgets": {},
     "nuid": "8ee1c711-10a1-4c79-8a64-9700db9271c0",
     "showTitle": false,
     "startTime": 1792270526662,
     "submitTime": 1792270479470,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>product</th><th>order_id</th><th>order_date</th><th>country</th><th>category</th><th>units_sold</th><th>unit_price</th><th>importe</th><th>product_family</th><th>supplier</th></tr></thead><tbody><tr><td>Laptop</td><td>1</td><td>2024-01-02</td><td>Spain</td><td>Electronics</td><td>2</td><td>1200</td><td>2400</td><td>Computers</td><td>TechCorp</td></tr><tr><td>Mouse</td><td>2</td><td>2024-01-03</td><td>Spain</td><td>Accessories</td><td>5</td><td>25</td><td>125</td><td>Peripherals</td><td>InputMakers</td></tr><tr><td>Monitor</td><td>5</td><td>2024-01-07</td><td>Spain</td><td>Electronics</td><td>2</td><td>300</td><td>600</td><td>Displays</td><td>ScreenWorld</td></tr><tr><td>Keyboard</td><td>8</td><td>2024-01-11</td><td>Spain</td><td>Accessories</td><td>4</td><td>42</td><td>168</td><td>Peripherals</td><td>InputMakers</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Laptop",
         1,
         "2024-01-02",
         "Spain",
         "Electronics",
         2,
         1200,
         2400,
         "Computers",
         "TechCorp"
        ],
        [
         "Mouse",
         2,
         "2024-01-03",
         "Spain",
         "Accessories",
         5,
         25,
         125,
         "Peripherals",
         "InputMakers"
        ],
        [
         "Monitor",
         5,
         "2024-01-07",
         "Spain",
         "Electronics",
         2,
         300,
         600,
         "Displays",
         "ScreenWorld"
        ],
        [
         "Keyboard",
         8,
         "2024-01-11",
         "Spain",
         "Accessories",
         4,
         42,
         168,
         "Peripherals",
         "InputMakers"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "product",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "order_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{\"__detected_date_formats\": \"yyyy-M-d\"}",
         "name": "order_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "country",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "units_sold",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "unit_price",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "importe",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "product_family",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "supplier",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "pais_elegido = \"Spain\"\n",
    "\n",
    "display(df_enriquecido.filter(F.col(\"country\") == pais_elegido))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "f04e4eb7-74a7-47a5-9fac-c870f3162499",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Crea o reemplaza la tabla Delta ventas_delta_demo copiando todos los registros y columnas de la tabla ventas."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270536967,
     "inputWidgets": {},
     "nuid": "670fdf41-b6aa-4a76-a933-523ef2e58099",
     "showTitle": false,
     "startTime": 1792270532624,
     "submitTime": 1792270532589,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>num_affected_rows</th><th>num_inserted_rows</th></tr></thead><tbody></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 34
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "num_affected_rows",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "num_inserted_rows",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE TABLE ventas_delta_demo USING DELTA AS SELECT * FROM ventas\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "59735b70-d903-43ff-8920-8c0b8db3a44a",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Consulta el historial de ventas_delta_demo, obtiene el número de su versión más reciente y lo guarda en version_inicial como referencia para comparar cambios posteriores."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270544166,
     "inputWidgets": {},
     "nuid": "105dbb38-8180-4f93-b952-d7b9a2fc676a",
     "showTitle": false,
     "startTime": 1792270542586,
     "submitTime": 1792270542547,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Versión de referencia: 0\n"
     ]
    }
   ],
   "source": [
    "historial = spark.sql(\"DESCRIBE HISTORY ventas_delta_demo\")\n",
    "version_inicial = historial.orderBy(F.desc(\"version\")).first()[\"version\"]\n",
    "\n",
    "print(\"Versión de referencia:\", version_inicial)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "ae969988-5b89-4759-983c-2eedbc0df8fe",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Actualiza la tabla ventas_delta_demo, aumentando en 1 el precio del registro cuyo id es 1 y redondeándolo a dos decimales."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270559089,
     "inputWidgets": {},
     "nuid": "88d4a0d0-4ae7-44b8-97c3-9a11887f18f8",
     "showTitle": false,
     "startTime": 1792270553756,
     "submitTime": 1792270553713,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>num_affected_rows</th></tr></thead><tbody><tr><td>1</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 36
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "num_affected_rows",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "UPDATE ventas_delta_demo SET precio = ROUND(precio + 1, 2) WHERE id = 1"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "d8838b95-9d9d-4a4b-af48-ddbfaa50185e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Consulta y muestra todos los datos del registro con id = 1 en ventas_delta_demo para verificar el precio actualizado."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270567995,
     "inputWidgets": {},
     "nuid": "347ac2e4-6355-4563-9a6e-f0f8cb0327d0",
     "showTitle": false,
     "startTime": 1792270566607,
     "submitTime": 1792270566567,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>fecha</th><th>producto</th><th>cantidad</th><th>precio</th></tr></thead><tbody><tr><td>1</td><td>2025-01-03</td><td>Teclado</td><td>20</td><td>26.99</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         "2025-01-03",
         "Teclado",
         20,
         26.99
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 37
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "fecha",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "producto",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "cantidad",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "precio",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT * FROM ventas_delta_demo WHERE id = 1"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "4d919037-e9e4-4dd7-82b1-0aee41c87d41",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Inserta un nuevo registro en ventas_delta_demo con ID 1001, fecha 2025-01-31, producto Producto demo, cantidad 2 y precio 10.0."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270579458,
     "inputWidgets": {},
     "nuid": "ff32be64-30a0-4b1b-a257-33f5862cc1f1",
     "showTitle": false,
     "startTime": 1792270577541,
     "submitTime": 1792270577502,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>num_affected_rows</th><th>num_inserted_rows</th></tr></thead><tbody><tr><td>1</td><td>1</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         1
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 38
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "num_affected_rows",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "num_inserted_rows",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "\n",
    "INSERT INTO ventas_delta_demo VALUES (1001, DATE '2025-01-31', 'Producto demo', 2, 10.0)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "e1e07508-4eea-431a-9d6d-2aebbd3125da",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Cuenta todos los registros de la tabla ventas_delta_demo y muestra el total con el nombre registros."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270587770,
     "inputWidgets": {},
     "nuid": "bb83254f-7215-4d16-afdc-6821adedb554",
     "showTitle": false,
     "startTime": 1792270585904,
     "submitTime": 1792270585835,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>registros</th></tr></thead><tbody><tr><td>101</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         101
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 39
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "registros",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT COUNT(*) AS registros FROM ventas_delta_demo"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "96da8092-0cf8-4a6c-a1d5-f0b58ef19cba",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Elimina de la tabla ventas_delta_demo el registro cuyo id es 1001."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270614516,
     "inputWidgets": {},
     "nuid": "619c2baf-e214-4e8b-8325-7229a0d02f87",
     "showTitle": false,
     "startTime": 1792270611759,
     "submitTime": 1792270611723,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>num_affected_rows</th></tr></thead><tbody><tr><td>1</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 41
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "num_affected_rows",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql \n",
    "DELETE FROM ventas_delta_demo WHERE id = 1001"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "4f2674a9-2ea1-4f1b-8ba8-89c5ce6b3d96",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Cuenta los registros actuales de ventas_delta_demo y muestra el total para comprobar que el registro eliminado ya no está en la tabla."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270627929,
     "inputWidgets": {},
     "nuid": "5da5931f-ec84-4876-bafd-74e47d8b18e6",
     "showTitle": false,
     "startTime": 1792270626512,
     "submitTime": 1792270626473,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>registros</th></tr></thead><tbody><tr><td>100</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         100
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 42
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "registros",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT COUNT(*) AS registros FROM ventas_delta_demo"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "9b661bbc-8016-47c5-ab6c-ef2d649b19c8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Crea o reemplaza la vista temporal cambios_ventas con dos registros de ejemplo, incluyendo sus IDs, fechas, productos, cantidades y precios, para utilizarlos en operaciones posteriores."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270637640,
     "inputWidgets": {},
     "nuid": "c6ad7921-84d1-46de-a9a8-00e8b39ba5f6",
     "showTitle": false,
     "startTime": 1792270637276,
     "submitTime": 1792270637228,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.databricks.empty-table+json": {
       "directive_name": "CreateView"
      }
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE TEMP VIEW cambios_ventas AS\n",
    "SELECT * FROM VALUES\n",
    "  (1, DATE '2025-01-03', 'Teclado', 20, CAST(27.99 AS DOUBLE)),\n",
    "  (1001, DATE '2025-01-31', 'Producto demo', 2, CAST(10.0 AS DOUBLE))\n",
    "AS cambios(id, fecha, producto, cantidad, precio)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "8ddbe6c9-8108-45e6-8383-6f10aa938388",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Muestra todos los registros de la vista temporal cambios_ventas para consultar los dos registros de ejemplo creados."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270648161,
     "inputWidgets": {},
     "nuid": "4381ff05-18a3-48d1-8510-45e4e838816c",
     "showTitle": false,
     "startTime": 1792270647326,
     "submitTime": 1792270647280,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>fecha</th><th>producto</th><th>cantidad</th><th>precio</th></tr></thead><tbody><tr><td>1</td><td>2025-01-03</td><td>Teclado</td><td>20</td><td>27.99</td></tr><tr><td>1001</td><td>2025-01-31</td><td>Producto demo</td><td>2</td><td>10.0</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         "2025-01-03",
         "Teclado",
         20,
         27.99
        ],
        [
         1001,
         "2025-01-31",
         "Producto demo",
         2,
         10.0
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 44
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "fecha",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "producto",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "cantidad",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "precio",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT * FROM cambios_ventas"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b2cedf26-f241-4044-9588-c599a44f7d13",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Sincroniza ventas_delta_demo con cambios_ventas: actualiza los registros cuyo id coincide e inserta los registros nuevos cuyo id no existe en la tabla destino."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270689403,
     "inputWidgets": {},
     "nuid": "538c3824-d191-4744-aeb6-d53bdf2f7623",
     "showTitle": false,
     "startTime": 1792270683883,
     "submitTime": 1792270683845,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>num_affected_rows</th><th>num_updated_rows</th><th>num_deleted_rows</th><th>num_inserted_rows</th></tr></thead><tbody><tr><td>2</td><td>1</td><td>0</td><td>1</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         2,
         1,
         0,
         1
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 45
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "num_affected_rows",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "num_updated_rows",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "num_deleted_rows",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "num_inserted_rows",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "MERGE INTO ventas_delta_demo AS destino\n",
    "USING cambios_ventas AS fuente\n",
    "ON destino.id = fuente.id\n",
    "WHEN MATCHED THEN UPDATE SET *\n",
    "WHEN NOT MATCHED THEN INSERT *"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "d442dd4e-c08c-4691-96f9-b677d8227f97",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Consulta los registros con ID 1 y 1001 de ventas_delta_demo y los ordena por ID para verificar los cambios realizados con el MERGE."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270697514,
     "inputWidgets": {},
     "nuid": "3cb9431c-9486-413c-8c48-4bf3016f0627",
     "showTitle": false,
     "startTime": 1792270695766,
     "submitTime": 1792270695697,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>fecha</th><th>producto</th><th>cantidad</th><th>precio</th></tr></thead><tbody><tr><td>1</td><td>2025-01-03</td><td>Teclado</td><td>20</td><td>27.99</td></tr><tr><td>1001</td><td>2025-01-31</td><td>Producto demo</td><td>2</td><td>10.0</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         "2025-01-03",
         "Teclado",
         20,
         27.99
        ],
        [
         1001,
         "2025-01-31",
         "Producto demo",
         2,
         10.0
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 46
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "fecha",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "producto",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "cantidad",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "precio",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT * FROM ventas_delta_demo WHERE id IN (1, 1001) ORDER BY id"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "73f52930-f484-4c78-955a-f5055296f29b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Muestra el historial de versiones y operaciones realizadas en ventas_delta_demo, incluyendo los cambios efectuados mediante UPDATE, INSERT, DELETE y MERGE."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792270716526,
     "inputWidgets": {},
     "nuid": "d96dfb39-1b42-45bb-aca1-79a0e7b181a1",
     "showTitle": false,
     "startTime": 1792270714656,
     "submitTime": 1792270714596,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>version</th><th>timestamp</th><th>userId</th><th>userName</th><th>operation</th><th>operationParameters</th><th>job</th><th>notebook</th><th>queryHistoryStatementId</th><th>clusterId</th><th>readVersion</th><th>isolationLevel</th><th>isBlindAppend</th><th>operationMetrics</th><th>userMetadata</th><th>engineInfo</th></tr></thead><tbody><tr><td>4</td><td>2026-10-17T20:58:09.000Z</td><td>77538316313781</td><td>andressabogal315@gmail.com</td><td>MERGE</td><td>Map(predicate -> [\"(id#17079 = id#17064)\"], clusterBy -> [], matchedPredicates -> [{\"actionType\":\"update\"}], statsOnLoad -> true, notMatchedBySourcePredicates -> [], notMatchedPredicates -> [{\"actionType\":\"insert\"}])</td><td>null</td><td>List(3412372199736854)</td><td>e51ddc9c-5d1b-4b3b-b893-28672faf7516</td><td>1017-190227-syaso9ee-v2n</td><td>3</td><td>WriteSerializable</td><td>false</td><td>Map(numTargetRowsCopied -> 0, numTargetRowsDeleted -> 0, numTargetFilesAdded -> 2, numTargetBytesAdded -> 3293, numTargetBytesRemoved -> 1660, numTargetDeletionVectorsAdded -> 0, numTargetRowsMatchedUpdated -> 1, executionTimeMs -> 3465, materializeSourceTimeMs -> 269, numTargetRowsInserted -> 1, numTargetRowsMatchedDeleted -> 0, numTargetDeletionVectorsUpdated -> 0, scanTimeMs -> 1669, numTargetRowsUpdated -> 1, numOutputRows -> 2, numTargetDeletionVectorsRemoved -> 0, numTargetRowsNotMatchedBySourceUpdated -> 0, numTargetChangeFilesAdded -> 0, numSourceRows -> 2, numTargetFilesRemoved -> 1, numTargetRowsNotMatchedBySourceDeleted -> 0, rewriteTimeMs -> 1461)</td><td>null</td><td>Databricks-Runtime/19.9.x-aarch64-photon-scala2.13</td></tr><tr><td>3</td><td>2026-10-17T20:56:54.000Z</td><td>77538316313781</td><td>andressabogal315@gmail.com</td><td>DELETE</td><td>Map(predicate -> [\"(id#16768 = 1001)\"])</td><td>null</td><td>List(3412372199736854)</td><td>3936e3f2-97c9-4fe5-b833-c74196d8d197</td><td>1017-190227-syaso9ee-v2n</td><td>2</td><td>WriteSerializable</td><td>false</td><td>Map(numRemovedFiles -> 1, numRemovedBytes -> 1663, numCopiedRows -> 0, numDeletionVectorsAdded -> 0, numDeletionVectorsRemoved -> 0, numAddedChangeFiles -> 0, executionTimeMs -> 1210, numDeletionVectorsUpdated -> 0, numDeletedRows -> 1, scanTimeMs -> 1085, numAddedFiles -> 0, numAddedBytes -> 0, rewriteTimeMs -> 124)</td><td>null</td><td>Databricks-Runtime/19.9.x-aarch64-photon-scala2.13</td></tr><tr><td>2</td><td>2026-10-17T20:56:19.000Z</td><td>77538316313781</td><td>andressabogal315@gmail.com</td><td>WRITE</td><td>Map(mode -> Append, statsOnLoad -> true, partitionBy -> [])</td><td>null</td><td>List(3412372199736854)</td><td>37b11227-19ff-4696-bea1-b539afec02fe</td><td>1017-190227-syaso9ee-v2n</td><td>1</td><td>WriteSerializable</td><td>true</td><td>Map(numFiles -> 1, numOutputRows -> 1, numOutputBytes -> 1663)</td><td>null</td><td>Databricks-Runtime/19.9.x-aarch64-photon-scala2.13</td></tr><tr><td>1</td><td>2026-10-17T20:55:59.000Z</td><td>77538316313781</td><td>andressabogal315@gmail.com</td><td>UPDATE</td><td>Map(predicate -> [\"(id#15966 = 1)\"])</td><td>null</td><td>List(3412372199736854)</td><td>d28af2bc-709b-414a-b058-af32b88895e7</td><td>1017-190227-syaso9ee-v2n</td><td>0</td><td>WriteSerializable</td><td>false</td><td>Map(numRemovedFiles -> 0, numRemovedBytes -> 0, numCopiedRows -> 0, numDeletionVectorsAdded -> 1, numDeletionVectorsRemoved -> 0, numAddedChangeFiles -> 0, executionTimeMs -> 3687, numDeletionVectorsUpdated -> 0, scanTimeMs -> 1798, numAddedFiles -> 1, numUpdatedRows -> 1, numAddedBytes -> 1660, rewriteTimeMs -> 1855)</td><td>null</td><td>Databricks-Runtime/19.9.x-aarch64-photon-scala2.13</td></tr><tr><td>0</td><td>2026-10-17T20:55:37.000Z</td><td>77538316313781</td><td>andressabogal315@gmail.com</td><td>CREATE OR REPLACE TABLE AS SELECT</td><td>Map(partitionBy -> [], clusterBy -> [], description -> null, isManaged -> true, properties -> {\"delta.parquet.compression.codec\":\"zstd\",\"delta.parquet.format.version.afe.internal\":\"2.12.0\",\"delta.enableDeletionVectors\":\"true\",\"delta.parquet.format.version\":\"2.12.0\",\"io.unitycatalog.tableId\":\"cd7f73bf-e780-4dcc-a5fa-50bcb6282957\"}, statsOnLoad -> true)</td><td>null</td><td>List(3412372199736854)</td><td>9b087f9b-c06f-42b9-9aaf-74d49ca2bbc2</td><td>1017-190227-syaso9ee-v2n</td><td>null</td><td>WriteSerializable</td><td>false</td><td>Map(numFiles -> 1, numRemovedFiles -> 0, numRemovedBytes -> 0, numDeletionVectorsRemoved -> 0, numOutputRows -> 100, numOutputBytes -> 2392)</td><td>null</td><td>Databricks-Runtime/19.9.x-aarch64-photon-scala2.13</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         4,
         "2026-10-17T20:58:09.000Z",
         "77538316313781",
         "andressabogal315@gmail.com",
         "MERGE",
         {
          "clusterBy": "[]",
          "matchedPredicates": "[{\"actionType\":\"update\"}]",
          "notMatchedBySourcePredicates": "[]",
          "notMatchedPredicates": "[{\"actionType\":\"insert\"}]",
          "predicate": "[\"(id#17079 = id#17064)\"]",
          "statsOnLoad": "true"
         },
         null,
         [
          "3412372199736854"
         ],
         "e51ddc9c-5d1b-4b3b-b893-28672faf7516",
         "1017-190227-syaso9ee-v2n",
         3,
         "WriteSerializable",
         false,
         {
          "executionTimeMs": "3465",
          "materializeSourceTimeMs": "269",
          "numOutputRows": "2",
          "numSourceRows": "2",
          "numTargetBytesAdded": "3293",
          "numTargetBytesRemoved": "1660",
          "numTargetChangeFilesAdded": "0",
          "numTargetDeletionVectorsAdded": "0",
          "numTargetDeletionVectorsRemoved": "0",
          "numTargetDeletionVectorsUpdated": "0",
          "numTargetFilesAdded": "2",
          "numTargetFilesRemoved": "1",
          "numTargetRowsCopied": "0",
          "numTargetRowsDeleted": "0",
          "numTargetRowsInserted": "1",
          "numTargetRowsMatchedDeleted": "0",
          "numTargetRowsMatchedUpdated": "1",
          "numTargetRowsNotMatchedBySourceDeleted": "0",
          "numTargetRowsNotMatchedBySourceUpdated": "0",
          "numTargetRowsUpdated": "1",
          "rewriteTimeMs": "1461",
          "scanTimeMs": "1669"
         },
         null,
         "Databricks-Runtime/19.9.x-aarch64-photon-scala2.13"
        ],
        [
         3,
         "2026-10-17T20:56:54.000Z",
         "77538316313781",
         "andressabogal315@gmail.com",
         "DELETE",
         {
          "predicate": "[\"(id#16768 = 1001)\"]"
         },
         null,
         [
          "3412372199736854"
         ],
         "3936e3f2-97c9-4fe5-b833-c74196d8d197",
         "1017-190227-syaso9ee-v2n",
         2,
         "WriteSerializable",
         false,
         {
          "executionTimeMs": "1210",
          "numAddedBytes": "0",
          "numAddedChangeFiles": "0",
          "numAddedFiles": "0",
          "numCopiedRows": "0",
          "numDeletedRows": "1",
          "numDeletionVectorsAdded": "0",
          "numDeletionVectorsRemoved": "0",
          "numDeletionVectorsUpdated": "0",
          "numRemovedBytes": "1663",
          "numRemovedFiles": "1",
          "rewriteTimeMs": "124",
          "scanTimeMs": "1085"
         },
         null,
         "Databricks-Runtime/19.9.x-aarch64-photon-scala2.13"
        ],
        [
         2,
         "2026-10-17T20:56:19.000Z",
         "77538316313781",
         "andressabogal315@gmail.com",
         "WRITE",
         {
          "mode": "Append",
          "partitionBy": "[]",
          "statsOnLoad": "true"
         },
         null,
         [
          "3412372199736854"
         ],
         "37b11227-19ff-4696-bea1-b539afec02fe",
         "1017-190227-syaso9ee-v2n",
         1,
         "WriteSerializable",
         true,
         {
          "numFiles": "1",
          "numOutputBytes": "1663",
          "numOutputRows": "1"
         },
         null,
         "Databricks-Runtime/19.9.x-aarch64-photon-scala2.13"
        ],
        [
         1,
         "2026-10-17T20:55:59.000Z",
         "77538316313781",
         "andressabogal315@gmail.com",
         "UPDATE",
         {
          "predicate": "[\"(id#15966 = 1)\"]"
         },
         null,
         [
          "3412372199736854"
         ],
         "d28af2bc-709b-414a-b058-af32b88895e7",
         "1017-190227-syaso9ee-v2n",
         0,
         "WriteSerializable",
         false,
         {
          "executionTimeMs": "3687",
          "numAddedBytes": "1660",
          "numAddedChangeFiles": "0",
          "numAddedFiles": "1",
          "numCopiedRows": "0",
          "numDeletionVectorsAdded": "1",
          "numDeletionVectorsRemoved": "0",
          "numDeletionVectorsUpdated": "0",
          "numRemovedBytes": "0",
          "numRemovedFiles": "0",
          "numUpdatedRows": "1",
          "rewriteTimeMs": "1855",
          "scanTimeMs": "1798"
         },
         null,
         "Databricks-Runtime/19.9.x-aarch64-photon-scala2.13"
        ],
        [
         0,
         "2026-10-17T20:55:37.000Z",
         "77538316313781",
         "andressabogal315@gmail.com",
         "CREATE OR REPLACE TABLE AS SELECT",
         {
          "clusterBy": "[]",
          "description": null,
          "isManaged": "true",
          "partitionBy": "[]",
          "properties": "{\"delta.parquet.compression.codec\":\"zstd\",\"delta.parquet.format.version.afe.internal\":\"2.12.0\",\"delta.enableDeletionVectors\":\"true\",\"delta.parquet.format.version\":\"2.12.0\",\"io.unitycatalog.tableId\":\"cd7f73bf-e780-4dcc-a5fa-50bcb6282957\"}",
          "statsOnLoad": "true"
         },
         null,
         [
          "3412372199736854"
         ],
         "9b087f9b-c06f-42b9-9aaf-74d49ca2bbc2",
         "1017-190227-syaso9ee-v2n",
         null,
         "WriteSerializable",
         false,
         {
          "numDeletionVectorsRemoved": "0",
          "numFiles": "1",
          "numOutputBytes": "2392",
          "numOutputRows": "100",
          "numRemovedBytes": "0",
          "numRemovedFiles": "0"
         },
         null,
         "Databricks-Runtime/19.9.x-aarch64-photon-scala2.13"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 47
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "version",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "userId",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "userName",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "operation",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "operationParameters",
         "type": "{\"keyType\":\"string\",\"type\":\"map\",\"valueContainsNull\":true,\"valueType\":\"string\"}"
        },
        {
         "metadata": "{}",
         "name": "job",
         "type": "{\"fields\":[{\"metadata\":{},\"name\":\"jobId\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"jobName\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"jobRunId\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"runId\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"jobOwnerId\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"triggerType\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"workspaceId\",\"nullable\":true,\"type\":\"string\"}],\"type\":\"struct\"}"
        },
        {
         "metadata": "{}",
         "name": "notebook",
         "type": "{\"fields\":[{\"metadata\":{},\"name\":\"notebookId\",\"nullable\":true,\"type\":\"string\"}],\"type\":\"struct\"}"
        },
        {
         "metadata": "{}",
         "name": "queryHistoryStatementId",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "clusterId",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "readVersion",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "isolationLevel",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "isBlindAppend",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "operationMetrics",
         "type": "{\"keyType\":\"string\",\"type\":\"map\",\"valueContainsNull\":true,\"valueType\":\"string\"}"
        },
        {
         "metadata": "{}",
         "name": "userMetadata",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "engineInfo",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "DESCRIBE HISTORY ventas_delta_demo"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "5ce1c172-d8e5-427a-9a4f-ae113ffe5c6b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Recupera la versión inicial de ventas_delta_demo usando version_inicial, compara la cantidad de filas iniciales y actuales, y muestra el registro con ID 1 en ambas versiones para identificar los cambios realizados."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792270741054,
     "inputWidgets": {},
     "nuid": "85da3164-f935-4eb6-be92-8624a8f3bd96",
     "showTitle": false,
     "startTime": 1792270737779,
     "submitTime": 1792270737736,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Filas iniciales: 100\nFilas actuales: 101\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>fecha</th><th>producto</th><th>cantidad</th><th>precio</th></tr></thead><tbody><tr><td>1</td><td>2025-01-03</td><td>Teclado</td><td>20</td><td>25.99</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         "2025-01-03",
         "Teclado",
         20,
         25.99
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "fecha",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "producto",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "cantidad",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "precio",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>fecha</th><th>producto</th><th>cantidad</th><th>precio</th></tr></thead><tbody><tr><td>1</td><td>2025-01-03</td><td>Teclado</td><td>20</td><td>27.99</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         "2025-01-03",
         "Teclado",
         20,
         27.99
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "fecha",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "producto",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "cantidad",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "precio",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "df_inicial = spark.read.option(\"versionAsOf\", version_inicial).table(\"ventas_delta_demo\")\n",
    "\n",
    "print(\"Filas iniciales:\", df_inicial.count())\n",
    "print(\"Filas actuales:\", spark.table(\"ventas_delta_demo\").count())\n",
    "display(df_inicial.filter(F.col(\"id\") == 1))\n",
    "display(spark.table(\"ventas_delta_demo\").filter(F.col(\"id\") == 1))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "9f1aa04b-0026-4ceb-bbc5-b54fb2be9fc9",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Guarda el DataFrame df_enriquecido como tabla Delta pedidos_enriquecido, sobrescribiendo completamente la tabla si ya existe."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792271082346,
     "inputWidgets": {},
     "nuid": "21642adb-eff8-4a4d-8b13-44a524ec0f50",
     "showTitle": false,
     "startTime": 1792271078922,
     "submitTime": 1792271078885,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "df_enriquecido.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\"pedidos_enriquecido\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "443dec06-8042-443e-8cfb-7f07d99f56db",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Cuenta el total de registros de la tabla pedidos_enriquecido y muestra el resultado con el nombre registros."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792271137612,
     "inputWidgets": {},
     "nuid": "c061221b-da9c-4434-ab1e-a0a3aa579d14",
     "showTitle": false,
     "startTime": 1792271135966,
     "submitTime": 1792271135889,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>registros</th></tr></thead><tbody><tr><td>10</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         10
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 60
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "registros",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT COUNT(*) AS registros\n",
    "FROM pedidos_enriquecido;"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "71bd710b-a22c-4b5d-8e6b-fe9a24177e84",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Cuenta los registros de las tablas pedidos_enriquecido, ventas y ventas_delta_demo, y combina los resultados en una sola tabla para comparar sus cantidades."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792271207989,
     "inputWidgets": {},
     "nuid": "7cab2112-8b71-43d6-aa80-208edfb8c360",
     "showTitle": false,
     "startTime": 1792271206021,
     "submitTime": 1792271205970,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>tabla</th><th>registros</th></tr></thead><tbody><tr><td>Pedidos enriquecidos</td><td>10</td></tr><tr><td>Ventas base</td><td>100</td></tr><tr><td>Ensayo Delta</td><td>101</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Pedidos enriquecidos",
         10
        ],
        [
         "Ventas base",
         100
        ],
        [
         "Ensayo Delta",
         101
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 63
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "tabla",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "registros",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT 'Pedidos enriquecidos' AS tabla, COUNT(*) AS registros\n",
    "FROM pedidos_enriquecido\n",
    "\n",
    "UNION ALL\n",
    "\n",
    "SELECT 'Ventas base', COUNT(*)\n",
    "FROM ventas\n",
    "\n",
    "UNION ALL\n",
    "\n",
    "SELECT 'Ensayo Delta', COUNT(*)\n",
    "FROM ventas_delta_demo;"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "29001d20-df27-48eb-8f18-046ac6e07201",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Crea o reemplaza la vista v_ventas_por_producto, que agrupa las ventas por producto y calcula las unidades vendidas y el importe total de cada producto."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792271213394,
     "inputWidgets": {},
     "nuid": "681beeb2-fe67-41ff-9056-54a65499cca2",
     "showTitle": false,
     "startTime": 1792271212466,
     "submitTime": 1792271212426,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.databricks.empty-table+json": {
       "directive_name": "CreateView"
      }
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE VIEW v_ventas_por_producto AS\n",
    "SELECT producto,\n",
    "       SUM(cantidad) AS unidades,\n",
    "       ROUND(SUM(cantidad * precio), 2) AS importe_total\n",
    "FROM ventas\n",
    "GROUP BY producto\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "c1b78ca6-ac6b-4eb2-9d46-e5c7fd41b2af",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Consulta la vista v_ventas_por_producto y muestra los productos ordenados de mayor a menor según su importe total de ventas."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792271218601,
     "inputWidgets": {},
     "nuid": "05d63273-4d8d-47d8-8788-fb5623194e3c",
     "showTitle": false,
     "startTime": 1792271216781,
     "submitTime": 1792271216748,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>producto</th><th>unidades</th><th>importe_total</th></tr></thead><tbody><tr><td>Monitor</td><td>64</td><td>12799.36</td></tr><tr><td>Impresora</td><td>85</td><td>7641.5</td></tr><tr><td>Altavoces</td><td>137</td><td>6165.0</td></tr><tr><td>Disco Duro</td><td>77</td><td>6159.23</td></tr><tr><td>Microfono</td><td>93</td><td>5579.07</td></tr><tr><td>Webcam</td><td>137</td><td>5478.63</td></tr><tr><td>Teclado</td><td>130</td><td>3378.7</td></tr><tr><td>Auriculares</td><td>92</td><td>2759.08</td></tr><tr><td>Mouse</td><td>88</td><td>1364.0</td></tr><tr><td>USB</td><td>99</td><td>989.01</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Monitor",
         64,
         12799.36
        ],
        [
         "Impresora",
         85,
         7641.5
        ],
        [
         "Altavoces",
         137,
         6165.0
        ],
        [
         "Disco Duro",
         77,
         6159.23
        ],
        [
         "Microfono",
         93,
         5579.07
        ],
        [
         "Webcam",
         137,
         5478.63
        ],
        [
         "Teclado",
         130,
         3378.7
        ],
        [
         "Auriculares",
         92,
         2759.08
        ],
        [
         "Mouse",
         88,
         1364.0
        ],
        [
         "USB",
         99,
         989.01
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 65
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "producto",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "unidades",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "importe_total",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT * FROM v_ventas_por_producto ORDER BY importe_total DESC"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "7cb58ebc-4699-4d6d-a28b-7973ddf42900",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Crea o reemplaza la función es_venta_alta, que recibe un importe y devuelve TRUE si es mayor que 500 o FALSE si es menor o igual a 500."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792271255640,
     "inputWidgets": {},
     "nuid": "060d4173-08d4-4bd4-aeea-e5d3769c00a5",
     "showTitle": false,
     "startTime": 1792271254560,
     "submitTime": 1792271254521,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr></tr></thead><tbody></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 66
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE FUNCTION es_venta_alta(importe DOUBLE)\n",
    "RETURNS BOOLEAN\n",
    "RETURN importe > 500"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "97d5772b-a153-408b-9d30-d0f472b266a2",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Muestra los primeros 10 registros de ventas, calcula el importe de cada venta (cantidad * precio) y utiliza es_venta_alta para indicar si supera los 500."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792271269061,
     "inputWidgets": {},
     "nuid": "2eb83435-453e-4d7c-a84c-8fc7b9bb2ccd",
     "showTitle": false,
     "startTime": 1792271267516,
     "submitTime": 1792271267448,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>producto</th><th>cantidad</th><th>precio</th><th>supera_umbral</th></tr></thead><tbody><tr><td>1</td><td>Teclado</td><td>20</td><td>25.99</td><td>true</td></tr><tr><td>2</td><td>Webcam</td><td>1</td><td>39.99</td><td>false</td></tr><tr><td>3</td><td>Auriculares</td><td>18</td><td>29.99</td><td>true</td></tr><tr><td>4</td><td>Webcam</td><td>10</td><td>39.99</td><td>false</td></tr><tr><td>5</td><td>Mouse</td><td>2</td><td>15.5</td><td>false</td></tr><tr><td>6</td><td>Altavoces</td><td>13</td><td>45.0</td><td>true</td></tr><tr><td>7</td><td>Teclado</td><td>11</td><td>25.99</td><td>false</td></tr><tr><td>8</td><td>Auriculares</td><td>5</td><td>29.99</td><td>false</td></tr><tr><td>9</td><td>Monitor</td><td>2</td><td>199.99</td><td>false</td></tr><tr><td>10</td><td>Microfono</td><td>20</td><td>59.99</td><td>true</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1,
         "Teclado",
         20,
         25.99,
         true
        ],
        [
         2,
         "Webcam",
         1,
         39.99,
         false
        ],
        [
         3,
         "Auriculares",
         18,
         29.99,
         true
        ],
        [
         4,
         "Webcam",
         10,
         39.99,
         false
        ],
        [
         5,
         "Mouse",
         2,
         15.5,
         false
        ],
        [
         6,
         "Altavoces",
         13,
         45.0,
         true
        ],
        [
         7,
         "Teclado",
         11,
         25.99,
         false
        ],
        [
         8,
         "Auriculares",
         5,
         29.99,
         false
        ],
        [
         9,
         "Monitor",
         2,
         199.99,
         false
        ],
        [
         10,
         "Microfono",
         20,
         59.99,
         true
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 67
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "producto",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "cantidad",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "precio",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "supera_umbral",
         "type": "\"boolean\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT id, producto, cantidad, precio,\n",
    "       es_venta_alta(cantidad * precio) AS supera_umbral\n",
    "FROM ventas\n",
    "ORDER BY id\n",
    "LIMIT 10"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "fe821453-de91-42f0-9ce4-429c943a3582",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Muestra información detallada de la vista v_ventas_por_producto, incluyendo su esquema, tipo de objeto, propiedades y definición SQL."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792271289671,
     "inputWidgets": {},
     "nuid": "d8bf196a-02a6-4012-a0f3-eefee983d7d2",
     "showTitle": false,
     "startTime": 1792271288574,
     "submitTime": 1792271288541,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>col_name</th><th>data_type</th><th>comment</th></tr></thead><tbody><tr><td>producto</td><td>string</td><td>null</td></tr><tr><td>unidades</td><td>bigint</td><td>null</td></tr><tr><td>importe_total</td><td>double</td><td>null</td></tr><tr><td></td><td></td><td></td></tr><tr><td># Detailed Table Information</td><td></td><td></td></tr><tr><td>Catalog</td><td>analitica</td><td></td></tr><tr><td>Database</td><td>data</td><td></td></tr><tr><td>Table</td><td>v_ventas_por_producto</td><td></td></tr><tr><td>Owner</td><td>andressabogal315@gmail.com</td><td></td></tr><tr><td>Created Time</td><td>Sat Oct 17 21:06:53 UTC 2026</td><td></td></tr><tr><td>Last Access</td><td>UNKNOWN</td><td></td></tr><tr><td>Created By</td><td>Spark </td><td></td></tr><tr><td>Type</td><td>VIEW</td><td></td></tr><tr><td>View Text</td><td>SELECT producto,\n",
       "       SUM(cantidad) AS unidades,\n",
       "       ROUND(SUM(cantidad * precio), 2) AS importe_total\n",
       "FROM ventas\n",
       "GROUP BY producto</td><td></td></tr><tr><td>View Schema Mode</td><td>COMPENSATION</td><td></td></tr><tr><td>View Catalog and Namespace</td><td>analitica.data</td><td></td></tr><tr><td>View Query Output Columns</td><td>[`producto`, `unidades`, `importe_total`]</td><td></td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "producto",
         "string",
         null
        ],
        [
         "unidades",
         "bigint",
         null
        ],
        [
         "importe_total",
         "double",
         null
        ],
        [
         "",
         "",
         ""
        ],
        [
         "# Detailed Table Information",
         "",
         ""
        ],
        [
         "Catalog",
         "analitica",
         ""
        ],
        [
         "Database",
         "data",
         ""
        ],
        [
         "Table",
         "v_ventas_por_producto",
         ""
        ],
        [
         "Owner",
         "andressabogal315@gmail.com",
         ""
        ],
        [
         "Created Time",
         "Sat Oct 17 21:06:53 UTC 2026",
         ""
        ],
        [
         "Last Access",
         "UNKNOWN",
         ""
        ],
        [
         "Created By",
         "Spark ",
         ""
        ],
        [
         "Type",
         "VIEW",
         ""
        ],
        [
         "View Text",
         "SELECT producto,\n       SUM(cantidad) AS unidades,\n       ROUND(SUM(cantidad * precio), 2) AS importe_total\nFROM ventas\nGROUP BY producto",
         ""
        ],
        [
         "View Schema Mode",
         "COMPENSATION",
         ""
        ],
        [
         "View Catalog and Namespace",
         "analitica.data",
         ""
        ],
        [
         "View Query Output Columns",
         "[`producto`, `unidades`, `importe_total`]",
         ""
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 68
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{\"comment\": \"name of the column\"}",
         "name": "col_name",
         "type": "\"string\""
        },
        {
         "metadata": "{\"comment\": \"data type of the column\"}",
         "name": "data_type",
         "type": "\"string\""
        },
        {
         "metadata": "{\"comment\": \"comment of the column\"}",
         "name": "comment",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "DESCRIBE TABLE EXTENDED v_ventas_por_producto"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "6370ce77-11d7-4459-bccc-0420da5d8114",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Muestra los permisos asignados sobre la vista v_ventas_por_producto, indicando qué usuarios o grupos pueden acceder a ella y qué acciones tienen autorizadas."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792271307091,
     "inputWidgets": {},
     "nuid": "00901ae3-6b58-46f5-89fc-111ab8071555",
     "showTitle": false,
     "startTime": 1792271305888,
     "submitTime": 1792271305851,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>Principal</th><th>ActionType</th><th>ObjectType</th><th>ObjectKey</th></tr></thead><tbody></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 69
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "Principal",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ActionType",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ObjectType",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ObjectKey",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SHOW GRANTS ON VIEW v_ventas_por_producto"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b541bac0-c2c6-4e0c-906f-04b0b173ff47",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Los comandos comentados muestran cómo otorgar al grupo lectores_clase permisos para usar el catálogo y esquema y consultar la vista; el comando final muestra los permisos actuales de v_ventas_por_producto sin modificarlos."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792271327107,
     "inputWidgets": {},
     "nuid": "ca12c313-5271-486d-8fd8-1703afdc7daa",
     "showTitle": false,
     "startTime": 1792271326393,
     "submitTime": 1792271326351,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>Principal</th><th>ActionType</th><th>ObjectType</th><th>ObjectKey</th></tr></thead><tbody></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 70
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "Principal",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ActionType",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ObjectType",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ObjectKey",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "-- GRANT USE CATALOG ON CATALOG analitica TO `lectores_clase`;\n",
    "-- GRANT USE SCHEMA ON SCHEMA analitica.data TO `lectores_clase`;\n",
    "-- GRANT SELECT ON VIEW analitica.data.v_ventas_por_producto TO `lectores_clase`;\n",
    "\n",
    "SHOW GRANTS ON VIEW v_ventas_por_producto;"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "d73b0387-f44d-4f83-a7a2-8f5c160c4e7c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Consulta y muestra todos los registros de la vista analitica.data.v_ventas_por_producto, que contiene las unidades vendidas y el importe total agrupados por producto."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "finishTime": 1792271341321,
     "inputWidgets": {},
     "nuid": "ecff328d-34bb-47bc-8da6-416d15a15803",
     "showTitle": false,
     "startTime": 1792271339644,
     "submitTime": 1792271339605,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>producto</th><th>unidades</th><th>importe_total</th></tr></thead><tbody><tr><td>Teclado</td><td>130</td><td>3378.7</td></tr><tr><td>Webcam</td><td>137</td><td>5478.63</td></tr><tr><td>Auriculares</td><td>92</td><td>2759.08</td></tr><tr><td>Mouse</td><td>88</td><td>1364.0</td></tr><tr><td>Altavoces</td><td>137</td><td>6165.0</td></tr><tr><td>Monitor</td><td>64</td><td>12799.36</td></tr><tr><td>Microfono</td><td>93</td><td>5579.07</td></tr><tr><td>USB</td><td>99</td><td>989.01</td></tr><tr><td>Impresora</td><td>85</td><td>7641.5</td></tr><tr><td>Disco Duro</td><td>77</td><td>6159.23</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Teclado",
         130,
         3378.7
        ],
        [
         "Webcam",
         137,
         5478.63
        ],
        [
         "Auriculares",
         92,
         2759.08
        ],
        [
         "Mouse",
         88,
         1364.0
        ],
        [
         "Altavoces",
         137,
         6165.0
        ],
        [
         "Monitor",
         64,
         12799.36
        ],
        [
         "Microfono",
         93,
         5579.07
        ],
        [
         "USB",
         99,
         989.01
        ],
        [
         "Impresora",
         85,
         7641.5
        ],
        [
         "Disco Duro",
         77,
         6159.23
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 71
       },
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "producto",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "unidades",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "importe_total",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "SELECT * FROM analitica.data.v_ventas_por_producto"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "ab65c540-82cd-4055-8544-32276da4e156",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Preguntas para revisar lo aprendido"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "cf6b7090-972f-4695-9abd-d38fbde3cbac",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "- #### Si el CSV está en un volumen, ¿qué falta para que otro notebook consulte una tabla persistente?\n",
    "\n",
    "En Databricks, hay que guardar los datos como una tabla persistente en Unity Catalog mediante saveAsTable, para que otros notebooks puedan consultarla.\n",
    "\n",
    "- #### Si una unión duplica el importe total, ¿qué comprobación haríamos sobre las llaves?\n",
    "\n",
    "Revisar que las llaves sean únicas en ambas tablas y que no existan duplicados que multipliquen los registros durante el JOIN.\n",
    "\n",
    "- #### Si una celda define un filtro, ¿significa que Spark ya calculó todo el resultado?\n",
    "\n",
    "No. Spark usa evaluación perezosa (lazy evaluation); el filtro se ejecuta cuando una acción, como display() o count(), requiere el resultado.\n",
    "\n",
    "- #### Si una persona tiene SELECT sobre una tabla, ¿qué otros permisos de uso necesita?\n",
    "\n",
    "Necesita USE CATALOG, USE SCHEMA y SELECT sobre la tabla o vista, según los permisos de Unity Catalog.\n",
    "\n",
    "- #### Si el historial de la tabla muestra una versión antigua, ¿basta para garantizar su recuperación?\n",
    "\n",
    "No. En Databricks, Delta Lake permite consultar versiones anteriores con Time Travel, pero depende de la retención de datos y archivos del historial; VACUUM puede eliminar archivos necesarios para recuperar versiones antiguas.\n",
    "\n",
    "- #### Si repetimos un append con el mismo archivo, ¿qué puede pasar con las ventas?\n",
    "\n",
    "Se pueden duplicar las ventas, porque append agrega los registros nuevamente sin comprobar si ya existen. En Databricks, se puede usar MERGE para actualizar o insertar registros según una llave única.\n"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "6"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "mostRecentlyExecutedCommandWithImplicitDF": {
     "commandId": 6897485212918380,
     "dataframes": [
      "_sqldf"
     ]
    },
    "pythonIndentUnit": 4
   },
   "notebookName": "Analitica_notebook",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}