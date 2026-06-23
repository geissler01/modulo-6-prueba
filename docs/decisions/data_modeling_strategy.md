# Data Modeling Strategy (ADR)

Este documento registra las decisiones arquitectónicas respecto al almacenamiento, estructura y modelado de datos para el Data Warehouse analítico de DataMart S.A.S.

## 1. Arquitectura Medallón (Medallion Architecture)

Para garantizar la trazabilidad, calidad y correcta transformación de los datos, hemos optado por implementar la **Arquitectura Medallón**. Esta arquitectura separa lógicamente los datos en tres capas progresivas (schemas en PostgreSQL) que refinan la información desde su estado crudo hasta el modelo de negocio final.

### 1.1 Capa Bronze (Raw / Espejo)
* **Objetivo:** Actuar como un espejo exacto e inmutable de los datos de origen (archivos CSV descargados de Kaggle).
* **Características:** En esta capa, los datos se ingieren tal cual vienen. Los tipos de datos suelen ser genéricos (como texto) para evitar fallos de carga por problemas de formato en el origen.
* **Justificación:** Si alguna vez se necesita auditar un dato o si la lógica de negocio cambia drásticamente en el futuro, siempre podemos reprocesar la información consultando la capa Bronze, sin necesidad de volver a descargar o consumir la API de origen.

### 1.2 Capa Silver (Limpieza y Tipado)
* **Objetivo:** Limpiar y estandarizar los datos crudos manteniendo la misma granularidad.
* **Características:** 
  * Eliminación de registros duplicados.
  * Manejo de valores nulos o anómalos.
  * Conversión de tipos de datos estrictos (ej. fechas reales, numéricos, decimales).
  * Estandarización de cadenas de texto (mayúsculas, minúsculas, limpieza de espacios).
* **Justificación:** Es imperativo tener una única "fuente de verdad" limpia. Al tener una capa Silver, los modelos analíticos y las reglas de negocio se construyen sobre datos confiables sin tener que repetir lógica de limpieza en cada script.

### 1.3 Capa Gold (Lógica de Negocio y Modelado Dimensional)
* **Objetivo:** Entregar datos listos para el consumo de los analistas de negocio, herramientas de BI (Business Intelligence) y reportes finales.
* **Características:**
  * Aplicación estricta de las reglas de negocio de DataMart S.A.S.
  * Modelado dimensional clásico: **Tablas de Hechos (Fact Tables)** y **Tablas de Dimensiones (Dimension Tables)**.
  * Agregaciones o consolidaciones financieras si el caso de uso lo requiere.
* **Justificación:** El equipo analítico no necesita lidiar con datos transaccionales normalizados complejos. La capa Gold está diseñada para responder preguntas de negocio de la manera más rápida y eficiente posible mediante modelos en estrella (Star Schema).

## 2. Aprovisionamiento Dinámico de Base de Datos (IaC)
En lugar de depender de una base de datos creada manualmente por el usuario o usar la base de datos por defecto (`postgres`), el pipeline de Airflow está programado para conectarse al motor de base de datos al inicio y ejecutar dinámicamente un `CREATE DATABASE db_geisler_prueba`. 
Posteriormente, genera de forma automática los tres esquemas (`bronze`, `silver`, `gold`). Esto cumple con las mejores prácticas de **Infrastructure as Code (IaC)**, garantizando que el pipeline se pueda desplegar desde cero en cualquier entorno sin intervención manual del DBA.
