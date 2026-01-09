# Dataiku Snowflake Project

Este proyecto implementa un pipeline de CI/CD para gestionar infraestructura y datos en Snowflake utilizando GitHub Actions.

## Arquitectura

El pipeline automatiza dos procesos principales:

1.  **Gestión de Esquema (IaC):** Utiliza [schemachange](https://github.com/Snowflake-Labs/schemachange) para gestionar objetos de base de datos de manera incremental.
    *   Los scripts SQL se encuentran en `scripts_sql/`.
2.  **Ingesta de Datos:** Scripts de Python para descargar datos de APIs externas e insertarlos en Snowflake.
    *   Los scripts Python se encuentran en `scripts_py/`.
    *   Actualmente descarga precios de electricidad (PVPC y OMIE) desde la API de ESIOS (Red Eléctrica).

## Estructura del Proyecto

```
.
├── .github/workflows/   # Definiciones de GitHub Actions
├── scripts_sql/         # Migraciones SQL (versionadas V1.1.1__, etc.)
├── scripts_py/          # Scripts de Python para ingesta de datos
└── README.md            # Documentación del proyecto
```

## Configuración y Despliegue

El despliegue es automático al hacer push a la rama `main`. El workflow (`deploy_snowflake.yml`) se encarga de:
1.  Instalar dependencias (`schemachange`, `snowflake-connector-python`, `requests`).
2.  Ejecutar `schemachange` para aplicar cambios pendientes en `scripts_sql/`.
3.  Ejecutar `scripts_py/ingest_pvpc.py` para actualizar los datos de precios.

### Variables de Entorno y Secretos

Para que el pipeline funcione, debes configurar los siguientes **GitHub Secrets** en tu repositorio (`Settings > Secrets and variables > Actions`):

| Nombre Variable | Descripción | Ejemplo / Notas |
| :--- | :--- | :--- |
| `SNOWFLAKE_ACCOUNT` | Identificador de tu cuenta Snowflake. | `xy12345.eu-west-1` |
| `SNOWFLAKE_USER` | Usuario de servicio para el despliegue. | `GITHUB_ACTIONS_USER` |
| `SNOWFLAKE_PASSWORD` | Contraseña del usuario (si usas autenticación básica). | `TuPasswordSegura` |
| `SNOWFLAKE_KEY` | (Opcional) Clave privada si usas Key Pair auth. | `-----BEGIN PRIVATE KEY...` |
| `REE_TOKEN` | Token para la API de ESIOS (Red Eléctrica). | Solicítalo en [api.esios.ree.es](https://api.esios.ree.es/) |

> **Nota:** El script de Python detecta automáticamente si se usa `SNOWFLAKE_KEY` o `SNOWFLAKE_PASSWORD`.

## Datos Ingestados

*   **Base de Datos:** `DataWareHouse`
*   **Esquema:** `ELECTRICITY_MARKET`
*   **Tabla:** `ELECTRICITY_PRICES`
    *   Contiene precios horarios en **€/kWh**.
    *   Fuentes: `PVPC` (Minorista) y `OMIE` (Mayorista).