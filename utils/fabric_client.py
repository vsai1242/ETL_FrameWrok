"""Microsoft Fabric Lakehouse Client for ETL Testing"""

import pyodbc
import struct
import configparser
from azure.identity import ClientSecretCredential, InteractiveBrowserCredential


class FabricClient:
    def __init__(self, layer="BRONZE"):
        self.config = configparser.ConfigParser()
        self.config.read("config/master.properties")
        self.layer = f"FABRIC_{layer.upper()}"
        self.connection = None

    def connect(self):
        """Connect to Microsoft Fabric Lakehouse SQL Endpoint"""

        # ---- Safety check: enforce correct ODBC driver ----
        required_driver = "ODBC Driver 18 for SQL Server"
        if required_driver not in pyodbc.drivers():
            raise RuntimeError(
                "ODBC Driver 18 for SQL Server is required for Microsoft Fabric"
            )

        # ---- Read SQL endpoint ----
        layer_name = self.layer.split("_")[1]
        sql_endpoint = self.config.get(self.layer, f"{layer_name}_SQL_ENDPOINT")

        # ---- Authentication ----
        auth_method = self.config.get(
            "FABRIC", "FABRIC_AUTH_METHOD", fallback="Interactive"
        )

        if auth_method == "ServicePrincipal":
            tenant_id = self.config.get("FABRIC", "FABRIC_TENANT_ID")
            client_id = self.config.get("FABRIC", "FABRIC_CLIENT_ID")
            client_secret = self.config.get("FABRIC", "FABRIC_CLIENT_SECRET")

            credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )
        else:
            # Interactive browser authentication (MFA supported)
            tenant_id = self.config.get("FABRIC", "FABRIC_TENANT_ID")
            credential = InteractiveBrowserCredential(tenant_id=tenant_id)

        # ---- Acquire access token ----
        token = credential.get_token(
            "https://database.windows.net/.default"
        ).token

        # ---- Convert token for pyodbc ----
        token_bytes = token.encode("utf-16-le")
        token_struct = struct.pack(
            f"<I{len(token_bytes)}s", len(token_bytes), token_bytes
        )

        # ---- ODBC Driver 18 connection string ----
        conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server={sql_endpoint};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

        # ---- Connect ----
        self.connection = pyodbc.connect(
            conn_str,
            attrs_before={1256: token_struct},
        )

        return self.connection

    def execute_query(self, query):
        """Execute SQL query and return results"""
        if not self.connection:
            self.connect()

        cursor = self.connection.cursor()
        cursor.execute(query)

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
