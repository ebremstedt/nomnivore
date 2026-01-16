from dataclasses import dataclass
from io import StringIO
from typing import Any

import paramiko
import polars as pl
import xmltodict


@dataclass
class SFTPReader:
    hostname: str
    username: str
    pem_file: str
    port: int = 22

    def connect(self) -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        private_key = paramiko.RSAKey.from_private_key_file(self.pem_file)

        client.connect(
            hostname=self.hostname,
            port=self.port,
            username=self.username,
            pkey=private_key,
            timeout=30,
        )

        sftp = client.open_sftp()
        return client, sftp

    def get_csv_file(
        self, sftp_connection: paramiko.SFTPClient, filename: str, remote_path: str = "."
    ) -> pl.DataFrame | None:
        try:
            remote_file_path = f"{remote_path.rstrip('/')}/{filename}"
            with sftp_connection.file(remote_file_path, "r") as file:
                content = file.read().decode("utf-8")
            return pl.read_csv(source=StringIO(content))
        except Exception as e:
            print(f"Error getting {filename}: {e}")
            return None

    def get_xml_file(
        self, sftp_connection: paramiko.SFTPClient, filename: str, remote_path: str = "."
    ) -> dict[str, Any] | None:
        try:
            remote_file_path = f"{remote_path.rstrip('/')}/{filename}"
            with sftp_connection.file(remote_file_path, "r") as file:
                content = file.read().decode("utf-8")
            return xmltodict.parse(xml_input=content)
        except Exception as e:
            print(f"Error getting {filename}: {e}")
            return None