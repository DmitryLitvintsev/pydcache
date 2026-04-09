import argparse
import json
import logging
import logging.config
import multiprocessing
import os
import re
import socket
import sys
from multiprocessing import Lock, Process, Queue
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, NoReturn
from urllib.parse import urlparse

import paramiko
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import yaml

from pydcache.util import admin, kerberos, ostools, paramiko, psycopg


from kafka import KafkaConsumer

"""

epoch_time\":1775496237.108132665,\"local_time\":\"2026-04-06T12:23:57-0500\",\"hostname\":\"tpsrvf2104\",\"program\":\"cta-taped\",\"log_level\":\"ERROR\",\"pid\":1160350,\"tid\":1638518,\"message\":\"In ArchiveMount::reportJobsBatchTransferred(): got an exception\",\"drive_name\":\"F1_F9B5D4\",\"instance\":\"prd\",\"sched_backend\":\"cephUser\",\"thread\":\"MainThread\",\"tapeDrive\":\"F1_F9B5D4\",\"mountId\":\"453120\",\"vo\":\"cms\",\"tapePool\":\"cms.Run2025DPrompt\",\"successfulBatchSize\":7,\"exceptionMessageValue\":\"commit problem committing the DB transaction: Database library reported: ERROR:  duplicate key value violates unique constraint \"archive_file_din_dfi_un\"DETAIL:  Key (disk_instance_name, disk_file_id)=(cms_prd, 00003DE869D8B37F4516A7FF64556A8A7E01) already exists. (DB Result Status:7 SQLState:23505)\"}

"""

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Global locks
print_lock = Lock()
kinit_lock = Lock()

CONFIG_FILE = os.getenv("DCACHE_CONFIG", "dcache.yaml")
SSH_HOST = "fndca"
SSH_PORT = 24223
SSH_USER = "enstore"
HOSTNAME = socket.getfqdn()


_VO = "vo"
_INSTANCE = "instance"


class Worker(Process):
    """Worker process to query CTA DB and process results."""

    def __init__(
        self,
        queue: Queue,
        config: Dict[str, Any]
    ) -> None:
        """Initialize the worker.

        Args:
            queue: Task queue
            config: Configuration dictionary
        """
        super().__init__()
        self.queue = queue
        self.config = config

    def run(self) -> None:
        """Process tasks from the queue."""
        cta_db = chimera_db = ssh = None
        try:
            cta_db = psycopg.create_connection(self.config["cta_db"])
            chimera_db = psycopg.create_connection(self.config["chimera_db"])
            ssh = paramiko.get_shell(
                self.config["admin"].get("host", SSH_HOST),
                self.config["admin"].get("port", SSH_PORT),
                self.config["admin"].get("user", SSH_USER)
            )

            for pnfsid in iter(self.queue.get, None):
                self._process_file(ssh, cta_db, chimera_db, pnfsid)
        except Exception as exc:
            logger.error("Worker failed: %s", exc)
        finally:
            for conn in (ssh, cta_db, chimera_db):
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _process_file(
        self,
        ssh,
        cta_db: psycopg2.extensions.connection,
        chimera_db: psycopg2.extensions.connection,
        pnfsid: str
    ) -> None:
        """Process a single file.

        Args:
            ssh: SSH connection
            cta_db: CTA database connection
            chimera_db: Chimera database connection
            pnfsid: PNFS ID to process
            instance: CTA instance name
        """

        rows = psycopg.select(
            cta_db,
            "select af.disk_instance_name, "
            "sc.storage_class_name, "
            "'cta://cta/'||af.disk_file_id||'?archiveid='||af.archive_file_id as location "
            "from archive_file af inner join storage_class sc on sc.storage_class_id = af.storage_class_id "
            "where af.disk_file_id = %s",
            (pnfsid,)
        )

        if not rows:
            return

        disk_instance_name = rows[0]["disk_instance_name"]
        location = rows[0]["location"]
        storage_class = rows[0]["storage_class_name"]
        storage_group, file_family = storage_class.split("@")[0].split(".")

        result = psycopg.select (
            chimera_db,
            "select count(*) from t_locationinfo "
            "where itype = 0 and inumber = "
            "(select inumber from t_inodes where ipnfsid = %s)",
            (pnfsid,)
        )

        if result[0]["count"] != 0:
            with print_lock:
                logger.error(
                    "File has location in chimera %s %s, Skipping",
                    pnfsid,
                    storage_class
                    )
            return

        psycopg.insert(
            chimera_db,
            "insert into t_storageinfo "
            "(inumber, ihsmname, istoragegroup, istoragesubgroup) "
            "values (pnfsid2inumber(%s), 'cta', %s, %s)",
            (pnfsid, storage_group, file_family)
        )

        psycopg.insert(
            chimera_db,
            "insert into t_locationinfo "
            "(inumber, itype, ipriority, ictime, iatime, istate, ilocation) "
            "values (pnfsid2inumber(%s), 0, 10, now(), now(), 1, %s)",
            (pnfsid, location)
        )

        admin.execute_admin_command(
            ssh,
            f"\\sl {pnfsid} rep set cached {pnfsid}"
        )

        admin.execute_admin_command(
            ssh,
            f"\\sl {pnfsid} st kill {pnfsid}"
        )


def main() -> None:

    """Main function to process files that are already on tape."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Process files that are already on tape and update their status"
    )

    parser.add_argument(
        "--cpu-count",
        type=int,
        default=10,
        help="Number of worker processes to spawn"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    parser.add_argument(
        "-i", "--instance",
        help='instance',
        default="public_prd",
        metavar="INSTANCE"
    )

    args = parser.parse_args()

    # Load configuration
    try:
        config_path = Path(CONFIG_FILE)
        #if config_path.stat().st_mode != 0o600:
        if config_path.stat().st_mode != 33152 :
            logger.error(
                "Config file %s permissions too permissive, should be 0600",
                CONFIG_FILE
            )
            sys.exit(1)

        config = yaml.safe_load(config_path.read_text())
        if not config:
            logger.error("Failed to load configuration from %s", CONFIG_FILE)
            sys.exit(1)

    except (OSError, IOError) as exc:
        if isinstance(exc, FileNotFoundError):
            logger.error("Config file %s does not exist", CONFIG_FILE)
        else:
            logger.error("Error reading config file: %s", exc)
        sys.exit(1)
    except yaml.YAMLError as exc:
        logger.error("Error parsing config file: %s", exc)
        sys.exit(1)

    logger.info("Starting processing")

    # Start Kerberos ticket refresh worker
    kinit_worker = kerberos.KinitWorker()
    kinit_worker.start()

    # Set up worker processes
    queue: Queue = Queue(maxsize=100)
    workers = [
        Worker(queue, config)
        for _ in range(args.cpu_count)
    ]

    for worker in workers:
        worker.start()


    consumer = KafkaConsumer(config["kafka"].get("topic", "ingest.logs.cta.taped"),
                             group_id=config["kafka"].get("group", "dcache-ctananny-prd"),  # Required to resume
                             bootstrap_servers=config["kafka"].get("bootstrap_servers", "lskafka.fnal.gov:9092"),
                             auto_offset_reset='earliest',    # Fallback if no offset is found
                             enable_auto_commit=True,          # Automatically save progress
                             value_deserializer=lambda m: json.loads(m.decode("utf-8")))

    try:
        for msg in consumer:
            message = msg.value
            message_string =  message["message"]
            payload = message["cta"]
            if _VO in payload and _INSTANCE in payload:
                vo = payload.get(_VO)
                instance = payload.get(_INSTANCE)
                exception_message = payload.get("exceptionMessageValue")
                if not exception_message:
                    continue
                if exception_message.find("duplicate key value violates unique constraint") != -1:
                    try:
                        tuple =  exception_message.split("=")[1].split("already")[0].strip()
                        disk_instance, pnfsid = re.sub("[()]", "", tuple).split(",")
                        if disk_instance == args.instance:
                            logger.debug(f"Found {pnfsid} {disk_instance}, putting on the queue")
                            queue.put(pnfsid)
                    except:
                        pass
    except KeyboardInterrupt:
        logger.info("Interrupted successfully.")
    finally:
        for _ in range(args.cpu_count):
            queue.put(None)
        # Clean up
        kinit_worker.stop = True
        kinit_worker.terminate()
        kinit_worker.join(timeout=1)




if __name__ == "__main__":
    main()
