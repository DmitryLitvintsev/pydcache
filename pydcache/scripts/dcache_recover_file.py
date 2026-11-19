#!/usr/bin/env python3

"""
Script to recover deleted file.

"""

from __future__ import annotations

import argparse
import logging
import multiprocessing
import os
import socket
import sys
import traceback
from datetime import datetime
from multiprocessing import Process, Queue
from pathlib import Path
from queue import Full
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import yaml

from pydcache.config import get_config_path
from pydcache.util import ostools, psycopg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)


HOSTNAME = socket.getfqdn()
CRC_SWITCH = '2019-08-21 09:54:26-05:00'
# seconds to wait on a full queue before checking that workers are alive
QUEUE_PUT_TIMEOUT = 5


def get_switch_epoch() -> int:
    """Timestamp when the change from 0 to 1 based adler checksum happened

    Returns:
        Seconds since the Unix epoch
    """
    return int(datetime.fromisoformat(CRC_SWITCH).timestamp())


def convert_0_adler32_to_1_adler32(crc: int, filesize: int) -> int:
    """Convert an adler32 checksum seeded with 0 to the standard seed of 1

    Standard adler32 starts its low sum at 1. Starting at 1 instead of 0
    adds 1 to the low 16 bits and, as the low sum is accumulated once per
    byte, the file size to the high 16 bits, both modulo 65521.

    Args:
        crc: adler32 checksum computed with seed 0
        filesize: file size in bytes

    Returns:
        adler32 checksum of the same data computed with seed 1
    """
    BASE = 65521
    size = filesize % BASE
    s1 = (crc & 0xffff)
    s2 = ((crc >> 16) & 0xffff)
    s1 = (s1 + 1) % BASE
    s2 = (size + s2) % BASE
    new_adler = (s2 << 16) + s1
    return new_adler


def insert_archive_file(
    cta_db: psycopg2.extensions.connection,
    file_info: Dict[str, Any]
) -> None:
    """Insert archive file record based on file_recycle_log record

    Args:
        cta_db: CTA database connection
        file_info: dictionary containing file record in file_recycle_log
    """
    psycopg.insert(
        cta_db,
        "insert into archive_file ( "
        "  archive_file_id, "
        "  disk_instance_name, "
        "  disk_file_id, "
        "  disk_file_uid, "
        "  disk_file_gid, "
        "  size_in_bytes, "
        "  checksum_blob, "
        "  checksum_adler32, "
        "  storage_class_id, "
        "  creation_time, "
        "  reconciliation_time "
        " ) values ( "
        "%s, "
        "%s, "
        "%s, "
        "%s, "
        "%s, "
        "%s, "
        "%s, "
        "%s, "
        "%s, "
        "%s, "
        "%s) "
        # already restored by a previous (interrupted) run; a different
        # archive file claiming this disk file id must still fail
        "on conflict (archive_file_id) do nothing",
        (file_info.get("archive_file_id"),
         file_info.get("disk_instance_name"),
         file_info.get("disk_file_id_when_deleted"),
         file_info.get("disk_file_uid"),
         file_info.get("disk_file_gid"),
         file_info.get("size_in_bytes"),
         file_info.get("checksum_blob"),
         file_info.get("checksum_adler32"),
         file_info.get("storage_class_id"),
         file_info.get("archive_file_creation_time"),
         file_info.get("reconciliation_time")))


def insert_tape_file(
    cta_db: psycopg2.extensions.connection,
    file_info: Dict[str, Any]
) -> None:
    """Insert tape file record based on file_recycle_log record

    Args:
        cta_db: CTA database connection
        file_info: dictionary containing file record in file_recycle_log
    """

    psycopg.insert(
        cta_db,
        "insert into tape_file( "
        "  vid, "
        "  fseq, "
        "  block_id, "
        "  logical_size_in_bytes, "
        "  copy_nb, "
        "  creation_time, "
        "  archive_file_id "
        "  ) values ( "
        "%s, "
        "%s, "
        "%s, "
        "%s, "
        "%s, "
        "%s, "
        "%s ) ",
        (file_info.get("vid"),
         file_info.get("fseq"),
         file_info.get("block_id"),
         file_info.get("size_in_bytes"),
         file_info.get("copy_nb"),
         file_info.get("tape_file_creation_time"),
         file_info.get("archive_file_id")))


def update_inode(
    chimera_db: psycopg2.extensions.connection,
    chimera_file_info: Dict[str, Any],
    pnfsid: str
) -> None:
    """Replace temporary file inode info with deleted file info

    Args:
        chimera_db: Chimera database connection
        chimera_file_info: dictionary containing original file info
        pnfsid: pnfsid of temporary file
    """
    psycopg.update(
        chimera_db,
        "update t_inodes set "
        " ipnfsid = %s, "
        " imode = %s, "
        " iuid = %s, "
        " igid = %s, "
        " icrtime = %s, "
        " ictime = %s, "
        " iatime = %s, "
        " imtime = %s, "
        " iaccess_latency = %s, "
        " iretention_policy = 0, "
        " isize = %s "
        "where ipnfsid = %s",
        (chimera_file_info.get("ipnfsid"),
         chimera_file_info.get("imode"),
         chimera_file_info.get("iuid"),
         chimera_file_info.get("igid"),
         chimera_file_info.get("icrtime"),
         chimera_file_info.get("icrtime"),
         chimera_file_info.get("icrtime"),
         chimera_file_info.get("icrtime"),
         chimera_file_info.get("iaccess_latency"),
         chimera_file_info.get("isize"),
         pnfsid,))


def update_inode_checksum(
    chimera_db: psycopg2.extensions.connection,
    adler32_csum: str,
    pnfsid: str
) -> None:
    """Set inode adler32 checksum, replacing an existing one

    Args:
        chimera_db: Chimera database connection
        adler32_csum: adler32 checksum
        pnfsid: pnfsid of deleted file
    """
    # itype 1 is ADLER32; a file created by touch has no checksum row yet
    psycopg.insert(
        chimera_db,
        "insert into t_inodes_checksum (inumber, itype, isum) "
        "values ("
        "(select inumber from t_inodes where ipnfsid = %s), 1, %s) "
        "on conflict (inumber, itype) "
        "do update set isum = excluded.isum",
        (pnfsid, adler32_csum))


def upsert_storageinfo(
    chimera_db: psycopg2.extensions.connection,
    pnfsid: str,
    hsm: str,
    storage_group: str,
    storage_subgroup: str
) -> None:
    """Set inode storage info, replacing an existing one

    Args:
        chimera_db: Chimera database connection
        pnfsid: pnfsid of deleted file
        hsm: HSM name
        storage_group: storage group (VO)
        storage_subgroup: storage subgroup (file family)
    """
    psycopg.insert(
        chimera_db,
        "insert into t_storageinfo ("
        " inumber, ihsmname, istoragegroup, istoragesubgroup "
        ") values ("
        "(select inumber from t_inodes where ipnfsid = %s), %s, %s, %s) "
        "on conflict (inumber) "
        "do update set ihsmname = excluded.ihsmname, "
        " istoragegroup = excluded.istoragegroup, "
        " istoragesubgroup = excluded.istoragesubgroup",
        (pnfsid, hsm, storage_group, storage_subgroup))
    logger.info(
        "Set t_storageinfo "
        f"{pnfsid} {hsm} {storage_group} {storage_subgroup}")


def inode_exists(
    chimera_db: psycopg2.extensions.connection,
    pnfsid: str
) -> bool:
    """Check whether an inode exists in the namespace

    Args:
        chimera_db: Chimera database connection
        pnfsid: pnfsid to check

    Returns:
        True if an inode with this pnfsid exists
    """
    rows = psycopg.select(
        chimera_db,
        "select 1 from t_inodes where ipnfsid = %s",
        (pnfsid,))
    return bool(rows)


def update_locationinfo(
    chimera_db: psycopg2.extensions.connection,
    chimera_file_info: Dict[str, Any],
    location: str,
    pnfsid_new: str
) -> None:
    """Update locationinfo for pnfsid

    Args:
        chimera_db: Chimera database connection
        chimera_file_info: dictionary containing original file info
        location: location string
        pnfsid_new: placeholder file pnfsid
    """

    pnfsid = chimera_file_info.get("ipnfsid")

    psycopg.insert(
        chimera_db,
        "insert into t_locationinfo ("
        " inumber, itype, ipriority, ictime, iatime, istate, ilocation "
        ") values ("
        "(select inumber from t_inodes where ipnfsid = %s), "
        "0, 10, %s, %s, 1, %s)",
        (chimera_file_info.get("ipnfsid"),
         chimera_file_info.get("icrtime"),
         chimera_file_info.get("icrtime"),
         location))

    rows = psycopg.select(
        chimera_db,
        "select ilocation from t_locationinfo "
        "where inumber = (select inumber from t_inodes where ipnfsid = %s) "
        " and itype = 1",
        (pnfsid,))

    if rows:
        locations = [row["ilocation"] for row in rows]
        logger.info(f"TODO: delete {pnfsid_new} on locations {locations}")

    psycopg.insert(
        chimera_db,
        "delete from t_locationinfo "
        "where inumber = (select inumber from t_inodes where ipnfsid = %s) "
        " and itype = 1",
        (pnfsid,))

    psycopg.insert(
        chimera_db,
        "delete from t_locationinfo "
        "where inumber = (select inumber from t_inodes where ipnfsid = %s) "
        "and ilocation like '%%*'",
        (pnfsid,))


def delete_from_deleted_paths(
    chimera_db: psycopg2.extensions.connection,
    pnfsid: str
) -> None:
    """Delete from deleted_paths

    Args:
        chimera_db: Chimera database connection
        pnfsid: pnfsid of deleted file
    """
    psycopg.insert(
        chimera_db,
        "delete from t_deleted_paths "
        "where ipnfsid = %s ",
        (pnfsid,))


def delete_from_file_recycle_log(
    cta_db: psycopg2.extensions.connection,
    archive_file_id: int,
    pnfsid: str
) -> None:
    """Delete entry file_recycle_log

    Args:
       cta_db: CTA database connection
       archive_file_id: archive file id
       pnfsid: pnfsid of deleted file
    """
    psycopg.insert(
        cta_db,
        "delete from file_recycle_log "
        "where "
        " archive_file_id = %s "
        " and disk_file_id_when_deleted = %s ",
        (archive_file_id, pnfsid))


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
        cta_db = chimera_db = enstore_db = None
        exit_code = 0
        try:
            cta_db = psycopg.create_connection(self.config["cta_db"])
            chimera_db = psycopg.create_connection(self.config["chimera_db"])
            if "enstore_db" in self.config:
                enstore_db = psycopg.create_connection(
                    self.config["enstore_db"])
            for file_name in iter(self.queue.get, None):
                try:
                    self._process_file(file_name, cta_db,
                                       chimera_db, enstore_db)
                except Exception as e:
                    logger.error(f"Error processing {file_name} {e}")
                    error_trace = traceback.format_exc()
                    logger.error(error_trace)
                    # a failed statement leaves the transaction aborted
                    for conn in (cta_db, chimera_db, enstore_db):
                        if conn:
                            conn.rollback()
        except Exception as exc:
            logger.error(f"Worker failed: {exc}")
            error_trace = traceback.format_exc()
            logger.error(error_trace)
            exit_code = 1
        finally:
            for conn in (cta_db, chimera_db, enstore_db):
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
        if exit_code:
            # let the main process see the failure in exitcode
            sys.exit(exit_code)

    def _process_file(
        self,
        file_name: str,
        cta_db: psycopg2.extensions.connection,
        chimera_db: psycopg2.extensions.connection,
        enstore_db: Optional[psycopg2.extensions.connection]
    ) -> None:
        """Process a single file.

        Args:
            file_name: full file name
            cta_db: CTA database connection
            chimera_db: Chimera database connection
            enstore_db: Enstore database connection, None if not
                        configured (SFA files cannot be recovered)
        """

        dir_name = os.path.dirname(file_name)
        if not os.path.exists(dir_name):
            logger.warning(
                f"Parent directory does not exist, skipping {file_name}")
            return

        if os.path.exists(file_name):
            logger.error(f"File exists, skipping {file_name}")
            return

        rows = psycopg.select(
            chimera_db,
            "select * "
            "from t_deleted_paths where iname = %s "
            "order by ideltime desc limit 1",
            (file_name, )
        )

        if not rows:
            logger.error(
                f"{file_name} does not exist in t_deleted_paths in chimera, "
                "skipping")
            return

        chimera_file_info = rows[0]
        pnfsid = chimera_file_info.get("ipnfsid")

        rows = psycopg.select(
            cta_db,
            "select sc.storage_class_name, frl.* "
            "from file_recycle_log frl inner join storage_class sc "
            "on frl.storage_class_id = sc.storage_class_id  "
            "where disk_file_id_when_deleted = %s ",
            (pnfsid, )
        )

        if not rows:
            if not enstore_db:
                logger.error(
                    f"{file_name} {pnfsid} does not exist in CTA DB, "
                    "skipping")
                return
            enstore_rows = psycopg.select(
                enstore_db,
                "select f.bfid, f.package_id, "
                "       f.pnfs_id as child_pnfsid, "
                "       p.pnfs_id as parent_pnfsid, "
                "       f.crc, f.size, v.storage_group, "
                "       v.file_family from file f "
                "inner join file p on f.package_id = p.bfid "
                "inner join volume v on v.id = f.volume "
                "where f.package_id != f.bfid and f.deleted = 'n' "
                "and f.package_id is not null and f.pnfs_id = %s "
                "and p.deleted = 'n'",
                (pnfsid, ))
            if not enstore_rows:
                logger.error(
                    f"{file_name} {pnfsid} does not exist in CTA DB and is "
                    "not an SFA file in Enstore DB, skipping")
                return
            if len(enstore_rows) > 1:
                logger.error(
                    f"{file_name} {pnfsid} has multiple SFA records in "
                    "Enstore DB, skipping")
                return
            self._process_sfa_file(
                file_name, chimera_db, chimera_file_info, enstore_rows[0])
            return

        archive_file_id = rows[0].get("archive_file_id")
        adler32_csum = f"{int(rows[0].get('checksum_adler32')):08x}"
        storage_class = rows[0].get("storage_class_name")
        parts = storage_class.split("@")
        (vo, file_family), hsm = parts[0].split("."), parts[1]
        location = f"cta://cta/{pnfsid}?archiveid={archive_file_id}"

        # placeholder file to be populated from deleted file metadata;
        # created only once all metadata has been validated
        Path(file_name).touch()
        new_pnfsid = ostools.get_pnfsid(file_name)

        update_inode(chimera_db, chimera_file_info, new_pnfsid)

        logger.info(f"Doing checksum {pnfsid} {adler32_csum}")

        update_inode_checksum(chimera_db, adler32_csum, pnfsid)

        upsert_storageinfo(chimera_db, pnfsid, hsm, vo, file_family)

        update_locationinfo(
            chimera_db, chimera_file_info, location, new_pnfsid)
        logger.info(f"Inserted into t_locationinfo {pnfsid} {location}")

        # CTA is modified last, so that a failure in Chimera leaves CTA
        # untouched and only one database needs to be fixed by hand
        # TODO: make the Chimera and CTA updates transactional; each
        # statement currently commits on its own, so losing a connection
        # between statements leaves a partially recovered file
        inserted_archive = False
        for file_info in rows:
            if not inserted_archive:
                insert_archive_file(cta_db, file_info)
                inserted_archive = True
            try:
                insert_tape_file(cta_db, file_info)
            except psycopg2.IntegrityError:
                logger.warning(
                    "Tape file record already exists "
                    f"{archive_file_id} {pnfsid} {file_name}")

            psycopg.insert(
                cta_db,
                "update tape set dirty = '1' where vid = %s",
                (file_info.get("vid"),))

        delete_from_file_recycle_log(cta_db, archive_file_id, pnfsid)
        delete_from_deleted_paths(chimera_db, pnfsid)

    def _process_sfa_file(
        self,
        file_name: str,
        chimera_db: psycopg2.extensions.connection,
        chimera_file_info: Dict[str, Any],
        enstore_file_info: Dict[str, Any]
    ) -> None:
        """Process a single SFA file.

        Args:
            file_name: full file name
            chimera_db: Chimera database connection
            chimera_file_info: dictionary containing
                               chimera info about file from
                               t_deleted_paths
            enstore_file_info: dictionary containing
                               enstore info about file
        """
        logger.debug(f"{chimera_file_info} {enstore_file_info}")

        pnfsid = enstore_file_info["child_pnfsid"]
        parent_pnfsid = enstore_file_info["parent_pnfsid"]

        # without its package the recovered file could never be read
        if not inode_exists(chimera_db, parent_pnfsid):
            logger.error(
                f"{file_name} {pnfsid} SFA package {parent_pnfsid} "
                "does not exist in chimera, skipping")
            return

        file_crc = enstore_file_info["crc"]
        file_size = enstore_file_info["size"]
        if file_crc is None or file_size is None:
            logger.error(
                f"{file_name} {pnfsid} has no checksum or size in "
                "Enstore DB, skipping")
            return

        file_create_time = int(enstore_file_info["bfid"][4:14])
        if (file_create_time < get_switch_epoch()
                and HOSTNAME.endswith(".fnal.gov")):
            file_crc = convert_0_adler32_to_1_adler32(file_crc, file_size)
            logger.debug(
                "Caught 0 seeded adler32 "
                f"{file_name} {pnfsid} {file_crc}")

        adler32_csum = f"{file_crc:08x}"
        vo = enstore_file_info["storage_group"]
        file_family = enstore_file_info["file_family"]
        if file_family.endswith("-MIGRATION"):
            file_family = file_family[:-len("-MIGRATION")]
        hsm = "cta"
        location = f"sfa://sfa/{pnfsid}?packageid={parent_pnfsid}"

        logger.debug(
            f"creation time {file_create_time} {location} "
            f"{adler32_csum} {vo} {file_family} {hsm}")

        # placeholder file to be populated from deleted file metadata;
        # created only once all metadata has been validated
        Path(file_name).touch()
        new_pnfsid = ostools.get_pnfsid(file_name)

        update_inode(chimera_db, chimera_file_info, new_pnfsid)

        logger.info(f"Doing checksum {pnfsid} {adler32_csum}")

        update_inode_checksum(chimera_db, adler32_csum, pnfsid)

        upsert_storageinfo(chimera_db, pnfsid, hsm, vo, file_family)

        update_locationinfo(
            chimera_db, chimera_file_info, location, new_pnfsid)
        logger.info(f"Inserted into t_locationinfo {pnfsid} {location}")
        delete_from_deleted_paths(chimera_db, pnfsid)


def put_while_workers_alive(
    queue: Queue,
    item: Any,
    workers: List[Process]
) -> bool:
    """Put item on the queue unless all workers have exited

    A worker exits if, e.g., it cannot connect to a database; blocking
    on a full queue with no worker left to consume it would hang forever.

    Args:
        queue: Task queue
        item: item to put
        workers: worker processes consuming the queue

    Returns:
        True if the item was queued, False if no worker is alive
    """
    while True:
        try:
            queue.put(item, timeout=QUEUE_PUT_TIMEOUT)
            return True
        except Full:
            if not any(worker.is_alive() for worker in workers):
                return False


def main() -> None:
    """Main function to recover deleted files."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Recover removed files"
    )

    parser.add_argument(
        "--cpu-count",
        type=int,
        default=multiprocessing.cpu_count(),
        help="Number of worker processes to spawn"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    parser.add_argument(
        "-p", "--path",
        nargs='+',
        help='File paths to recover, space separated',
        required=False,
        metavar="PATH")

    parser.add_argument(
        "-f", "--file",
        type=str,
        default=None,
        metavar="FILE",
        help="File with list of full path names to recover, one path per line",
    )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if not args.file and not args.path:
        parser.print_help(sys.stderr)
        sys.exit(1)

    if args.cpu_count < 1:
        parser.error("--cpu-count must be at least 1")

    # Load configuration
    config = {}
    try:
        config_path = get_config_path()
        if config_path.stat().st_mode != 33152:
            logger.error(
                "Config file %s permissions too permissive, should be 0600",
                config_path
            )
            sys.exit(1)

        config = yaml.safe_load(config_path.read_text())
        if not config:
            logger.error("Failed to load configuration from %s", config_path)
            sys.exit(1)

    except (OSError, IOError) as exc:
        if isinstance(exc, FileNotFoundError):
            logger.error("Config file %s does not exist", exc.filename)
        else:
            logger.error("Error reading config file: %s", exc)
        sys.exit(1)
    except yaml.YAMLError as exc:
        logger.error("Error parsing config file: %s", exc)
        sys.exit(1)

    logger.info("Starting recovery processing")

    # Process files
    files = set()
    if args.path:
        files = set(args.path)

    if args.file:
        with open(args.file, "r") as f:
            for line in f:
                file_path = line.strip()
                if not file_path:
                    continue
                files.add(file_path)

    if not files:
        logger.info("Nothing to do")
        return

    logger.info(f"Collected {len(files)} files for processing")

    # no point in more workers (and database connections) than files
    num_workers = min(args.cpu_count, len(files))

    queue: Queue = Queue(maxsize=1000)
    workers = [
        Worker(queue, config)
        for _ in range(num_workers)
    ]

    for worker in workers:
        worker.start()

    aborted = False
    for i, item in enumerate(files, 1):
        if i % 100 == 0:
            logger.info(f"Queued {i} files for processing")
        if not put_while_workers_alive(queue, item, workers):
            logger.error(
                f"All workers exited, {len(files) - i + 1} files "
                "were not queued for processing")
            aborted = True
            break

    # Signal workers to finish
    if not aborted:
        for _ in range(num_workers):
            if not put_while_workers_alive(queue, None, workers):
                break

    # Wait for workers
    for worker in workers:
        worker.join()

    failed = sum(1 for worker in workers if worker.exitcode != 0)
    if failed:
        logger.error(f"{failed} of {num_workers} workers failed")

    if aborted or failed:
        # items left in the queue would otherwise block the exit of
        # this process, waiting for them to be consumed
        queue.cancel_join_thread()
        sys.exit(1)

    logger.info("Processing completed successfully")


if __name__ == "__main__":
    main()
