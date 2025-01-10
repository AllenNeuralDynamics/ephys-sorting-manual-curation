import json
import os
import os.path
import platform as system_platform
import re
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.models.computations_requests import RunCapsuleRequest
from aind_data_schema.core.data_description import (
    DataLevel,
    DataRegex,
    DerivedDataDescription,
    Funding,
    Modality,
    build_data_name,
)
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.platforms import Platform
from aind_data_schema_models.pid_names import PIDName
from botocore.exceptions import ClientError


_INVESTIGATORS_GH_TO_NAME_MAP = json.loads(os.getenv("INVESTIGATORS_GH_TO_NAME_MAP", "{}"))


def download_params_from_aws(store_name):
    """Attempt to download the endpoints from an aws parameter store"""
    ssm_client = boto3.client("ssm")
    try:
        param_from_store = ssm_client.get_parameter(Name=store_name)
        param_string = param_from_store["Parameter"]["Value"]
        params = json.loads(param_string)
    except ClientError as e:
        print(f"WARNING: Unable to retrieve parameters from aws: {e.response}")
        params = None
    finally:
        ssm_client.close()
    return params


def download_secrets_from_aws(secrets_name):
    """Attempt to download the endpoints from an aws secrets manager"""
    sm_client = boto3.client("secretsmanager")
    try:
        secret_from_aws = sm_client.get_secret_value(SecretId=secrets_name)
        secret_as_string = secret_from_aws["SecretString"]
        secrets = json.loads(secret_as_string)
    except ClientError as e:
        print(f"WARNING: Unable to retrieve parameters from aws: {e.response}")
        secrets = None
    finally:
        sm_client.close()
    return secrets


def get_list_of_new_files_to_upload():
    if system_platform.system() == "Windows":
        shell = True
    else:
        shell = False
    latest_commit_id = str(
        subprocess.run(
            ["git", "log", "-1", "--pretty=format:%h"],
            stdout=subprocess.PIPE,
            shell=shell,
        ).stdout.decode("utf-8")
    )
    latest_commit_timestamp = int(
        subprocess.run(
            ["git", "show", "-s", "--format=%ct", latest_commit_id],
            stdout=subprocess.PIPE,
            shell=shell,
        ).stdout.decode("utf-8")
    )
    author_from_commit = str(
        subprocess.run(
            ["git", "show", "-s", "--format=%an", latest_commit_id],
            stdout=subprocess.PIPE,
            shell=shell,
        ).stdout.decode("utf-8")
    )
    datetime_from_commit = datetime.utcfromtimestamp(latest_commit_timestamp)
    files_in_last_commit = str(
        subprocess.run(
            ["git", "log", "-1", "--pretty=oneline", "--name-status"],
            stdout=subprocess.PIPE,
            shell=shell,
        ).stdout.decode("utf-8")
    )
    files_in_last_commit_list = [
        f
        for f in files_in_last_commit.split("\n")
        if f.startswith("A") or f.startswith("M")
    ]
    curation_files_added = set()
    platform_abbreviations = list(Platform.abbreviation_map.keys())
    commit_pattern = re.compile(r"^[AM]\s+([\w-]+)/")
    for line in files_in_last_commit_list:
        print(f"R: {re.match(commit_pattern, line)}")
        if (
            re.match(commit_pattern, line)
            and re.match(commit_pattern, line).group(1).split("_")[0]
            in platform_abbreviations
        ):
            root_folder = re.match(commit_pattern, line).group(1)
            if "curation" in line:
                curation_files_added.add(line[line.find(root_folder) :])

    return author_from_commit, datetime_from_commit, curation_files_added


def get_list_of_all_files_to_upload():
    if system_platform.system() == "Windows":
        shell = True
    else:
        shell = False
    root_folder = Path(__file__).parent.parent

    curation_files = [p for p in root_folder.glob("**/*.json") if "curation" in str(p)]
    authors_from_commit = []
    datetimes_from_commit = []
    curation_files_to_upload = [p.relative_to(root_folder) for p in curation_files]

    for curation_file in curation_files:
        latest_commit_id = str(
            subprocess.run(
                ["git", "log", "-1", "--pretty=format:%h", str(curation_file)],
                stdout=subprocess.PIPE,
                shell=shell,
            ).stdout.decode("utf-8")
        )
        latest_commit_timestamp = int(
            subprocess.run(
                ["git", "show", "-s", "--format=%ct", latest_commit_id],
                stdout=subprocess.PIPE,
                shell=shell,
            ).stdout.decode("utf-8")
        )
        author_from_commit = str(
            subprocess.run(
                ["git", "show", "-s", "--format=%an", latest_commit_id],
                stdout=subprocess.PIPE,
                shell=shell,
            ).stdout.decode("utf-8")
        )
        datetime_from_commit = datetime.utcfromtimestamp(latest_commit_timestamp)
        authors_from_commit.append(author_from_commit)
        datetimes_from_commit.append(datetime_from_commit)

    return authors_from_commit, datetimes_from_commit, curation_files_to_upload


def upload_derived_data_contents_to_s3(
    path_to_curated_file: Path,
    s3_bucket: str,
    author_from_commit: str,
    datetime_from_commit: Optional[datetime] = None,
    dryrun=None,
):
    process_name = path_to_curated_file.stem.replace("curation", "curated").replace(
        "_", "-"
    )
    author_from_commit = author_from_commit.replace("\n", "")
    path_to_curated_dir = list(path_to_curated_file.parents)[-2]
    creation_datetime = (
        datetime.utcnow() if datetime_from_commit is None else datetime_from_commit
    )
    modality = [Modality.ECEPHYS]
    investigators = [
        PIDName(
            name=_INVESTIGATORS_GH_TO_NAME_MAP.get(
                author_from_commit, author_from_commit
            )
        )
    ]
    print(f"Investigators: {investigators}")
    institution = Organization.AIND
    m = re.match(f"{DataRegex.RAW.value}", path_to_curated_dir.name)
    platform = m.group(1)
    subject_id = m.group(2)
    funding_source = [Funding(funder=Organization.AI)]

    derived_data = DerivedDataDescription(
        creation_time=creation_datetime,
        process_name=process_name,
        input_data_name=path_to_curated_dir.name,
        modality=modality,
        platform=Platform.from_abbreviation(platform),
        institution=institution,
        subject_id=subject_id,
        investigators=investigators,
        funding_source=funding_source,
    )

    new_path_name_suffix = build_data_name(
        label=process_name, creation_datetime=creation_datetime
    )
    s3_prefix = path_to_curated_dir.name + f"_{new_path_name_suffix}"

    with tempfile.TemporaryDirectory() as td:
        files_to_upload_path = Path(td) / "files_to_upload"
        (files_to_upload_path / path_to_curated_file).parent.mkdir(
            parents=True, exist_ok=True
        )
        shutil.copyfile(
            path_to_curated_file, files_to_upload_path / path_to_curated_file
        )
        derived_data_filename = derived_data.default_filename()
        output_file_name = os.path.join(files_to_upload_path, derived_data_filename)
        with open(output_file_name, "w") as f:
            json.dump(json.loads(derived_data.model_dump_json()), f, indent=3)
        if system_platform.system() == "Windows":
            shell = True
        else:
            shell = False
        aws_dest = f"s3://{s3_bucket}/{s3_prefix}"
        base_command = ["aws", "s3", "sync", files_to_upload_path, aws_dest]
        if dryrun:
            base_command.append("--dryrun")
            print(
                f"Dry-run set to true. Would have tried to upload "
                f"{files_to_upload_path} to {aws_dest}"
            )
        subprocess.run(base_command, shell=shell)
    return s3_prefix, subject_id, platform


def register_to_codeocean(
    co_client: CodeOceanClient,
    capsule_id: str,
    s3_bucket: str,
    s3_prefix: str,
    subject_id: str,
    platform_abbr: str,
):

    # For legacy purposes, the custom metadata still needs the experiment_type
    # key
    custom_metadata = {
        "modality": "Extracellular electrophysiology",
        "experiment type": platform_abbr,
        "data level": DataLevel.DERIVED.value,
        "subject id": subject_id,
    }
    tags = ["ecephys", subject_id, "curated", platform_abbr, DataLevel.DERIVED.value]

    co_job_params = {
        "trigger_codeocean_job": {
            "job_type": "register_data",
            "capsule_id": capsule_id,
            "bucket": s3_bucket,
            "prefix": s3_prefix,
            "tags": tags,
            "custom_metadata": custom_metadata,
        }
    }

    run_capsule_request = RunCapsuleRequest(
        capsule_id=capsule_id,
        parameters=[json.dumps(co_job_params)],
    )

    run_response = co_client.run_capsule(request=run_capsule_request)
    print(run_response.json())

    return None
