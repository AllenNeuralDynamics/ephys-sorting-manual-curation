import argparse
import json
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
from aind_data_schema.models.platforms import Platform
from aind_data_schema.models.organizations import Organization
from aind_data_schema.core.data_description import (
    DataLevel,
    DataRegex,
    DerivedDataDescription,
    Funding,
    Modality,
    build_data_name,
)
from botocore.exceptions import ClientError


def _get_list_of_folders_to_upload():
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
    datetime_from_commit = datetime.utcfromtimestamp(latest_commit_timestamp)
    files_in_last_commit = str(
        subprocess.run(
            ["git", "log", "-1", "--pretty=oneline", "--name-status"],
            stdout=subprocess.PIPE,
            shell=shell,
        ).stdout.decode("utf-8")
    )
    files_in_last_commit_list = files_in_last_commit.split("\n")
    root_folders_added = set()
    platform_abbreviations = list(Platform._abbreviation_map.keys())
    commit_pattern = re.compile(r'^A\s+([\w-]+)/')
    for line in files_in_last_commit_list:
        if re.match(commit_pattern, line) and re.match(commit_pattern, line).group(1).split("_")[0] in platform_abbreviations:
            root_folders_added.add(re.match(commit_pattern, line).group(1))

    return datetime_from_commit, root_folders_added


def _download_params_from_aws(store_name):
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


def _download_secrets_from_aws(secrets_name):
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


def upload_derived_data_contents_to_s3(
    path_to_curated_dir: Path,
    s3_bucket: str,
    datetime_from_commit: Optional[datetime] = None,
    dryrun=None,
):
    process_name = "curated"
    creation_datetime = (
        datetime.utcnow() if datetime_from_commit is None else datetime_from_commit
    )
    modality = [Modality.ECEPHYS]
    institution = Organization.AIND.value
    m = re.match(f"{DataRegex.RAW.value}", path_to_curated_dir.name)
    platform = m.group(1)
    subject_id = m.group(2)
    funding_source = [Funding(funder=Organization.AI)]

    derived_data = DerivedDataDescription(
        creation_time=creation_datetime,
        process_name=process_name,
        input_data_name=path_to_curated_dir.name,
        modality=modality,
        platform=platform,
        institution=institution,
        subject_id=subject_id,
        investigators=[],
        funding_source=funding_source,
    )

    new_path_name_suffix = build_data_name(
        label=process_name,
        creation_datetime=creation_datetime
    )
    s3_prefix = path_to_curated_dir.name + f"_{new_path_name_suffix}"

    with tempfile.TemporaryDirectory() as td:
        files_to_upload_path = os.path.join(td, "files_to_upload")
        shutil.copytree(path_to_curated_dir, files_to_upload_path)
        derived_data_filename = derived_data.default_filename()
        output_file_name = os.path.join(files_to_upload_path, derived_data_filename)
        with open(output_file_name, "w") as f:
            f.write(derived_data.json(indent=3))
        if system_platform.system() == "Windows":
            shell = True
        else:
            shell = False
        aws_dest = f"s3://{s3_bucket}/{s3_prefix}"
        base_command = ["aws", "s3", "sync", files_to_upload_path, aws_dest]
        if dryrun:
            base_command.append("--dryrun")
        subprocess.run(base_command, shell=shell)
    return s3_prefix, subject_id, platform


def register_to_codeocean(
    param_store_name: str,
    secrets_name: str,
    s3_bucket: str,
    s3_prefix: str,
    subject_id: str,
    platform_abbr: str
):
    params = _download_params_from_aws(param_store_name)
    secrets = _download_secrets_from_aws(secrets_name)
    co_token = secrets["codeocean_api_token"]
    co_domain = params["codeocean_domain"]
    capsule_id = params["codeocean_trigger_capsule_id"]
    co_client = CodeOceanClient(domain=co_domain, token=co_token)

    # It'd be nice if these were pulled from an Enum
    custom_metadata = {
        "modality": "Extracellular electrophysiology",
        "experiment type": platform_abbr,
        "data level": DataLevel.DERIVED.value,
        "subject id": subject_id,
    }
    tags = ["ecephys", subject_id, "curated", platform_abbr]

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

    run_response = co_client.run_capsule(
        request=run_capsule_request
    )
    print(run_response.json())

    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--s3-bucket", type=str)
    parser.add_argument("-p", "--param-store", type=str)
    parser.add_argument("-s", "--secrets-name", type=str)
    parser.add_argument("--dry-run", action="store_true")
    parser.set_defaults(dry_run=False)
    args = parser.parse_args()

    datetime_of_commit, folders_added = _get_list_of_folders_to_upload()
    print("Datetime of last commit: ", datetime_of_commit)
    print("Ecephys folders added in last commit: ", folders_added)

    for folder_name in folders_added:
        main_s3_prefix, main_subject_id, main_platform = upload_derived_data_contents_to_s3(
            path_to_curated_dir=Path(folder_name),
            s3_bucket=args.s3_bucket,
            datetime_from_commit=datetime_of_commit,
            dryrun=args.dry_run,
        )
        if args.dry_run is False:
            register_to_codeocean(
                param_store_name=args.param_store,
                secrets_name=args.secrets_name,
                s3_bucket=args.s3_bucket,
                s3_prefix=main_s3_prefix,
                subject_id=main_subject_id,
                platform_abbr=main_platform
            )
        else:
            print(
                f"Dry-run set to true. Would have tried to register "
                f"s3://{args.s3_bucket}/{main_s3_prefix}"
            )
