import argparse
from pathlib import Path

from aind_codeocean_api.codeocean import CodeOceanClient

from upload_utils import (
    upload_derived_data_contents_to_s3,
    register_to_codeocean,
    get_list_of_new_files_to_upload, download_params_from_aws,
    download_secrets_from_aws,
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--s3-bucket", type=str)
    parser.add_argument("-p", "--param-store", type=str)
    parser.add_argument("-s", "--secrets-name", type=str)
    parser.add_argument("--dry-run", action="store_true")
    parser.set_defaults(dry_run=False)
    args = parser.parse_args()

    (
        author_of_commit,
        datetime_of_commit,
        curation_files_added,
    ) = get_list_of_new_files_to_upload()
    print("Datetime of last commit: ", datetime_of_commit)
    print("Author of last commit: ", author_of_commit)
    print("Curation files added in last commit: ", curation_files_added)

    param_store_name = args.param_store
    secrets_name = args.secrets_name
    params = download_params_from_aws(param_store_name)
    secrets = download_secrets_from_aws(secrets_name)
    co_token = secrets["codeocean_api_token"]
    co_domain = params["codeocean_domain"]
    co_client = CodeOceanClient(domain=co_domain, token=co_token)
    capsule_id = params["codeocean_trigger_capsule_id"]

    for curation_file in curation_files_added:
        (
            main_s3_prefix,
            main_subject_id,
            main_platform,
        ) = upload_derived_data_contents_to_s3(
            path_to_curated_file=Path(curation_file),
            s3_bucket=args.s3_bucket,
            datetime_from_commit=datetime_of_commit,
            author_from_commit=author_of_commit,
            dryrun=args.dry_run,
        )
        if args.dry_run is False:
            register_to_codeocean(
                co_client=co_client,
                capsule_id=capsule_id,
                s3_bucket=args.s3_bucket,
                s3_prefix=main_s3_prefix,
                subject_id=main_subject_id,
                platform_abbr=main_platform,
            )
        else:
            print(
                f"Dry-run set to true. Would have tried to register "
                f"s3://{args.s3_bucket}/{main_s3_prefix}"
            )
