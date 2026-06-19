from databricks.sdk.runtime import dbutils
import os
import sys


class EnvParams:
    def __init__(self, domain, layer):
        self.layer = layer

        if domain == "mom_factory":
            self.domain = "mom_factory"
        else:
            self.domain = "engineering"

        self.catalog_name = sys.argv[1]

        scope_name = f"cf_{self.domain}_secrets"
        
        self.env = dbutils.secrets.get(scope=scope_name, key="ENV")
        self.s3_bucket = dbutils.secrets.get(scope=scope_name, key="S3_BUCKET")
        self.project_dir = dbutils.secrets.get(scope=scope_name, key="PROJECT_DIR")

    def get_path(self, param):
        if param == "s3_source_path":
            if self.domain == "mom_factory":
                return f"s3://{self.s3_bucket}/{self.project_dir}/raw/"
            else:
                return f"s3://{self.s3_bucket}/{self.project_dir}/raw/{self.layer}/"

        if param == "schema_location":
            return f"s3://{self.s3_bucket}/{self.project_dir}/checkpoints/{self.layer}/{self.catalog_name}/_schema_metadata"

        if param == "checkpoint_location":
            return f"s3://{self.s3_bucket}/{self.project_dir}/checkpoints/{self.layer}/{self.catalog_name}/"

        return None
