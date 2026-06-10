from databricks.sdk.runtime import dbutils
import os

class EnvParams:
    
    def __init__(self, domain, layer):
        self.layer = layer

        current_path = os.getcwd()
        if "_prod" in current_path:
            self.env = "prod"
        else:
            self.env = "dev"

        if domain == "mom_factory":
            self.domain = "mom_factory"
        else:
            self.domain = "engineering"

        scope_name = f"cf_{self.domain}_secrets_{self.env}"

        self.s3_bucket = dbutils.secrets.get(scope=scope_name, key="s3_bucket")
        self.project_dir = dbutils.secrets.get(scope = scope_name, key = "project_dir")

    def get_path(self, param):

        if param == "s3_source_path":
            if self.domain == "mom_factory":
                return f"s3://{self.s3_bucket}/raw/"
            else:
                return f"s3://{self.s3_bucket}/raw/production_standard/"
        
        if param == "schema_location":
            if self.domain == "mom_factory":
                return f"s3://{self.s3_bucket}/checkpoints/{self.layer}/_schema_metadata"
            else:
                return f"s3://{self.s3_bucket}/checkpoints/production_standard/_schema_metadata"
        
        if param == "checkpoint_location":
            if self.domain == "mom_factory":
                return f"s3://{self.s3_bucket}/checkpoints/{self.layer}/"
            else:
                return f"s3://{self.s3_bucket}/checkpoints/production_standard/"
        
        if param == "table_name":
            if self.domain == "mom_factory":
                return f"cf_{self.env}.{self.layer}.{self.layer}_mom_factory"
            else:
                return f"cf_{self.env}.{self.layer}.{self.layer}_engineering"

        if param == "data_path":
            if self.domain == "mom_factory":
                return f"s3://{self.s3_bucket}/database/{self.layer}/"
            else:
                return f"s3://{self.s3_bucket}/database/production_standard/"
        
        return None
    
    def get_env_params(self, param):
        if param == "env":
            return self.env
        
        if param == "s3_bucket":
            return self.s3_bucket
        
        if param == "project_dir":
            return self.project_dir