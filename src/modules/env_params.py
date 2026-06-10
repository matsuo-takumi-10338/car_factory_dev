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
            s3_source_path = f"s3://{self.s3_bucket}/{self.project_dir}_{self.env}/raw/"
            return s3_source_path
        
        if param == "schema_location":
            schema_location = f"s3://{self.s3_bucket}/{self.project_dir}_{self.env}/checkpoints/{self.layer}/_schema_metadata"
            return schema_location
        
        if param == "checkpoint_location":
            checkpoint_location = f"s3://{self.s3_bucket}/{self.project_dir}_{self.env}/checkpoints/{self.layer}"
            return checkpoint_location
        
        if param == "table_name":
            table_name = f"car_factory_{self.env}.{self.layer}.car_factory_{self.layer}"
            return table_name

        if param == "data_path":
            data_path = f"s3://{self.s3_bucket}/car_factory_{self.env}/database/{self.layer}/car_factory_{self.layer}"
            return data_path
        
        return None
    
    def get_env_params(self, param):
        if param == "env":
            return self.env
        
        if param == "s3_bucket":
            return self.s3_bucket
        
        if param == "project_dir":
            return self.project_dir