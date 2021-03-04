from typing import List, Optional, Dict, Union
from datetime import datetime
from pydantic import BaseModel

class FeatureBase(BaseModel):
    feature_set_id: Optional[int] = None
    name: str
    description: Optional[str] = None
    feature_data_type: str
    feature_type: str
    tags: Optional[List[str]] = None
    attributes: Optional[Dict[str, str]] = None

class FeatureCreate(FeatureBase):
    pass
  
class Feature(FeatureBase):
    feature_id: int
    compliance_level: Optional[int] = None
    last_update_ts: Optional[datetime] = None
    last_update_username: Optional[str] = None
    
    class Config:
        orm_mode = True

class FeatureDescription(Feature):
    feature_set_name: Optional[str] = None

class FeatureSetBase(BaseModel):
    schema_name: str
    table_name: str
    description: Optional[str] = None
    primary_keys: Dict[str, str]

class FeatureSetCreate(FeatureSetBase):
    pass

class FeatureSet(FeatureSetBase):
    feature_set_id: int
    deployed: Optional[bool] = False
    deploy_ts: Optional[datetime] = None

    class Config:
        orm_mode = True

class FeatureSetDescription(FeatureSet):
    features: List[Feature]

class TrainingViewBase(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    pk_columns: List[str]
    ts_column: str
    label_column: Optional[str] = None
    join_columns: Optional[List[str]] = None

class TrainingViewCreate(TrainingViewBase):
    sql_text: str

class TrainingView(TrainingViewBase):
    view_id: int
    view_sql: str

    class Config:
        orm_mode = True

class TrainingViewDescription(TrainingView):
    features: List[FeatureDescription]

# Basically just for neat documentation
class FeatureTimeframe(BaseModel):
    features: Union[List[Feature], List[str]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

class FeatureJoinKeys(BaseModel):
    features: List[Union[str, FeatureDescription]]
    join_key_values: Dict[str, Union[str, int]]

class Deployment(BaseModel):
    model_schema_name: str
    model_table_name: str
    training_set_id: Optional[int] = None
    training_set_start_ts: Optional[datetime] = None
    training_set_end_ts: Optional[datetime] = None
    run_id: Optional[str] = None
    last_update_ts: datetime
    last_update_username: str

    class Config:
        orm_mode = True

class DeploymentDescription(Deployment):
    training_set_name: Optional[str] = None

class DeploymentFeatures(DeploymentDescription):
    features: List[Feature]

class TrainingSetMetadata(BaseModel):
    name: Optional[str] = None
    training_set_start_ts: Optional[datetime] = None
    training_set_end_ts: Optional[datetime] = None
    training_set_create_ts: datetime
    features: Optional[str] = None

    class Config:
        orm_mode = True

class TrainingSet(BaseModel):
    sql: str
    training_view: Optional[TrainingView] = None
    features: List[Feature]
    metadata: Optional[TrainingSetMetadata] = None

class FeatureStoreSummary(BaseModel):
    num_feature_sets: int
    num_deployed_feature_sets: int
    num_features: int
    num_deployed_features: int
    num_training_sets: int
    num_training_views: int
    num_models: int
    num_deployed_models: int
    num_pending_feature_set_deployments: int