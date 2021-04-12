from .. import crud
from .. import schemas
import shared.models.feature_store_models as models
from sqlalchemy.orm import Session
from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from fastapi import status
from shared.logger.logging_config import logger
from ..utils.airflow_utils import Airflow
from typing import List, Optional
import json
from .utils import sql_to_datatype

def _deploy_feature_set(schema: str, table: str, db: Session):
    """
    Deploys a feature set to the database. This persists the feature stores existence.
    As of now, once deployed you cannot delete the feature set or add/delete features.
    The feature set must have already been created with :py:meth:`~features.FeatureStore.create_feature_set`
    """
    try:
        fset = crud.get_feature_sets(db, _filter={'schema_name': schema, 'table_name': table})[0]
    except:
        raise SpliceMachineException(
            status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
            message=f"Cannot find feature set {schema}.{table}. Ensure you've created this"
            f"feature set using fs.create_feature_set before deploying.")
    if fset.deployed:
        raise SpliceMachineException(
            status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_DEPLOYED,
            message=f"Feature set {schema}.{table} is already deployed.")

    fset = crud.deploy_feature_set(db, fset)
    if Airflow.is_active:
        Airflow.schedule_feature_set_calculation(f'{schema}.{table}')
    return fset

def _create_feature_set(fset: schemas.FeatureSetCreate, db: Session):
    """
    The implementation of the create_feature_set route with logic here so other functions can call it
    :param fset: The feature set schema to create
    :param db: The database session
    :return: The created Feature Set
    """
    crud.validate_feature_set(db, fset)
    logger.info(f'Registering feature set {fset.schema_name}.{fset.table_name} in Feature Store')
    created_fset = crud.register_feature_set_metadata(db, fset)
    if fset.features:
        logger.info("Validating features")
        for fc in fset.features:
            crud.validate_feature(db, fc.name, fc.feature_data_type)
            fc.feature_set_id = created_fset.feature_set_id
        logger.info("Done. Bulk registering features")
        crud.bulk_register_feature_metadata(db, fset.features)
    return created_fset

def _get_feature_sets(names: List[str], db: Session):
    """
    Logic implementation of get_feature_sets route implemented here so other
    functions can call it directly

    :param names: The names of the desired feature sets
    :param db: Session
    :return: List of Feature Sets
    """
    crud.validate_schema_table(names)
    return crud.get_feature_sets(db, feature_set_names=names)

def model_to_schema_feature(feat: models.Feature) -> schemas.Feature:
    """
    A function that converts a models.Feature into a schemas.Feature through simple manipulations.
    Splice Machine does not support complex data types like JSON or Arrays, so we stringify them and store them as
    Strings in the database, so they need some manipulation when we retrieve them.
        * Turns tags into a list
        * Turns attributes into a Dict
        * Turns feature_data_type into a DataType object (dict)
    :param feat: The feature from the database
    :return: The schemas.Feature representation
    """
    f = feat.__dict__
    f['tags'] = f['tags'].split(',') if f.get('tags') else None
    f['attributes'] = json.loads(f['attributes']) if f.get('attributes') else None
    f['feature_data_type'] = sql_to_datatype(f['feature_data_type'])
    return schemas.Feature(**f)