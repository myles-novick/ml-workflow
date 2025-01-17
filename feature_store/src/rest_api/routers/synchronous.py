from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from shared.api.auth_dependency import authenticate
from shared.api.decorators import managed_transaction
from shared.api.exceptions import ExceptionCodes, SpliceMachineException
from shared.db.connection import SQLAlchemyClient
from shared.logger.logging_config import logger

import crud
import schemas
from utils.airflow_utils import Airflow
from utils.feature_utils import (_alter_feature_set, _create_feature_set,
                                   _deploy_feature_set, _get_feature_sets,
                                   _update_feature_set, delete_feature_set,
                                   drop_feature_set_table)
from utils.pipeline_utils.pipeline_utils import (_alter_pipe, _create_pipe, _update_pipe, 
                                                    _update_pipeline, _alter_pipeline,
                                                    _undeploy_pipeline, _deploy_pipeline, 
                                                    _create_pipeline)
from utils.training_utils import (_get_training_set,
                                    _get_training_set_from_view,
                                    _get_training_view_by_name, dict_to_lower,
                                    register_training_set,
                                    training_set_to_json)
from utils.utils import parse_version

# Synchronous API Router -- we can mount it to the main API
SYNC_ROUTER = APIRouter(
    dependencies=[Depends(authenticate)]
)

DB_SESSION = Depends(SQLAlchemyClient.get_session)


@SYNC_ROUTER.get('/feature-sets', status_code=status.HTTP_200_OK, response_model=List[schemas.FeatureSetDetail],
                 description="Returns a list of available feature sets", operation_id='get_feature_sets',
                 tags=['Feature Sets'])
@managed_transaction
def get_feature_sets(names: Optional[List[str]] = Query([], alias="name"),
                     db: Session = DB_SESSION):
    """
    Returns a list of available feature sets
    """
    return _get_feature_sets(names, db)


@SYNC_ROUTER.get('/feature-sets/{name}', status_code=status.HTTP_200_OK, response_model=schemas.FeatureSetDetail,
                 description="Returns a specific feature set if it exists", operation_id='get_feature_set',
                 tags=['Feature Sets'])
@managed_transaction
def get_feature_set(name: str, version: Optional[Union[int, str]] = None, db: Session = DB_SESSION):
    """
    Returns a specific feature set if it exists
    """
    version = parse_version(version)

    crud.validate_schema_table([name])
    schema, table = name.split('.')
    _filter = {'table_name': table, 'schema_name': schema}
    if version:
        _filter['feature_set_version'] = version
    fsets = crud.get_feature_sets(db, _filter=_filter)
    if not fsets:
        v = f"with version '{version}' " if version and version != 'latest' else ""
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"Feature Set {schema}.{table} {v}does not exist. Please enter "
                                             f"a valid feature set.")
    return fsets[0]


@SYNC_ROUTER.get('/summary', status_code=status.HTTP_200_OK, response_model=schemas.FeatureStoreSummary,
                 description="Returns feature store summary metrics", operation_id='get_summary',
                 tags=['Feature Store'])
@managed_transaction
def get_summary(db: Session = DB_SESSION):
    """
    This function returns a summary of the feature store including:
        * Number of feature sets
        * Number of deployed feature sets
        * Number of features
        * Number of deployed features
        * Number of training sets
        * Number of training views
        * Number of associated models - this is a count of the MLManager.RUNS table where the `splice.model_name` tag is set and the `splice.feature_store.training_set` parameter is set
        * Number of active (deployed) models (that have used the feature store for training)
        * Number of pending feature sets - this will will require a new table `featurestore.pending_feature_set_deployments` and it will be a count of that
        * 5 Most newly added features
        * 5 Most used features (across deployments)
    """
    return crud.get_fs_summary(db)


@SYNC_ROUTER.get('/training-views', status_code=status.HTTP_200_OK, response_model=List[schemas.TrainingViewDetail],
                 description="Returns a list of all available training views", operation_id='get_training_views',
                 tags=['Training Views'])
@managed_transaction
def get_training_views(db: Session = DB_SESSION):
    """
    Returns a list of all available training views
    """
    return crud.get_training_views(db)


@SYNC_ROUTER.get('/training-views/{name}', status_code=status.HTTP_200_OK,
                 response_model=List[schemas.TrainingViewDetail],
                 description="Returns a list of all available training views with an optional filter",
                 operation_id='get_training_views', tags=['Training Views'])
@managed_transaction
def get_training_view(name: str, version: Optional[Union[str, int]] = 'latest', db: Session = DB_SESSION):
    """
    Returns a specified training view with an optional version filter
    """
    version = parse_version(version)

    return _get_training_view_by_name(db, name, version)


@SYNC_ROUTER.get('/training-view-id', status_code=status.HTTP_200_OK, response_model=int,
                 description="Returns the unique view ID from a name", operation_id='get_training_view_id',
                 tags=['Training Views'])
@managed_transaction
def get_training_view_id(name: str, db: Session = DB_SESSION):
    """
    Returns the unique view ID from a name
    """
    vid = crud.get_training_view_id(db, name)
    if not vid:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"Training View {name} does not exist. Please enter a valid Training View.")
    return vid


@SYNC_ROUTER.get('/features', status_code=status.HTTP_200_OK, response_model=List[schemas.FeatureDetail],
                 description="Returns a list of all (or the specified) features and details",
                 operation_id='get_features', tags=['Features'])
@managed_transaction
def get_features_by_name(names: List[str] = Query([], alias="name"), db: Session = DB_SESSION):
    """
    Returns a list of features whose names are provided

    """
    return crud.get_feature_descriptions_by_name(db, names)


search_description = """
An advanced, server side search of Features. There are particular rules for each parameter:
* name: (dictionary) The key can be ('is' or 'like'). Can one contain 1 key and 1 value
* tags: A list of strings. The search will look for each individual tag (exact search, not fuzzy match)
* attributes: A dictionary of key:value pairs. It will match exact key/value attributes, no fuzzy matching
* feature_data_type: (string) Must be a valid database type
* feature_type: (string) must be one of ('C','O','N') 
* schema_name: (dictionary) The key can be ('is' or 'like'). Can only contain 1 key and 1 value
* table_name: (dictionary) The key can be ('is' or 'like'). Can only contain 1 key and 1 value
* deployed: boolean
* last_update_username: (dictionary) The key can be ('is' or 'like'). Can only contain 1 key and 1 value
* last_update_ts: (dictionary) for comparing dates, must be one of ('lt', 'lte', 'eq', 'gt', 'gte'). Can have multiple
"""


@SYNC_ROUTER.post('/feature-search', status_code=status.HTTP_200_OK, response_model=List[schemas.FeatureDetail],
                  description=search_description, operation_id='feature_search', tags=['Features'])
@managed_transaction
def feature_search(fs: schemas.FeatureSearch, db: Session = DB_SESSION):
    """
    Returns a list of features who fall into the search criteria
    """
    return crud.feature_search(db, fs)


@SYNC_ROUTER.get('/feature-exists', status_code=status.HTTP_200_OK, response_model=bool,
                 description="Returns whether or not a feature name exists", operation_id='feature_exists',
                 tags=['Features'])
@managed_transaction
def feature_exists(name: str, db: Session = DB_SESSION):
    """
    Returns whether or not a given feature exists

    """
    return bool(crud.get_feature_descriptions_by_name(db, [name]))


@SYNC_ROUTER.get('/feature-details', status_code=status.HTTP_200_OK, response_model=schemas.FeatureDetail,
                 description="Returns Feature Details for a single feature", operation_id='get_feature_details',
                 tags=['Features'])
@managed_transaction
def get_features_details(name: str, db: Session = DB_SESSION):
    """
    Returns a list of features whose names are provided. Same as /features but we keep it here for continuity with other
    routes (Feature Sets and Training Views have a "details" route
    """
    dets = crud.get_feature_descriptions_by_name(db, [name])
    if not dets:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"Feature {name} does not exist. Please enter a valid feature.")
    return dets[0]


@SYNC_ROUTER.post('/feature-vector', status_code=status.HTTP_200_OK, response_model=Union[Dict[str, Any], str],
                  description="Gets a feature vector given a list of Features and primary key values for their corresponding Feature Sets",
                  operation_id='get_feature_vector', tags=['Features'])
@managed_transaction
def get_feature_vector(fjk: schemas.FeatureJoinKeys, pks: bool = True, sql: bool = False, db: Session = DB_SESSION):
    """
    Gets a feature vector given a list of Features and primary key values for their corresponding Feature Sets
    """
    feats: List[schemas.FeatureDetail] = crud.process_features(db, fjk.features)

    not_live = [f.name for f in feats if not f.deployed]
    if not_live:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message=f"Some of the provided features are not live: {not_live}. Either deploy a new version "
                                             "of your feature set that includes these features, or remove them from your query.")
    # Match the case of the keys
    join_keys = dict_to_lower(fjk.join_key_values)

    # Get the feature sets and their primary key column names
    feature_sets = crud.get_feature_sets(db, [f.feature_set_id for f in feats])
    crud.validate_feature_vector_keys(join_keys, feature_sets)

    return crud.get_feature_vector(db, feats, join_keys, feature_sets, pks, sql)


@SYNC_ROUTER.post('/feature-vector-sql', status_code=status.HTTP_200_OK, response_model=str,
                  description="Returns the parameterized feature retrieval SQL used for online model serving.",
                  operation_id='get_feature_vector_sql_from_training_view', tags=['Features'])
@managed_transaction
def get_feature_vector_sql_from_training_view(features: List[Union[schemas.Feature, str]], view: str,
                                              db: Session = DB_SESSION):
    """
    Returns the parameterized feature retrieval SQL used for online model serving.
    """
    feats = crud.process_features(db, features)

    tctx = _get_training_view_by_name(db, view)[0]

    return crud.get_feature_vector_sql(db, feats, tctx)


# @SYNC_ROUTER.get('/feature-primary-keys', status_code=status.HTTP_200_OK, response_model=Dict[str, List[str]],
#                 description="Returns a dictionary mapping each individual feature to its primary key(s).",
#                 operation_id='get_feature_primary_keys', tags=['Features'])
# @managed_transaction
# def get_feature_primary_keys(features: List[str] = Query([], alias="feature"), db: Session = DB_SESSION):
#     """
#     Returns a dictionary mapping each individual feature to its primary key(s). This function is not yet implemented.
#     """
#     pass

@SYNC_ROUTER.get('/training-view-features', status_code=status.HTTP_200_OK, response_model=List[schemas.Feature],
                 description="Returns the available features for the given a training view name",
                 operation_id='get_training_view_features', tags=['Training Views'])
@managed_transaction
def get_training_view_features(view: str, version: Union[str, int] = 'latest', db: Session = DB_SESSION):
    """
    Returns the available features for the given a training view name
    """
    version = parse_version(version)
    tv = _get_training_view_by_name(db, view, version)[0]
    return crud.get_training_view_features(db, tv)


@SYNC_ROUTER.post('/training-sets', status_code=status.HTTP_200_OK, response_model=schemas.TrainingSet,
                  description="Gets a set of feature values across feature sets that is not time dependent (ie for non time series clustering)",
                  operation_id='get_training_set', tags=['Training Sets'])
@managed_transaction
def get_training_set(ftf: schemas.FeatureTimeframe, current: bool = False, label: str = None,
                     return_pk_cols: bool = Query(False, alias='pks'), return_ts_col: bool = Query(False, alias='ts'),
                     save_as: Optional[str] = None, return_type: Optional[str] = None, db: Session = DB_SESSION):
    """
    Gets a set of feature values across feature sets that is not time dependent (ie for non time series clustering).
    This feature dataset will be treated and tracked implicitly the same way a training_dataset is tracked from
    :py:meth:`features.FeatureStore.get_training_set` . The dataset's metadata and features used will be tracked in mlflow automatically (see
    get_training_set for more details).

    The way point-in-time correctness is guaranteed here is by choosing one of the Feature Sets as the "anchor" dataset.
    This means that the points in time that the query is based off of will be the points in time in which the anchor
    Feature Set recorded changes. The anchor Feature Set is the Feature Set that contains the superset of all primary key
    columns across all Feature Sets from all Features provided. If more than 1 Feature Set has the superset of
    all Feature Sets, the Feature Set with the most primary keys is selected. If more than 1 Feature Set has the same
    maximum number of primary keys, the Feature Set is chosen by alphabetical order (schema_name, table_name).

    If save_as is set, the training set metadata will be persisted as a TrainingSetInstance record. This enables recreation
    of the exact Training Set data as an Ad-Hoc query
    """
    create_time = crud.get_current_time(db)
    ts: schemas.TrainingSet = _get_training_set(db, ftf.features, create_time, ftf.start_time, ftf.end_time, current,
                                                label, return_pk_cols, return_ts_col, return_type=return_type)
    if save_as:
        ts = register_training_set(db, ts, save_as)

    if ts.data:
        ts: JSONResponse = training_set_to_json(ts)

    return ts


@SYNC_ROUTER.post('/training-set-from-view', status_code=status.HTTP_200_OK, response_model=schemas.TrainingSet,
                  description="Returns the training set as a Spark Dataframe from a Training View",
                  operation_id='get_training_set_from_view', tags=['Training Sets'])
@managed_transaction
def get_training_set_from_view(view: str, ftf: schemas.FeatureTimeframe,
                               return_pk_cols: bool = Query(False, alias='pks'),
                               return_ts_col: bool = Query(False, alias='ts'), save_as: Optional[str] = None,
                               return_type: Optional[str] = None, db: Session = DB_SESSION):
    """
    Returns the training set as a Spark Dataframe from a Training View. When a user calls this function (assuming they have registered
    the feature store with mlflow using :py:meth:`~mlflow.register_feature_store` )
    the training dataset's metadata will be tracked in mlflow automatically. The following will be tracked:
    including:
        * Training View
        * Selected features
        * Start time
        * End time
    This tracking will occur in the current run (if there is an active run)
    or in the next run that is started after calling this function (if no run is currently active).
    """
    create_time = crud.get_current_time(db)

    ts = _get_training_set_from_view(db, view, create_time, ftf.features, ftf.start_time, ftf.end_time,
                                     return_pk_cols, return_ts_col, return_type=return_type)

    if save_as:
        ts = register_training_set(db, ts, save_as)

    if ts.data:
        ts = training_set_to_json(ts)

    return ts


@SYNC_ROUTER.get('/training-sets/{name}', status_code=status.HTTP_200_OK, response_model=schemas.TrainingSet,
                 description="Returns the training set as a Spark Dataframe from an EXISTING training set",
                 operation_id='get_training_set_by_name', tags=['Training Sets'])
@managed_transaction
def get_training_set_by_name(name: str, version: Optional[int] = None, return_pk_cols: bool = Query(False, alias='pks'),
                             return_ts_col: bool = Query(False, alias='ts'), return_type: Optional[str] = None,
                             db: Session = DB_SESSION):
    """
    Gets an EXISTING training set by name. Returns the Training Set that exists. If no version is specified, the most
    recent version (largest version ID) will be returned.
    """

    tsm: schemas.TrainingSetMetadata = crud.get_training_set_instance_by_name(db, name, version=version)

    if not tsm:
        v = f'{name}, version {version}' if version else f'{name}'
        err = f"Training Set {v} does not exist. Please enter a valid training set."
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND,
                                     code=ExceptionCodes.DOES_NOT_EXIST, message=err)

    features: List[schemas.Feature] = crud.get_features_by_id(db, [int(i) for i in tsm.features.split(',')])
    if tsm.view_id and tsm.view_version:
        view: schemas.TrainingView = \
            crud.get_training_views(db, _filter={'view_id': tsm.view_id, 'view_version': tsm.view_version})[0]
        ts: schemas.TrainingSet = _get_training_set_from_view(
            db, view.name, tsm.training_set_create_ts, features, tsm.training_set_start_ts,
            tsm.training_set_end_ts, return_pk_cols, return_ts_col, return_type=return_type
        )
    else:
        ts: schemas.TrainingSet = _get_training_set(
            db, features, tsm.training_set_create_ts, tsm.training_set_start_ts, tsm.training_set_end_ts,
            current=False, label=tsm.label, return_pk_cols=return_pk_cols, return_ts_col=return_ts_col,
            return_type=return_type
        )

    if ts.data:
        ts: JSONResponse = training_set_to_json(ts)

    return ts


list_ts_desc = """
Returns information about all Training Sets:
* Name
* View ID
* Training Set ID
* Last Update TS
* Last Update Username
* label column (if one exists)

If a Training View is provided (optional) it will return only training sets created from that Training View
"""


@SYNC_ROUTER.get('/training-sets', status_code=status.HTTP_200_OK, response_model=List[schemas.TrainingSetMetadata],
                 description=list_ts_desc,
                 operation_id='list_training_sets', tags=['Training Sets'])
@managed_transaction
def list_training_sets(training_view: str = None, db: Session = DB_SESSION):
    """
    Returns information about all Training Sets:
    * Name
    * View ID
    * Training Set ID
    * Last Update TS
    * Last Update Username
    * label column (if one exists)
    """
    view_id = None
    if training_view:
        view_id = crud.get_training_view_id(db, training_view)
        if not view_id:
            raise SpliceMachineException(message=f'Cannot find view {training_view}',
                                         code=ExceptionCodes.DOES_NOT_EXIST, status_code=status.HTTP_404_NOT_FOUND)
    return crud.list_training_sets(db, tvw_id=view_id)


@SYNC_ROUTER.get('/training-set-details', status_code=status.HTTP_200_OK, response_model=schemas.TrainingSet,
                 description='Returns details about a particular training set instance given a name and version. '
                             'If no version is provided, the newest training set version will be fetched',
                 operation_id='get_training_set_details', tags=['Training Sets'])
@managed_transaction
def get_training_set_details(name: str, version: int = None, db: Session = DB_SESSION):
    """
    Returns training set instance details for a given version
    """
    tsi: schemas.TrainingSetMetadata = crud.get_training_set_instance_by_name(db, name, version)
    if not tsi:
        err = f'Cannot find Training Set {name}'
        if version: err += f' of version {version}'
        raise SpliceMachineException(message=err,
                                     code=ExceptionCodes.DOES_NOT_EXIST, status_code=status.HTTP_404_NOT_FOUND)
    # This returns features as a comma seperated string of IDs, we want the features
    ids = [int(id_) for id_ in tsi.features.split(',')]
    tvws = crud.get_training_views(db, _filter={'view_id': tsi.view_id, 'view_version': tsi.view_version})
    ts = schemas.TrainingSet(
        sql='',  # Don't need the SQL for the UI
        training_view=tvws[0] if tvws else None,  # This training set may not have a view
        features=crud.get_features_by_id(db, ids),
        metadata=tsi
    )
    return ts


@SYNC_ROUTER.post('/feature-sets', status_code=status.HTTP_201_CREATED, response_model=schemas.FeatureSetDetail,
                  description="Creates and returns a new feature set", operation_id='create_feature_set',
                  tags=['Feature Sets'])
@managed_transaction
def create_feature_set(fset: schemas.FeatureSetCreate, db: Session = DB_SESSION):
    """
    Creates and returns a new feature set
    """
    return _create_feature_set(fset, db)


@SYNC_ROUTER.put('/feature-sets/{name}', status_code=status.HTTP_201_CREATED, response_model=schemas.FeatureSetDetail,
                 description="Creates a new version of an existing feature set", operation_id='update_feature_set',
                 tags=['Feature Sets'])
@managed_transaction
def update_feature_set(name: str, fset: schemas.FeatureSetUpdate, db: Session = DB_SESSION):
    """
    Creates a new version of an existing feature set
    """
    crud.validate_schema_table([name])
    schema, table = name.split('.')
    return _update_feature_set(fset, schema, table, db)


@SYNC_ROUTER.patch('/feature-sets/{name}', status_code=status.HTTP_201_CREATED, response_model=schemas.FeatureSet,
                   description="Updates an existing feature set", operation_id='alter_feature_set',
                   tags=['Feature Sets'])
@managed_transaction
def alter_feature_set(name: str, fset: schemas.FeatureSetAlter, version: Union[str, int] = 'latest',
                      db: Session = DB_SESSION):
    """
    Alters an existing feature set version
    """
    version = parse_version(version)

    crud.validate_schema_table([name])
    schema, table = name.split('.')
    return _alter_feature_set(fset, schema, table, version, db)


@SYNC_ROUTER.get('/feature-set-exists', status_code=status.HTTP_200_OK, response_model=bool,
                 description="Returns whether or not a feature set exists", operation_id='feature_set_exists',
                 tags=['Feature Set'])
@managed_transaction
def feature_set_exists(schema: str, table: str, db: Session = DB_SESSION):
    """
    Returns whether or not the provided feature set exists
    """

    return bool(crud.get_feature_sets(db, feature_set_names=[f'{schema}.{table}']))


@SYNC_ROUTER.post('/features', status_code=status.HTTP_201_CREATED, response_model=schemas.Feature,
                  description="Add a feature to a feature set", operation_id='create_feature', tags=['Features'])
@managed_transaction
def create_feature(fc: schemas.FeatureCreate, schema: str, table: str, db: Session = DB_SESSION):
    """
    Add a feature to a feature set
    """
    fsets: List[schemas.FeatureSetDetail] = crud.get_feature_sets(db,
                                                                  _filter={'table_name': table, 'schema_name': schema,
                                                                           'feature_set_version': 'latest'})
    if not fsets:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"Feature Set {schema}.{table} does not exist. Please enter "
                                             f"a valid feature set.")
    fset = fsets[0]
    if fset.deployed:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_DEPLOYED,
                                     message=f"Feature Set {schema}.{table} is already deployed. You cannot "
                                             f"add features to a deployed feature set. If you wish to create a new version "
                                             "of this feature set with different features, you can do so with fs.update_feature_set()")
    crud.validate_feature(db, fc.name, fc.feature_data_type)
    fc.feature_set_id = fset.feature_set_id
    logger.info(f'Registering feature {fc.name} in Feature Store')
    feature = crud.register_feature_metadata(db, fc)
    crud.register_feature_version(db, feature, fset.feature_set_id, fset.feature_set_version)
    return feature


@SYNC_ROUTER.post('/training-views', status_code=status.HTTP_201_CREATED, response_model=schemas.TrainingViewDetail,
                  description="Registers a training view for use in generating training SQL",
                  operation_id='create_training_view', tags=['Training Views'])
@managed_transaction
def create_training_view(tv: schemas.TrainingViewCreate, db: Session = DB_SESSION):
    """
    Registers a training view for use in generating training SQL
    """
    if not tv.name:
        raise SpliceMachineException(
            status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
            message="Name of training view cannot be None!")

    if len(crud.get_training_views(db, _filter={'name': tv.name})) > 0:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS,
                                     message=f"Training View {tv.name} already exists!")

    crud.validate_training_view(db, tv)
    detail = crud.create_training_view(db, tv)
    return crud.create_training_view_version(db, detail)


@SYNC_ROUTER.put('/training-views/{name}', status_code=status.HTTP_201_CREATED,
                 response_model=schemas.TrainingViewDetail,
                 description="Creates a new version of an existing training view",
                 operation_id='update_training_view', tags=['Training Views'])
@managed_transaction
def update_training_view(name: str, tv: schemas.TrainingViewUpdate, db: Session = DB_SESSION):
    """
    Creates a new version of an already existing training view for use in generating training SQL
    """
    views = crud.get_training_views(db, _filter={'name': name})
    if not views:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"Training View {name} does not exist. Please create it, or enter "
                                             "an existing training view.")

    view = views[0]

    crud.validate_training_view(db, tv)

    if tv.description:
        logger.info(f'Updating description for {name}')
        crud.update_feature_set_description(db, view.view_id, tv.description)

    view.__dict__.update(tv.__dict__)
    return crud.create_training_view_version(db, view, view.view_version + 1)


@SYNC_ROUTER.patch('/training-views/{name}', status_code=status.HTTP_201_CREATED,
                   response_model=schemas.TrainingViewDetail,
                   description="Updates an unreferenced version of a training view",
                   operation_id='alter_training_view', tags=['Training Views'])
@managed_transaction
def alter_training_view(name: str, tv: schemas.TrainingViewAlter, version: Optional[Union[str, int]] = 'latest',
                        db: Session = DB_SESSION):
    """
    Updates an unreferenced version of a training view
    """
    version = parse_version(version)

    views = crud.get_training_views(db, _filter={'name': name, 'view_version': version})
    if not views:
        v = f'with version {version}' if version != 'latest' else ''
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"Training View {name} {v}does not exist. Please create it, or enter "
                                             "an existing training view.")

    view = views[0]

    changes = {k: v for k, v in tv.__dict__.items() if v is not None}
    desc = changes.pop('description', None)

    if changes:
        tsets = crud.get_training_sets_from_view(db, view)
        if tsets:
            tsets = [tset[1] for tset in tsets]
            raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.DEPENDENCY_CONFLICT,
                                         message=f'The training view {name} cannot be altered because the following training '
                                                 f'sets are using it: {tsets}')
        new = schemas.TrainingViewDetail(sql_text=view.sql_text, ts_column=view.ts_column, pk_columns=view.pk_columns)
        new.__dict__.update((k, view.__dict__[k]) for k in new.__dict__.keys() & view.__dict__.keys())
        new.__dict__.update((k, changes[k]) for k in new.__dict__.keys() & changes.keys())

        crud.validate_training_view(db, new)
        crud.alter_training_view_version(db, new)
        crud.alter_training_view_keys(db, new)
    if desc:
        crud.update_training_view_description(db, view.view_id, desc)
        new.description = desc
    return new


@SYNC_ROUTER.get('/training-view-exists', status_code=status.HTTP_200_OK, response_model=bool,
                 description="Returns whether or not a training view exists", operation_id='training_view_exists',
                 tags=['Training Views'])
@managed_transaction
def training_view_exists(name: str, db: Session = DB_SESSION):
    """
    Returns whether or not a given training view exists
    """
    return bool(crud.get_training_view_id(db, name))


@SYNC_ROUTER.post('/deploy-feature-set', status_code=status.HTTP_200_OK, response_model=schemas.FeatureSet,
                  description="Deploys a feature set to the database", operation_id='deploy_feature_set',
                  tags=['Feature Sets'])
@managed_transaction
def deploy_feature_set(schema: str, table: str, version: Union[str, int] = 'latest', migrate: bool = False,
                       db: Session = DB_SESSION):
    """
    Deploys a feature set to the database. This persists the feature stores existence.
    As of now, once deployed you cannot delete the feature set or add/delete features.
    The feature set must have already been created with :py:meth:`~features.FeatureStore.create_feature_set`
    """
    version = parse_version(version)
    return _deploy_feature_set(schema, table, version, migrate, db)


@SYNC_ROUTER.get('/feature-set-details', status_code=status.HTTP_200_OK,
                 response_model=Union[List[schemas.FeatureSetDetail], schemas.FeatureSetDetail],
                 description="Returns details of all feature sets (or 1 if specified), with all features "
                             "in the feature sets and whether the feature set is deployed",
                 operation_id='get_feature_set_details', tags=['Feature Sets'])
@managed_transaction
def get_feature_set_details(schema: Optional[str] = None, table: Optional[str] = None, db: Session = DB_SESSION):
    """
    Returns a description of all feature sets, with all features in the feature sets and whether the feature
    set is deployed
    """
    if schema and table:
        fsets = crud.get_feature_sets(db, feature_set_names=[f'{schema}.{table}'])
        if not fsets:
            raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                         message=f'The feature set {schema}.{table} Does not exist.')
        deps = crud.get_feature_set_dependencies(db, fsets[0])
        fsets[0].has_training_sets = bool(deps.get('training_set'))
        fsets[0].has_deployments = bool(deps.get('model'))
    else:
        fsets = crud.get_feature_sets(db)

    # We need to pop features here because it's set to None in the dictionary. If we don't remove it manually, we
    # Will get an error that we are trying to set 2 different values of features for FeatureSetDetail
    fsets = [
        schemas.FeatureSetDetail(**fset.__dict__, features=fset.__dict__.pop('features') or crud.get_features(db, fset))
        for fset in fsets]

    return fsets if len(fsets) != 1 else fsets[0]


@SYNC_ROUTER.get('/training-view-details', status_code=status.HTTP_200_OK,
                 response_model=Union[List[schemas.TrainingViewDetail], schemas.TrainingViewDetail],
                 description="Returns details of all training views (or the specified one): the ID, name, "
                             "description and optional label",
                 operation_id='get_training_view_details', tags=['Training Views'])
@managed_transaction
def get_training_view_details(name: Optional[str] = None, version: Optional[Union[str, int]] = 'latest',
                              db: Session = DB_SESSION):
    """
    Returns a description of all (or the specified) training views, the ID, name, description and optional label
    """
    version = parse_version(version)

    if name:
        tvws = _get_training_view_by_name(db, name, version)

    else:
        tvws = crud.get_training_views(db)
    descs = []
    for tvw in tvws:
        feats: List[schemas.Feature] = crud.get_training_view_features(db, tvw)
        # Grab the feature set info and their corresponding names (schema.table) for the display table
        feat_sets: List[schemas.FeatureSet] = crud.get_feature_sets(db,
                                                                    feature_set_ids=[f.feature_set_id for f in feats])
        feat_sets: Dict[int, str] = {fset.feature_set_id: f'{fset.schema_name}.{fset.table_name}' for fset in feat_sets}
        fds = list(map(lambda f, feat_sets=feat_sets: schemas.FeatureDetail(**f.__dict__, feature_set_name=feat_sets[
            f.feature_set_id]), feats))
        descs.append(schemas.TrainingViewDetail(**tvw.__dict__, features=fds))

    return descs if len(descs) != 1 else descs[0]


@SYNC_ROUTER.put('/features/{name}', status_code=status.HTTP_200_OK, response_model=schemas.Feature,
                 description="Updates a feature's metadata (description, tags, attributes)",
                 operation_id='set_feature_description', tags=['Features'])
@managed_transaction
def update_feature_metadata(name: str, metadata: schemas.FeatureMetadata, db: Session = DB_SESSION):
    fs = crud.get_feature_descriptions_by_name(db, [name])
    if not fs:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"Feature {name} does not exist. Please enter a valid feature.")
    return crud.update_feature_metadata(db, fs[0].name, desc=metadata.description,
                                        tags=metadata.tags, attributes=metadata.attributes)


@SYNC_ROUTER.get('/training-set-from-deployment', status_code=status.HTTP_200_OK, response_model=schemas.TrainingSet,
                 description="Reads Feature Store metadata to rebuild orginal training data set used for the given deployed model.",
                 operation_id='get_training_set_from_deployment', tags=['Training Sets'])
@managed_transaction
def get_training_set_from_deployment(schema: str, table: str, label: str = None,
                                     return_pk_cols: bool = Query(False, alias='pks'),
                                     return_ts_col: bool = Query(False, alias='ts'),
                                     return_type: Optional[str] = None, db: Session = DB_SESSION):
    """
    Reads Feature Store metadata to rebuild orginal training data set used for the given deployed model.
    """
    # database stores object names in upper case
    metadata = crud.retrieve_training_set_metadata_from_deployment(db, schema, table)
    features = metadata.features.split(',')
    tv_name = metadata.name
    start_time = metadata.training_set_start_ts
    end_time = metadata.training_set_end_ts
    create_time = metadata.training_set_create_ts

    if tv_name:
        ts: schemas.TrainingSet = _get_training_set_from_view(
            db, view=tv_name, create_time=create_time, features=features, start_time=start_time,
            end_time=end_time, return_pk_cols=return_pk_cols, return_ts_col=return_ts_col, return_type=return_type
        )
    else:
        ts: schemas.TrainingSet = _get_training_set(
            db, features=features, create_time=create_time, start_time=start_time, end_time=end_time,
            label=label, return_pk_cols=return_pk_cols, return_ts_col=return_ts_col, return_type=return_type
        )

    ts.metadata = metadata

    if ts.data:
        ts: JSONResponse = training_set_to_json(ts)

    return ts


@SYNC_ROUTER.delete('/features/{name}', status_code=status.HTTP_200_OK, description="Remove a feature",
                    operation_id='remove_feature', tags=['Features'])
@managed_transaction
def remove_feature(name: str, db: Session = DB_SESSION):
    """
    Removes a feature from the Feature Store
    """
    features = crud.get_latest_feature(db, name)
    if not features:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"Feature {name} does not exist. Please enter a valid feature.")
    feature, schema, table, version, deployed = features[0]
    if bool(deployed):
        raise SpliceMachineException(status_code=status.HTTP_406_NOT_ACCEPTABLE, code=ExceptionCodes.ALREADY_DEPLOYED,
                                     message=f"Cannot delete Feature {feature.name} from deployed Feature Set {schema}.{table}. "
                                             "If you wish to create a new version "
                                             "of this feature set with different features, you can do so with fs.update_feature_set()")
    crud.remove_feature(db, feature, version)


@SYNC_ROUTER.delete('/training-views/{name}', status_code=status.HTTP_200_OK, description="Remove a training view",
                    operation_id='remove_training_view', tags=['Training Views'])
@managed_transaction
def remove_training_view(name: str, version: Optional[Union[str, int]] = 'latest', db: Session = DB_SESSION):
    """
    Removes a Training View from the feature store as long as the training view isn't being used in a deployment
    """
    version = parse_version(version)

    tvw: schemas.TrainingViewDetail = _get_training_view_by_name(db, name, version)[0]
    deps = crud.get_training_view_dependencies(db, tvw)
    if deps:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.DEPENDENCY_CONFLICT,
                                     message=f'The training view {name} cannot be deleted because the following models '
                                             f'have been deployed using it: {deps}')
    tsets = crud.get_training_sets_from_view(db, tvw)
    tset_ids = [tset[0] for tset in tsets]
    # Delete the dependent training sets
    crud.delete_training_set_stats(db, set(tset_ids))
    crud.delete_training_set_instances(db, set(tset_ids))
    crud.delete_training_set_features(db, set(tset_ids))
    crud.delete_training_sets(db, set(tset_ids))
    # Delete the training view components (key, version and view)
    crud.delete_training_view_keys(db, tvw)
    crud.delete_training_view_version(db, tvw)


del_fset_desc = """Removes a Feature Set. You can only delete a feature set if there are no training set or model
dependencies. If you set purge=True, dependent training sets will be deleted as well, and you will be able to remove
the feature set. If keep_metadata is true, the feature set table and history table will be dropped, but the metadata
about the feature set (and features) will remain. The feature set's deploy status will be changed from true to false"""


@SYNC_ROUTER.delete('/feature-sets', status_code=status.HTTP_200_OK, description=del_fset_desc,
                    operation_id='remove_feature_set', tags=['Feature Sets'])
@managed_transaction
def remove_feature_set(schema: str, table: str, purge: bool = False,
                       version: Union[str, int] = None, db: Session = DB_SESSION):
    """
    Deletes a feature set if appropriate. You can currently delete a feature set in two scenarios:
    1. The feature set has not been deployed
    2. The feature set has been deployed, but not linked to any training sets/model deployments

    :param db: SQLAlchemy Session
    :return: None
    """
    version = parse_version(version)

    schema = schema.upper()
    table = table.upper()

    fsets = crud.get_feature_sets(db,
                                  _filter={'table_name': table, 'schema_name': schema, 'feature_set_version': version})
    if not fsets:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f'The feature set ({schema}.{table}) you are trying to delete has not '
                                             'been created. Please ensure the feature set exists.')
    err = ''
    models = {}
    training_sets = {}
    for fset in fsets:
        deps = crud.get_feature_set_dependencies(db, fset)
        if deps['model']:
            models[fset.feature_set_version] = deps["model"]
        elif deps['training_set']:
            training_sets[fset.feature_set_version] = deps['training_set']
    if models:
        err += f'''You cannot remove a Feature Set that has an associated model deployment. 
        The Following Feature Set Versions have these models as dependents: {models}\n'''
    if training_sets and not purge:
        err += f'''You cannot remove a Feature Set that has associated Training Sets. 
        The following Feature Set Versions have these Training Sets as dependents: {training_sets}. 
        To drop these Feature Set Versions anyway, set purge=True (be careful! the training sets will be deleted)'''
    if err:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.DEPENDENCY_CONFLICT,
                                     message=err)
    # No dependencies
    if training_sets:
        sets = set(training_sets.values())
        delete_feature_set(db, fset.feature_set_id, version, purge=purge, training_sets=sets)
    else:
        delete_feature_set(db, fset.feature_set_id, version, purge=purge)
    for fset in fsets:
        table_name = fset.versioned_table
        drop_feature_set_table(db, fset.schema_name, table_name)
        if Airflow.is_active:
            fset_name = f'{fset.schema_name}.{table_name}'
            Airflow.unschedule_feature_set_calculation(fset_name)


@SYNC_ROUTER.get('/deployments', status_code=status.HTTP_200_OK, response_model=List[schemas.DeploymentDetail],
                 description="Get all deployments given either a deployment (schema/table), a training_view (name), "
                             "a feature (feat), or a feature set (fset)", operation_id='get_deployments',
                 tags=['Deployments'])
@managed_transaction
def get_deployments(schema: Optional[str] = None, table: Optional[str] = None, name: Optional[str] = None,
                    feature=Query('', alias='feat'), feature_set=Query('', alias='fset'),
                    version: Optional[Union[str, int]] = None, db: Session = DB_SESSION):
    """
    Returns a list of available deployments. If no parameters are passed in, all deployments are returned.
    Schema and Table can be passed in to get a specific deployment
    name can be passed in as a Training View name, and this will return all deployments from tha Training View
    feat can be passed in and this will return all deployments that use this feature
    fset can be passed in and this will return all deployments that use this feature set.
    You cannot pass in more than 1 of these options (schema+table counting as 1 parameter)
    """
    if schema or table or name:
        _filter = {'model_schema_name': schema, 'model_table_name': table, 'name': name}
        _filter = {k: v for k, v in _filter.items() if v}
        return crud.get_deployments(db, _filter)
    if feature and feature_set:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message='You cannot pass in both a feature set and a feature. Only 1 is allowed')
    if feature:
        f = crud.get_feature_descriptions_by_name(db, [feature])
        if not f:
            raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.DOES_NOT_EXIST,
                                         message=f"Feature {feature} does not exist!")
        return crud.get_deployments(db, feature=f[0])
    if feature_set:
        version = parse_version(version)
        fset = crud.get_feature_sets(db, _filter={'name': feature_set, 'feature_set_version': version})
        if not fset:
            raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.DOES_NOT_EXIST,
                                         message=f"Feature Set {feature_set} does not exist!")
        return crud.get_deployments(db, feature_set=fset[0])
    return crud.get_deployments(db)


@SYNC_ROUTER.get('/training-set-features', status_code=status.HTTP_200_OK, response_model=schemas.DeploymentFeatures,
                 description="Returns a training set and the features associated with it",
                 operation_id='get_training_set_features', tags=['Training Sets'])
@managed_transaction
def get_training_set_features(name: str, db: Session = DB_SESSION):
    """
    Returns a training set and the features associated with it
    """
    crud.validate_schema_table([name])
    schema, table = name.split('.')
    deployments = crud.get_deployments(db,
                                       _filter={'model_schema_name': schema.upper(), 'model_table_name': table.upper()})
    if not deployments:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"Could not find Training Set {schema}.{table}")
    ts = deployments[0]
    features = crud.get_features_from_deployment(db, ts.training_set_id)
    return schemas.DeploymentFeatures(**ts.__dict__, features=features)

@SYNC_ROUTER.get('/pipes', status_code=status.HTTP_200_OK, response_model=List[schemas.PipeDetail],
                 description="Returns a list of available pipes", operation_id='get_pipes', tags=['Pipes'])
@managed_transaction
def get_pipes(names: Optional[List[str]] = Query([], alias="name"), db: Session = DB_SESSION):
    """
    Returns a list of available pipes
    """
    return crud.get_pipes(db, names)


@SYNC_ROUTER.get('/pipes/{name}', status_code=status.HTTP_200_OK, response_model=List[schemas.PipeDetail],
                 description="Returns a list of available pipes", operation_id='get_pipes', tags=['Pipes'])
@managed_transaction
def get_pipe(name: str, version: Optional[Union[str, int]] = None, db: Session = DB_SESSION):
    """
    Returns a list of available pipes
    """
    version = parse_version(version)
    pipes = crud.get_pipes(db, _filter={"name": name, "pipe_version": version})
    if not pipes:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"Cannot find any pipe with name '{name}'.")
    return pipes


@SYNC_ROUTER.post('/pipes', status_code=status.HTTP_201_CREATED, response_model=schemas.PipeDetail,
                  description="Creates and returns a new pipe", operation_id='create_pipe', tags=['Pipes'])
@managed_transaction
def create_pipe(pipe: schemas.PipeCreate, db: Session = DB_SESSION):
    """
    Creates and returns a new pipe
    """
    return _create_pipe(pipe, db)


@SYNC_ROUTER.put('/pipes/{name}', status_code=status.HTTP_201_CREATED, response_model=schemas.PipeDetail,
                 description="Creates a new version of an existing pipe", operation_id='update_pipe', tags=['Pipes'])
@managed_transaction
def update_pipe(name: str, pipe: schemas.PipeUpdate, db: Session = DB_SESSION):
    """
    Creates a new version of an existing pipe
    """
    return _update_pipe(pipe, name, db)


@SYNC_ROUTER.patch('/pipes/{name}', status_code=status.HTTP_201_CREATED, response_model=schemas.PipeDetail,
                   description="Alters an undeployed version of a pipe", operation_id='alter_pipe', tags=['Pipes'])
@managed_transaction
def alter_pipe(name: str, pipe: schemas.PipeAlter, version: Union[str, int] = 'latest', db: Session = DB_SESSION):
    """
    Alters an undeployed version of a pipe
    """
    if not (pipe.description or pipe.func):
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message=f"Either description or func must be set to alter a pipe.")
    version = parse_version(version)
    return _alter_pipe(pipe, name, version, db)


@SYNC_ROUTER.delete('/pipes/{name}', status_code=status.HTTP_200_OK, description="Remove a pipe",
                    operation_id='remove_pipe', tags=['Pipes'])
@managed_transaction
def remove_pipe(name: str, version: Optional[Union[str, int]] = None, db: Session = DB_SESSION):
    """
    Removes a pipe from the Feature Store
    """
    version = parse_version(version)
    pipes = crud.get_pipes(db, _filter={'name': name, 'pipe_version': version})
    if not pipes:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"Pipe {name} does not exist. Please enter a valid pipe.")
    pipe = pipes[0]
    
    pipelines = crud.get_pipelines_from_pipe(db, pipe)
    if pipelines:
        deps = [f'{pipeline.name} v{pipeline.pipeline_version}' for pipeline in pipelines]
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.DEPENDENCY_CONFLICT,
                                    message=f"Cannot remove pipe {name} since it is used by the following pipelines: {deps}")

    crud.delete_pipe(db, pipe.pipe_id, pipe.pipe_version if version == 'latest' else version)

@SYNC_ROUTER.get('/pipelines', status_code=status.HTTP_200_OK, response_model=List[schemas.PipelineDetail],
                description="Returns a list of available pipelines", operation_id='get_pipelines', tags=['Pipelines'])
@managed_transaction
def get_pipelines(names: Optional[List[str]] = Query([], alias="name"), deployed: Optional[bool] = False, db: Session = DB_SESSION):
    """
    Returns a list of available (or, if specified, deployed) pipelines
    """
    if deployed:
        return crud.get_deployed_pipelines(db)
    return crud.get_pipelines(db, names)

@SYNC_ROUTER.get('/pipelines/{name}', status_code=status.HTTP_200_OK, response_model=List[schemas.PipelineDetail],
                description="Returns a list of available pipelines", operation_id='get_pipelines', tags=['Pipelines'])
@managed_transaction
def get_pipeline(name: str, version: Optional[Union[str, int]] = None, db: Session = DB_SESSION):
    """
    Returns a list of available pipeline versions
    """
    version = parse_version(version)
    pipelines = crud.get_pipelines(db, _filter={"name": name, "pipeline_version": version})
    if not pipelines:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                    message=f"Cannot find any pipeline with name '{name}'.")
    return pipelines

@SYNC_ROUTER.post('/pipelines', status_code=status.HTTP_201_CREATED, response_model=schemas.PipelineDetail,
                description="Creates and returns a new pipeline", operation_id='create_pipeline', tags=['Pipelines'])
@managed_transaction
def create_pipeline(pipeline: schemas.PipelineCreate, db: Session = DB_SESSION):
    """
    Creates and returns a new pipeline
    """
    return _create_pipeline(pipeline, db)

@SYNC_ROUTER.put('/pipelines/{name}', status_code=status.HTTP_201_CREATED, response_model=schemas.PipelineDetail,
                description="Creates a new version of an existing pipeline", operation_id='update_pipeline', tags=['Pipelines'])
@managed_transaction
def update_pipeline(name: str, pipeline: schemas.PipelineUpdate, db: Session = DB_SESSION):
    """
    Creates a new version of an existing pipeline
    """
    return _update_pipeline(pipeline, name, db)

@SYNC_ROUTER.patch('/pipelines/{name}', status_code=status.HTTP_201_CREATED, response_model=schemas.PipelineDetail,
                description="Alters an undeployed version of a pipeline", operation_id='alter_pipeline', tags=['Pipelines'])
@managed_transaction
def alter_pipeline(name: str, pipeline: schemas.PipelineAlter, version: Union[str, int] = 'latest', db: Session = DB_SESSION):
    """
    Alters an undeployed version of a pipeline
    """
    version = parse_version(version)
    return _alter_pipeline(pipeline, name, version, db)

@SYNC_ROUTER.delete('/pipelines/{name}', status_code=status.HTTP_200_OK, description="Remove a pipeline",
                    operation_id='remove_pipeline', tags=['Pipelines'])
@managed_transaction
def remove_pipeline(name: str, version: Optional[Union[str, int]] = None, db: Session = DB_SESSION):
    """
    Removes a pipeline from the Feature Store
    """
    version = parse_version(version)
    pipelines = crud.get_pipelines(db, _filter={'name': name, 'pipeline_version': version})
    if not pipelines:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Pipeline {name} does not exist. Please enter a valid pipeline.")
    pipeline = pipelines[0]
    if pipeline.feature_set_id:
        raise SpliceMachineException(status_code=status.HTTP_406_NOT_ACCEPTABLE, code=ExceptionCodes.ALREADY_DEPLOYED,
                                        message=f"Cannot delete Pipeline {name} v{pipeline.pipeline_version} as it is "
                                        "already deployed. Please undeploy it first with fs.undeploy_pipeline()")
    crud.delete_pipeline(db, pipeline.pipeline_id, pipeline.pipeline_version if version == 'latest' else version)

@SYNC_ROUTER.post('/deploy-pipeline', status_code=status.HTTP_200_OK, response_model=schemas.PipelineDetail,
                description="Deploys a pipeline to a specified Feature Set", operation_id='deploy_pipeline', tags=['Pipelines'])
@managed_transaction
def deploy_pipeline(name: str, schema: str, table: str, version: Union[str, int] = 'latest', db: Session = DB_SESSION):
    """
    Deploys a pipeline to a specified Feature Set
    """
    version = parse_version(version)
    return _deploy_pipeline(name, schema, table, version, db)

@SYNC_ROUTER.post('/undeploy-pipeline', status_code=status.HTTP_200_OK, response_model=schemas.PipelineDetail,
                description="Undeploys a pipeline", operation_id='undeploy_pipeline', tags=['Pipelines'])
@managed_transaction
def undeploy_pipeline(name: str, version: Union[str, int] = 'latest', db: Session = DB_SESSION):
    """
    Undeploys a pipeline
    """
    version = parse_version(version)
    return _undeploy_pipeline(name, version, db)
