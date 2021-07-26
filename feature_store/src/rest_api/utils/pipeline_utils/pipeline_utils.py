from typing import List, Union
from fastapi import status
from sqlalchemy.orm import Session

from shared.api.exceptions import ExceptionCodes, SpliceMachineException
from shared.logger.logging_config import logger
from shared.db.connection import SQLAlchemyClient

import crud
import schemas

def create_default_pipes(pipes: schemas.PipeCreate):
    db = SQLAlchemyClient().SessionMaker()
    created = [p.name for p in crud.get_pipes(db, [pipe.name for pipe in pipes])]
    new = filter(lambda x: x.name not in created, pipes)
    for pipe in new:
        _create_pipe(pipe, db)
    # More might come later
    db.commit()
    db.close()

def _create_pipe(pipe: schemas.PipeCreate, db: Session) -> schemas.PipeDetail:
    """
    The implementation of the create_pipe route with logic here so other functions can call it
    :param pipe: The pipe to create
    :param db: The database session
    :return: The created Pipe
    """
    crud.validate_pipe(db, pipe)
    logger.info(f'Registering pipe {pipe.name} in Feature Store')
    pipe_metadata = crud.register_pipe_metadata(db, pipe)
    pipe_version = crud.create_pipe_version(db, pipe_metadata)
    pd = pipe_metadata.__dict__
    pd.update(pipe_version.__dict__)
    return schemas.PipeDetail(**pd)

def _update_pipe(update: schemas.PipeUpdate, name: str, db: Session):
    """
    The implementation of the update_pipe route with logic here so other functions can call it
    :param update: The pipe version to create
    :param db: The database session
    :return: The created Pipe version
    """

    pipes: List[schemas.PipeDetail] = crud.get_pipes(db, _filter={'name': name, 'pipe_version': 'latest'})
    if not pipes:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Pipe {name} does not exist. Please enter "
                                        f"a valid pipe, or create this pipe using fs.create_pipe()")
    pipe = pipes[0]

    if update.description:
        logger.info(f'Updating description for {name}')
        crud.update_pipe_description(db, pipe.pipe_id, update.description)

    crud.validate_pipe_function(update, pipe.ptype)
    pipe.__dict__.update(update.__dict__)
    pipe_version = crud.create_pipe_version(db, pipe, pipe.pipe_version + 1)
    pipe.pipe_version = pipe_version.pipe_version

    return pipe

def _alter_pipe(alter: schemas.PipeAlter, name: str, version: Union[str, int], db: Session):
    """
    The implementation of the update_pipe route with logic here so other functions can call it
    :param alter: The pipe to alter
    :param name: The pipe name
    :param version: The version to alter
    :param db: The database session
    :return: The updated Pipe
    """
    pipes: List[schemas.PipeDetail] = crud.get_pipes(db, _filter={'name': name, 'pipe_version': version})
    if not pipes:
        v = f'with version {version} ' if isinstance(version, int) else ''
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Pipe {name} {v}does not exist. Please enter a valid pipe.")
    pipe = pipes[0]

    if alter.func:
        pipelines = crud.get_pipelines_from_pipe(db, pipe)
        if pipelines:
            deps = [f'{pipeline.name} v{pipeline.pipeline_version}' for pipeline in pipelines]
            raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.DEPENDENCY_CONFLICT,
                                        message=f"Cannot remove pipe {name} since it is used by the following pipelines: {deps}. "
                                        "Either remove this pipe from these pipelines, or create a new version of this pipe "
                                        "using fs.update_pipe().")
        crud.validate_pipe_function(alter, pipe.ptype)
        crud.alter_pipe_function(db, pipe, alter.func, alter.code)
        pipe.func = alter.func
        pipe.code = alter.code

    if alter.description:
        logger.info(f'Updating description for {name}')
        crud.update_pipe_description(db, pipe.pipe_id, alter.description)
        pipe.description = alter.description

    return pipe

def _create_pipeline(pipeline: schemas.PipelineCreate, db: Session) -> schemas.PipelineDetail:
    """
    The implementation of the create_pipeline route with logic here so other functions can call it
    :param pipeline: The pipeline to create
    :param db: The database session
    :return: The created Pipeline
    """
    crud.validate_pipeline(db, pipeline)
    logger.info(f'Registering pipeline {pipeline.name} in Feature Store')
    pipeline_metadata = crud.register_pipeline_metadata(db, pipeline)
    pipeline_version = crud.create_pipeline_version(db, pipeline_metadata)
    pd = pipeline_metadata.__dict__
    pd.update(pipeline_version.__dict__)
    pl = schemas.PipelineDetail(**pd)
    if pipeline.pipes:
        crud.register_pipeline_pipes(db, pl, pipeline.pipes)
        pl.pipes = pipeline.pipes
    return pl

def _update_pipeline(update: schemas.PipelineUpdate, name: str, db: Session):
    """
    The implementation of the update_pipeline route with logic here so other functions can call it
    :param update: The pipeline version to create
    :param db: The database session
    :return: The created Pipeline version
    """

    pipelines: List[schemas.PipelineDetail] = crud.get_pipelines(db, _filter={'name': name, 'pipeline_version': 'latest'})
    if not pipelines:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Pipeline {name} does not exist. Please enter "
                                        f"a valid pipeline, or create this pipeline using fs.create_pipeline()")
    pipeline = pipelines[0]

    if update.description:
        logger.info(f'Updating description for {name}')
        crud.update_pipeline_description(db, pipeline.pipeline_id, update.description)

    pipeline.__dict__.update(update.__dict__)
    pipeline_version = crud.create_pipeline_version(db, pipeline, pipeline.pipeline_version + 1)
    pipeline.pipeline_version = pipeline_version.pipeline_version

    if pipeline.pipes:
        pipeline.pipes = crud.process_pipes(db, pipeline.pipes)
        crud.register_pipeline_pipes(db, pipeline, pipeline.pipes)
    
    return pipeline

def _alter_pipeline(alter: schemas.PipelineAlter, name: str, version: Union[str, int], db: Session):
    """
    The implementation of the update_pipeline route with logic here so other functions can call it
    :param alter: The pipeline to alter
    :param name: The pipeline name
    :param version: The version to alter
    :param db: The database session
    :return: The updated Pipeline
    """
    pipelines: List[schemas.PipelineDetail] = crud.get_pipelines(db, _filter={'name': name, 'pipeline_version': version})
    if not pipelines:
        v = f'with version {version} ' if isinstance(version, int) else ''
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Pipeline {name} {v}does not exist. Please enter a valid pipeline.")
    pipeline = pipelines[0]

    changes = {k: v for k, v in alter.__dict__.items() if v is not None}
    desc = changes.pop('description', None)

    if changes:
        if pipeline.feature_set_id:
            raise SpliceMachineException(status_code=status.HTTP_406_NOT_ACCEPTABLE, code=ExceptionCodes.ALREADY_DEPLOYED,
                                        message=f"Cannot alter Pipeline {name} version {pipeline.pipeline_version} as it has "
                                                "already been deployed to a Feature Set. If you wish to make changes, please create "
                                                "a new version with fs.update_pipeline()")
        pipes = changes.pop('pipes', None)
        if pipes:
            pipes = crud.process_pipes(db, pipes)
            crud.update_pipeline_pipes(db, pipeline, pipes)
            pipeline.pipes = pipes

        if changes:
            pipeline.__dict__.update((k, changes.__dict__[k]) for k in pipeline.__dict__.keys() & changes.__dict__.keys())
            crud.alter_pipeline_version(db, pipeline)

    if desc:
        logger.info(f'Updating description for {name}')
        crud.update_pipe_description(db, pipeline.pipeline_id, desc)
        pipeline.description = desc

    return pipeline

def _deploy_pipeline(name: str, schema: str, table: str, version: Union[str, int], db: Session):
    pipelines = crud.get_pipelines(db, _filter={"name": name, "pipeline_version": version})
    if not pipelines:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                    message=f"Cannot find any pipeline with name '{name}'.")
    pipeline = pipelines[0]

    if not crud.get_pipes_in_pipeline(db, pipeline):
        raise SpliceMachineException(status_code=status.HTTP_406_NOT_ACCEPTABLE, code=ExceptionCodes.NOT_DEPLOYABLE,
                                        message=f"Pipeline {name} has no pipes. "
                                        "You cannot deploy a pipeline with no pipes.")

    fset_name = f'{schema}.{table}'
    fsets = crud.get_feature_sets(db, feature_set_names=[fset_name])
    if not fsets:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                message=f"Feature Set {schema}.{table} does not exist. Please enter "
                                f"a valid feature set.")
    
    fset = fsets[0]
    if not fset.deployed:
        raise SpliceMachineException(status_code=status.HTTP_406_NOT_ACCEPTABLE, code=ExceptionCodes.NOT_DEPLOYED,
                                        message=f"Cannot deploy Pipeline to Feature Set {fset_name} v{fset.feature_set_version} as it is undeployed. "
                                        "Either deploy this Feature Set, or enter a deployed Feature Set.")

    fset_pipelines = crud.get_feature_set_pipelines(db, fset.feature_set_id, fset.feature_set_version)
    if fset_pipelines:
        pl = fset_pipelines[0]
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_DEPLOYED,
                                        message=f"Cannot deploy Pipeline to Feature Set {fset_name} v{fset.feature_set_version} as Pipeline {pl} is already "
                                        "deployed to it. Either undeploy this Pipeline, or deploy your Pipeline to a different Feature Set or version.")

    crud.set_pipeline_deployment_metadata(db, pipeline, fset)

    return pipeline

def _undeploy_pipeline(name: str, version: Union[str, int], db: Session):
    pipelines = crud.get_pipelines(db, _filter={"name": name, "pipeline_version": version})
    if not pipelines:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                    message=f"Cannot find any pipeline with name '{name}'.")
    pipeline = pipelines[0]

    if not pipeline.feature_set_id:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.NOT_DEPLOYED,
                                        message=f"Pipeline {name} v{pipeline.pipeline_version} is not deployed.")

    crud.unset_pipeline_deployment_metadata(db, pipeline)

    return pipeline
