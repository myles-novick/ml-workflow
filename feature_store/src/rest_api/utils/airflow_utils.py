import json
from datetime import datetime
from os import environ as env_vars
from typing import List

import requests
from shared.api.exceptions import ExceptionCodes, SpliceMachineException
from shared.logger.logging_config import logger

import schemas


class Endpoints:
    """
    Enum for Airflow Endpoints
    """
    VARIABLES: str = "variables"
    BACKFILL: str = "dags/Feature_Set_Backfill/dagRuns"


class Variables:
    """
    Enum for the Variables stored in Airflow
    """
    AGG_FEATURE_SETS: str = "agg_feature_sets"
    FEATURE_SETS: str = "feature_sets"
    PIPELINES: str = "pipelines"

class Airflow:
    AIRFLOW_URL: str = None
    EXTERNAL_UI: str = None
    auth = None
    is_active = True

    @staticmethod
    def get_variable_if_exists(variable: str):
        r = requests.get(f'{Airflow.AIRFLOW_URL}/{Endpoints.VARIABLES}/{variable}', auth=Airflow.auth)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as error:
            if error.response.status_code != 404:
                raise SpliceMachineException(status_code=error.response.status_code, code=ExceptionCodes.UNKNOWN,
                                             message=str(error))
            return None
        return json.loads(r.json()['value'])

    @staticmethod
    def create_or_update_variable(variable: str, key: str, value):
        items = Airflow.get_variable_if_exists(variable)
        if items == None:  # Need to compare to None because [] and {} require a PUT
            logger.info(f'Variable {variable} not found in airflow - creating...')
            items = {key: value}
            body = {'key': variable, 'value': json.dumps(items)}
            r = requests.post(f'{Airflow.AIRFLOW_URL}/{Endpoints.VARIABLES}', json=body, auth=Airflow.auth)
        else:
            logger.info(f'Variable {variable} found in airflow - updating...')
            items[key] = value
            body = {'key': variable, 'value': json.dumps(items)}
            r = requests.patch(f'{Airflow.AIRFLOW_URL}/{Endpoints.VARIABLES}/{variable}', json=body, auth=Airflow.auth)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise SpliceMachineException(status_code=error.response.status_code, code=ExceptionCodes.UNKNOWN,
                                         message=str(error))

    @staticmethod
    def remove_from_variable(variable: str, key: str):
        items = Airflow.get_variable_if_exists(variable)
        if items and key in items:
            items.pop(key)
            body = {'key': variable, 'value': json.dumps(items)}
            try:
                requests.patch(f'{Airflow.AIRFLOW_URL}/{Endpoints.VARIABLES}/{variable}', json=body,
                               auth=Airflow.auth).raise_for_status()
            except requests.exceptions.HTTPError as error:
                raise SpliceMachineException(status_code=error.response.status_code, code=ExceptionCodes.UNKNOWN,
                                             message=str(error))

    @staticmethod
    def remove_multiple_from_variable(variable: str, keys: List[str]):
        """
        Removes multiple keys from an Airflow variable

        :param variable: The variable from which to remove the keys
        :param keys: List of keys to remove from the variable
        """
        items = Airflow.get_variable_if_exists(variable)
        if items:
            [items.pop(key, None) for key in keys]
            body = { 'key': variable, 'value': json.dumps(items) }
            try:
                requests.patch(f'{Airflow.AIRFLOW_URL}/{Endpoints.VARIABLES}/{variable}', json=body, auth=Airflow.auth).raise_for_status()
            except requests.exceptions.HTTPError as error:
                raise SpliceMachineException(status_code=error.response.status_code, code=ExceptionCodes.UNKNOWN,
                        message=str(error))

    @staticmethod
    def schedule_feature_set_calculation(fset: schemas.FeatureSetDetail):
        key = f'{fset.schema_name}.{fset.versioned_table}'
        args = [fset.feature_set_id, fset.feature_set_version]
        value = {'schedule_interval': '@daily', 'start_date': datetime.today().strftime('%Y-%m-%d'), 'op_args': args}
        Airflow.create_or_update_variable(Variables.FEATURE_SETS, key, value)

    @staticmethod
    def unschedule_feature_set_calculation(fset: str):
        Airflow.remove_from_variable(Variables.FEATURE_SETS, fset)

    @staticmethod
    def schedule_pipeline(fset: str, schedule: str, start_date: datetime):
        value = {'schedule_interval': schedule, 'start_date': start_date.strftime('%Y-%m-%d')}
        Airflow.create_or_update_variable(Variables.AGG_FEATURE_SETS, fset, value)

    @staticmethod
    def unschedule_pipeline(fset: str):
        Airflow.remove_from_variable(Variables.AGG_FEATURE_SETS, fset)

    @staticmethod
    def trigger_backfill(schema: str, table: str):
        run_id = f'{schema}.{table}-{datetime.now()}'
        conf = {"schema": schema, "table": table}
        body = {"dag_run_id": run_id, "conf": conf}
        try:
            requests.post(f'{Airflow.AIRFLOW_URL}/{Endpoints.BACKFILL}', json=body,
                          auth=Airflow.auth).raise_for_status()
        except requests.exceptions.HTTPError as error:
            if error.response.status_code != 404:
                raise SpliceMachineException(status_code=error.response.status_code, code=ExceptionCodes.UNKNOWN,
                                             message=str(error))

    @staticmethod
    def get_dag_url(name: str, version: int):
        if not Airflow.EXTERNAL_UI:
            return None
        return Airflow.EXTERNAL_UI + f'/dag_details?dag_id={name}_v{version}'

    @staticmethod
    def setup():
        url = env_vars.get('AIRFLOW_URL')
        if url:
            Airflow.AIRFLOW_URL = url
        else:
            logger.warning('Could not find URL for Airflow API. Airflow functionality will not be available')
            Airflow.is_active = False
            return
        user = env_vars.get('AIRFLOW_USER')
        password = env_vars.get('AIRFLOW_PASSWORD')
        if user and password:
            Airflow.auth = (user, password)
        else:
            logger.warning('Could not find credentials for Airflow API. Airflow functionality will not be available')
            Airflow.is_active = False
            return
        try:
            requests.get(f'{Airflow.AIRFLOW_URL}/health').raise_for_status()
        except requests.exceptions.RequestException:
            logger.warning('Could not connect to Airflow API. Airflow functionality will not be available')
            Airflow.is_active = False
            return
        logger.info('Successfully connected to Airflow API')
        Airflow.EXTERNAL_UI = env_vars.get('AIRFLOW_EXTERNAL_UI', None)
