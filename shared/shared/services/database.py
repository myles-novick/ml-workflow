from os import environ as env_vars

from mlflow.store.db.base_sql_model import Base as MLFlowBase
from sqlalchemy import create_engine
from sqlalchemy import inspect as peer_into_db
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from shared.logger.logging_config import logger


class DatabaseConnectionConfig:
    """
    Database Connection Config
    """
    database_name: str = env_vars.get("DB_DATABASE_NAME", "splicedb")
    database_schema: str = env_vars['DB_USER'].upper()
    database_user: str = env_vars['DB_USER']
    database_port: str = env_vars.get('SPLICEDB_HREGION_SERVICE_PORT_JDBC', '1527')
    database_password: str = env_vars['DB_PASSWORD']
    database_host: str = env_vars['DB_HOST']

    @staticmethod
    def connection_string():
        """
        Get the SQLAlchemy Connection String
        :return: cnxn string
        """
        return f"splicemachinesa://{DatabaseConnectionConfig.database_user}:" \
               f"{DatabaseConnectionConfig.database_password}@{DatabaseConnectionConfig.database_host}:" \
               f"{DatabaseConnectionConfig.database_port}/{DatabaseConnectionConfig.database_name}"


class DatabaseEngineConfig:
    """
    Database Engine Connection Configuration
    """
    pool_size: int = 3
    max_overflow: int = 0
    echo: bool = env_vars['MODE'] == 'development'
    pool_pre_ping: bool = True

    @staticmethod
    def as_dict():
        """
        Serialize the Engine Configuration to a dictionary
        :return: serialized version
        """
        return {
            'pool_size': DatabaseEngineConfig.pool_size,
            'max_overflow': DatabaseEngineConfig.max_overflow,
            'echo': DatabaseEngineConfig.echo,
            'pool_pre_ping': DatabaseEngineConfig.pool_pre_ping
        }


class SQLAlchemyClient:
    """
    Database configuration constants
    """
    engine = None  # SQLAlchemy Engine
    reflector = None  # Inspector for DB reflection

    SessionMaker = None  # Session Maker
    SessionFactory = None  # Thread-safe session factory issuer

    # We have two bases because there are two different types of tables to create.
    # There are MLFlow tables (created by their code, such as Experiment
    SpliceBase = declarative_base()
    MlflowBase = MLFlowBase

    _created = False

    @staticmethod
    def create():
        """
        Connect to the Splice Machine Database
        :return: sqlalchemy engine
        """
        # if there are multiple writers, setting var before
        # will ensure that only one engine is created
        if not SQLAlchemyClient._created:
            logger.info("SQLAlchemy Engine has not been created... creating SQLAlchemy Client...")
            SQLAlchemyClient.engine = create_engine(
                DatabaseConnectionConfig.connection_string(),
                **DatabaseEngineConfig.as_dict()
            )
            SQLAlchemyClient.SpliceBase.metadata.bind = SQLAlchemyClient.engine
            logger.debug("Created engine...")
            SQLAlchemyClient.SessionMaker = sessionmaker(bind=SQLAlchemyClient.engine, expire_on_commit=False)
            logger.debug("Created session maker")
            SQLAlchemyClient.SessionFactory = scoped_session(SQLAlchemyClient.SessionMaker)
            logger.debug("created session factory")
            SQLAlchemyClient._created = True
            logger.debug("Created SQLAlchemy Client...")
        else:
            logger.debug("Using existing SQLAlchemy Client...")
        return SQLAlchemyClient.engine

    @staticmethod
    def execute(sql: str) -> list:
        """
        Directly Execute SQL on the
        SQLAlchemy ENGINE without
        using the ORM (more performant)

        :param sql: (str) the SQL to execute
        :return: (list) returned result set
        """
        return list(SQLAlchemyClient.engine.execute(sql))


class DatabaseSQL:
    """
    Namespace for SQL Commands
    """
    live_status_view_selector: str = \
        """
       SELECT mm.run_uuid,
           mm.action,
           CASE
               WHEN ((sta.tableid IS NULL
                      OR st.triggerid IS NULL
                      OR (mm.triggerid_2 IS NOT NULL
                          AND st2.triggerid IS NULL))
                     AND mm.action = 'DEPLOYED') THEN 'Table or Trigger Missing'
               ELSE mm.action
           END AS deployment_status,
           mm.tableid,
           mm.trigger_type,
           mm.triggerid,
           mm.triggerid_2,
           mm.db_env,
           mm.db_user,
           mm.action_date
    FROM model_metadata mm
    LEFT OUTER JOIN sys.systables sta USING (tableid)
    LEFT OUTER JOIN sys.systriggers st ON (mm.triggerid = st.triggerid)
    LEFT OUTER JOIN sys.systriggers st2 ON (mm.triggerid_2 = st2.triggerid)
        """

    retrieve_jobs: str = \
        """
        SELECT id, "timestamp" FROM JOBS
        WHERE status='PENDING'
        ORDER BY "timestamp"
        """
    get_monthly_aggregated_jobs = \
        """
        SELECT MONTH(INNER_TABLE.parsed_date) AS month_1, COUNT(*) AS count_1, user_1
        FROM (
            SELECT TIMESTAMP("timestamp") AS parsed_date, "user" as user_1
            FROM JOBS
        ) AS INNER_TABLE
        WHERE YEAR(INNER_TABLE.parsed_date) = YEAR(CURRENT_TIMESTAMP)
        GROUP BY 1, 3
        """


SQLAlchemyClient.create()