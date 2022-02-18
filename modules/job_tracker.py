import sys
import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import select
import inspect 

from toml_config import config



class Tracker:
    """
    Tracks job_id, job_status, updated_time for pipeline runs. 
    """

    def __init__(self, config):

        self.config = config
        self.db_url = config['DB_SETTINGS']
        self.engine = self.get_db_connection()[0]
        self.meta = self.get_db_connection()[1]
        self.session = self.get_db_connection()[2]
        self.tracker_table = self.create_table()


    def get_db_connection(self):
        """
        Establishes PostgreSQL database connection using SQLAlchemy
        :param: none
        :returns: engine, meta, session
        :return tyep: SQLAlchemy connection engine, metadata, and session instances.
        """

        db_url = self.db_url

        try:
            engine = create_engine(URL.create(**db_url))
            # print(f"Connected to PostgreSQL database.")
            conn = engine.connect()
            meta = MetaData()
            Session = sessionmaker(bind=engine)
            session = Session()
            
            return engine, meta, session

        except (Exception, SQLAlchemyError) as e:
            print(f'Encountered exception while attempting to connect to PostgreSQL database:\n{e}')


    def create_table(self):
        """
        Creates PostgreSQL table from name supplied in config file. 
        Table is created in default default 'postgres' database 
        :param: none
        :returns: PostgreSQL table
        """
        engine = self.engine
        meta = self.meta
        session = self.session

        if engine is not None:

            tracking_table = self.config['ETL_TRACKER']['TRACKING_TABLE']

            try: 
                tracker_table = Table(tracking_table, meta,
                    Column('job_id', String(50), primary_key=True),
                    Column('job_status', String(20)),
                    Column('execution_time', DateTime),
                    extend_existing=True)

                meta.create_all(engine)
                session.commit()

                return tracker_table

            except (Exception, SQLAlchemyError) as e:
                print(f'Encountered exception while attempting to create table: "{tracker_table}"\n{e}')


    def assign_job_id(self):
        """
        Create job_id variable from the name of the calling module.
        :param: none
        :returns: name of calling module without '.py' suffix 
        :type job_id: str
        """

        job_id = inspect.stack()[-1].filename.split('/')[-1].removesuffix('.py')

        return job_id


    def update_job_status(self, dates, target, status):
        """
        Inserts job status to tracker_table for each job run.
        date param is provided from other variables within module calling update_job_status method.
        :param: date
        :type date: str
        :param: status
        :type status: str
        """
        engine = self.engine
        meta = self.meta
        session = self.session
        tracker_table = self.tracker_table
        conn = self.engine.connect()
        for date in dates:
            job_id = f'{self.assign_job_id()}_{target}_{date}'

            print(f'Job ID Assigned: {job_id}')

            update_time = datetime.datetime.now()

            try:
                ins = tracker_table.insert().values(job_id=job_id, job_status=status, execution_time=update_time)
                result = conn.execute(ins)
                session.commit()

                print(f'Status of {job_id} has been updated to: {status}')

            except (Exception, SQLAlchemyError) as e:
                print(f'Encountered exception while attempting to update job status table:\n{e}')


    def get_job_status(self, job_id):
        """
        Get job status for job by user supplied job_id.
        :param: job_id
        :type job_id: str
        :returns: matching records for specified job_id param
        """
        engine = self.engine
        meta = self.meta
        session = self.session
        tracker_table = self.tracker_table
        conn = self.engine.connect()

        try:
            stmt = select(tracker_table).where(tracker_table.c.job_id == job_id)
            result = conn.execute(stmt)
            if result:
                for row in result:
                    print(f"Status of {job_id}: {row[1]}")
                    return row
                else:
                    return print(f'No record exists for job ID: {job_id}')

        except (Exception, SQLAlchemyError) as e:
            print(f'Encountered exception while attempting to get job status:\n{e}')
