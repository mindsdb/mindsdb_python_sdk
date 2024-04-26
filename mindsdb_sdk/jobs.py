import datetime as dt
from typing import Union, List


import pandas as pd

from mindsdb_sql.parser.dialects.mindsdb import CreateJob, DropJob
from mindsdb_sql.parser.ast import Identifier, Star, Select

from mindsdb_sdk.query import Query
from mindsdb_sdk.utils.sql import dict_to_binary_op
from mindsdb_sdk.utils.objects_collection import CollectionBase
from mindsdb_sdk.utils.context import set_saving


class Job:
    def __init__(self, project, name, data=None, create_callback=None):
        self.project = project
        self.name = name
        self.data = data

        self.query_str = None
        if data is not None:
            self._update(data)
        self._queries = []
        self._create_callback = create_callback

    def _update(self, data):
        # self.name = data['name']
        self.query_str = data['query']
        self.start_at = data['start_at']
        self.end_at = data['end_at']
        self.next_run_at = data['next_run_at']
        self.schedule_str = data['schedule_str']

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name}, query='{self.query_str}')"

    def __enter__(self):
        if self._create_callback is None:
            raise ValueError("The job is already created and can't be used to create context."
                               " To be able to use context: create job without 'query_str' parameter: "
                               "\n>>> with con.jobs.create('j1') as job:"
                               "\n>>>    job.add_query(...)")
        set_saving(f'job-{self.name}')

        return self

    def __exit__(self, type, value, traceback):
        set_saving(None)
        if type is None:
            if len(self._queries) == 0:
                raise ValueError('No queries were added to job')

            query_str = '; '.join(self._queries)

            self._create_callback(query_str)

            self.refresh()

    def refresh(self):
        """
        Retrieve job data from mindsdb server
        """
        job = self.project.get_job(self.name)
        self._update(job.data)

    def add_query(self, query: Union[Query, str]):
        """
        Add a query to job. Method is used in context of the job

        >>> with con.jobs.create('j1') as job:
        >>>    job.add_query(table1.insert(table2))

        :param query: string or Query object. Query.database should be emtpy or the same as job's project
        """
        if isinstance(query, Query):

            if query.database is not None and query.database != self.project.name:
                # we can't execute this query in jobs project
                raise ValueError(f"Wrong query database: {query.database}. You could try to use sql string instead")

            query = query.sql
        elif not isinstance(query, str):
            raise ValueError(f'Unable to use add this object as a query: {query}. Try to use sql string instead')
        self._queries.append(query)

    def get_history(self) -> pd.DataFrame:
        """
        Get history of job execution

        :return: dataframe with job executions
        """
        ast_query = Select(
            targets=[Star()],
            from_table=Identifier('jobs_history'),
            where=dict_to_binary_op({
                'name': self.name
            })
        )
        return self.project.api.sql_query(ast_query.to_string(), database=self.project.name)


class Jobs(CollectionBase):
    def __init__(self, project, api):
        self.project = project
        self.api = api

    def _list(self, name: str = None) -> List[Job]:

        ast_query = Select(targets=[Star()], from_table=Identifier('jobs'))

        if name is not None:
            ast_query.where = dict_to_binary_op({'name': name})

        df = self.api.sql_query(ast_query.to_string(), database=self.project.name)

        # columns to lower case
        cols_map = {i: i.lower() for i in df.columns}
        df = df.rename(columns=cols_map)

        return [
            Job(self.project, item.pop('name'), item)
            for item in df.to_dict('records')
        ]

    def list(self) -> List[Job]:
        """
        Show list of jobs in project

        :return: list of Job objects
        """

        return self._list()

    def get(self, name: str) -> Job:
        """
        Get job by name from project

        :param name: name of the job
        :return: Job object
        """

        jobs = self._list(name)
        if len(jobs) == 1:
            return jobs[0]
        elif len(jobs) == 0:
            raise AttributeError("Job doesn't exist")
        else:
            raise RuntimeError("Several jobs with the same name")

    def create(
            self,
            name: str,
            query_str: str = None,
            start_at: dt.datetime = None,
            end_at: dt.datetime = None,
            repeat_str: str = None,
            repeat_min: int = None,
        ) -> Union[Job, None]:
        """
        Create new job in project and return it.

        If it is not possible (job executed and not accessible anymore):
           return None

        Usage options:

        Option 1: to use string query
        All job tasks could be passed as string with sql queries. Job is created immediately

        >>> job = con.jobs.create('j1', query_str='retrain m1; show models', repeat_min=1):

        Option 2: to use 'with' block.
        It allows to pass sdk commands to job tasks.
        Not all sdk commands could be accepted here,
         only those which are converted in to sql in sdk and sent to /query endpoint
        Adding query sql string is accepted as well
        Job will be created after exit from 'with' block

        >>> with con.jobs.create('j1', repeat_min=1) as job:
        >>>    job.add_query(table1.insert(table2))
        >>>    job.add_query('retrain m1')  # using string

        More info about jobs: https://docs.mindsdb.com/sql/create/jobs

        :param name: name of the job
        :param query_str: str, job's query (or list of queries with ';' delimiter) which job have to execute
        :param start_at: datetime, first start of job,
        :param end_at: datetime, when job have to be stopped,
        :param repeat_str: str, optional, how to repeat job (e.g. '1 hour', '2 weeks', '3 min')
        :param repeat_min: int, optional, period to repeat the job in minutes
        :return: Job object or None
        """

        if start_at is not None:
            start_str = start_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            start_str = None

        if end_at is not None:
            end_str = end_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            end_str = None

        if repeat_min is not None:
            repeat_str = f'{repeat_min} minutes'

        def _create_callback(query):
            ast_query = CreateJob(
                name=Identifier(name),
                query_str=query,
                start_str=start_str,
                end_str=end_str,
                repeat_str=repeat_str
            )

            self.api.sql_query(ast_query.to_string(), database=self.project.name)

        if query_str is None:
            # allow to create context with job
            job = Job(self.project, name, create_callback=_create_callback)
            return job
        else:
            # create it
            _create_callback(query_str)

            # job can be executed and remove it is not repeatable
            jobs = self._list(name)
            if len(jobs) == 1:
                return jobs[0]


    def drop(self, name: str):
        """
        Drop job from project

        :param name: name of the job
        """
        ast_query = DropJob(Identifier(name))

        self.api.sql_query(ast_query.to_string(), database=self.project.name)
