import datetime as dt
from typing import Union, List

import pandas as pd

from mindsdb_sql.parser.dialects.mindsdb import CreateJob, DropJob
from mindsdb_sql.parser.ast import Identifier, Star, Select

from mindsdb_sdk.utils.sql import dict_to_binary_op
from mindsdb_sdk.utils.objects_collection import CollectionBase


class Job:
    def __init__(self, project, data):
        self.project = project
        self.data = data
        self._update(data)

    def _update(self, data):
        self.name = data['name']
        self.query_str = data['query']
        self.start_at = data['start_at']
        self.end_at = data['end_at']
        self.next_run_at = data['next_run_at']
        self.schedule_str = data['schedule_str']

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name}, query='{self.query_str}')"

    def refresh(self):
        """
        Retrieve job data from mindsdb server
        """
        job = self.project.get_job(self.name)
        self._update(job.data)

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

    def list(self, name: str = None) -> List[Job]:
        """
        Show list of jobs in project

        :return: list of Job objects
        """

        ast_query = Select(targets=[Star()], from_table=Identifier('jobs'))

        if name is not None:
            ast_query.where = dict_to_binary_op({'name': name})

        df = self.api.sql_query(ast_query.to_string(), database=self.project.name)

        # columns to lower case
        cols_map = {i: i.lower() for i in df.columns}
        df = df.rename(columns=cols_map)

        return [
            Job(self.project, item)
            for item in df.to_dict('records')
        ]

    def get(self, name: str) -> Job:
        """
        Get job by name from project

        :param name: name of the job
        :return: Job object
        """

        jobs = self.list(name)
        if len(jobs) == 1:
            return jobs[0]
        elif len(jobs) == 0:
            raise AttributeError("Job doesn't exist")
        else:
            raise RuntimeError("Several jobs with the same name")

    def create(self, name: str, query_str: str,
                   start_at: dt.datetime = None, end_at: dt.datetime = None,
                   repeat_str: str = None) -> Union[Job, None]:
        """
        Create new job in project and return it.
        If it is not possible (job executed and not accessible anymore): return None
        More info: https://docs.mindsdb.com/sql/create/jobs

        :param name: name of the job
        :param query_str: str, job's query (or list of queries with ';' delimiter) which job have to execute
        :param start_at: datetime, first start of job,
        :param end_at: datetime, when job have to be stopped,
        :param repeat_str: str, optional, how to repeat job (e.g. '1 hour', '2 weeks', '3 min')
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
        ast_query = CreateJob(
            name=Identifier(name),
            query_str=query_str,
            start_str=start_str,
            end_str=end_str,
            repeat_str=repeat_str
        )

        self.api.sql_query(ast_query.to_string(), database=self.project.name)

        # job can be executed and remove it is not repeatable
        jobs = self.list(name)
        if len(jobs) == 1:
            return jobs[0]

    def drop(self, name: str):
        """
        Drop job from project

        :param name: name of the job
        """
        ast_query = DropJob(Identifier(name))

        self.api.sql_query(ast_query.to_string(), database=self.project.name)
