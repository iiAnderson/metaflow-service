import asyncio
import json
from datetime import datetime, timedelta
from aiohttp import web
from .utils import read_body, get_week_times, get_formatted_time
from ..data.models import RunRow
from ..data.postgres_async_db import AsyncPostgresDB
import logging

class DashboardAPI(object):
    _run_table = None
    lock = asyncio.Lock()

    def __init__(self, app, cors):
        cors.add(app.router.add_route("GET", "/dashboard/flows", self.get_flows))
        cors.add(app.router.add_route("GET", "/dashboard/flows/{flow_id}/count", self.count_runs))
        cors.add(app.router.add_route("GET", "/dashboard/flows/{flow_id}/recent", self.get_recent_run))
        cors.add(app.router.add_route("GET", "/dashboard/flows/{flow_id}/last", self.get_last_n_runs))
        cors.add(app.router.add_route("GET", "/dashboard/flows/{flow_id}/{timestamp}", self.get_runs_since))

        self._run_async_table = AsyncPostgresDB.get_instance().run_table_postgres
        self._flow_async_table = AsyncPostgresDB.get_instance().flow_table_postgres
        self._rich_run_async_table = AsyncPostgresDB.get_instance().rich_run_table_postgres

    async def get_flows(self, request):
        """
        ---
        description: Get run by run number
        tags:
        - Run
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "run_number"
          in: "path"
          description: "run_number"
          required: true
          type: "integer"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Return specified run
            "404":
                description: specified run not found
            "405":
                description: invalid HTTP Method
        """

        flow_response = await self._flow_async_table.get_all_flows()
        data = []

        for flow in flow_response.body:
            flow_id = flow['flow_id']

            run_response = await self._run_async_table.get_all_runs(flow_id)
            last_run = run_response.body[-1]

            rich_run_response = await self._rich_run_async_table.get_rich_run(flow_id, last_run['run_number'])
            rich_last_run = rich_run_response.body

            data.append({
                "success": rich_last_run['success'],
                "finished": rich_last_run['finished'],
                "finished_at": rich_last_run['finished_at'],
                "created_at": last_run['ts_epoch'],
                "run_id": last_run['run_number'],
                "flow": flow_id,
                "user": last_run['user_name']
            })

        return web.Response(
            status=rich_run_response.response_code, body=json.dumps(data)
        )

    async def count_runs(self, request):
        """
        ---
        description: Get all runs
        tags:
        - Run
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: Returned all runs of specified flow
            "405":
                description: invalid HTTP Method
        """
        flow_name = request.match_info.get("flow_id")

        if flow_name == "all":

            flow_response = await self._flow_async_table.get_all_flows()
            flows = [x['flow_id'] for x in flow_response.body]
        else:
            flows = [flow_name]

        counts = get_week_times()
        time_bound = (datetime.now() - timedelta(days=7)).timestamp()

        for flow_id in flows:

            run_response = await self._rich_run_async_table.get_rich_run_since(flow_id, time_bound)

            for run in run_response.body:
                
                logging.error(run)
                datetime_created = datetime.fromtimestamp(run['ts_epoch']/1000)
                counts[get_formatted_time(datetime_created)] = counts[get_formatted_time(datetime_created)] + 1

        return_data =[]
        for key, value in counts.items():
            return_data.append({"time": key, "count": value})

        return web.Response(status=run_response.response_code, body=json.dumps(return_data))

    async def get_runs_since(self, request):
        """
        ---
        description: Get all runs
        tags:
        - Run
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: Returned all runs of specified flow
            "405":
                description: invalid HTTP Method
        """
        timestamp = request.match_info.get("timestamp")
        flow_name = request.match_info.get("flow_id")

        if flow_name == "all":

            flow_response = await self._flow_async_table.get_all_flows()
            flows = [x['flow_id'] for x in flow_response.body]
        else:
            flows = [flow_name]

        data = []

        for flow_id in flows:

            run_response = await self._rich_run_async_table.get_rich_run_since(flow_id, timestamp)
            rich_runs = run_response.body

            for rich_run_data in rich_runs:
                logging.error(flow_id + " " + str(rich_run_data['run_number']))

                run_response = await self._run_async_table.get_run(flow_id, rich_run_data['run_number'])
                run_data = run_response.body

                data.append({
                    "success": rich_run_data['success'],
                    "finished": rich_run_data['finished'],
                    "finished_at": rich_run_data['finished_at'],
                    "created_at": run_data['ts_epoch'],
                    "run_id": run_data['run_number'],
                    "flow": flow_id,
                    "user": run_data['user_name']
                })

        return web.Response(status=run_response.response_code, body=json.dumps(data))

    async def get_run_data(self, request):
        """
        ---
        description: Get all runs
        tags:
        - Run
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: Returned all runs of specified flow
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")

        run_response = await self._rich_run_async_table.get_rich_run(flow_id, run_number)
        rich_run_data = run_response.body

        run_response = await self._run_async_table.get_run(flow_id, run_number)
        run_data = run_response.body

        data = {
            "success": rich_run_data['success'],
            "finished": rich_run_data['finished'],
            "finished_at": rich_run_data['finished_at'],
            "created_at": run_data['ts_epoch'],
            "run_id": run_data['run_number'],
            "flow": flow_id,
            "user": run_data['user_name']
        }

        return web.Response(status=run_response.response_code, body=json.dumps(data))

    async def get_recent_run(self, request):
        """
        ---
        description: Get all runs
        tags:
        - Run
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: Returned all runs of specified flow
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")

        run_response = await self._run_async_table.get_all_runs(flow_id)
        run_data = run_response.body

        recent_run = run_data[-1]

        run_response = await self._rich_run_async_table.get_rich_run(flow_id, recent_run['run_number'])
        rich_run_data = run_response.body

        data = {
            "success": rich_run_data['success'],
            "finished": rich_run_data['finished'],
            "finished_at": rich_run_data['finished_at'],
            "created_at": recent_run['ts_epoch'],
            "run_id": recent_run['run_number'],
            "flow": flow_id,
            "user": recent_run['user_name']
        }

        return web.Response(status=run_response.response_code, body=json.dumps(data))

    async def get_last_n_runs(self, request):
        """
        ---
        description: Get all runs
        tags:
        - Run
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: Returned all runs of specified flow
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")

        run_response = await self._run_async_table.get_all_runs(flow_id)
        run_data = run_response.body

        n_recent_runs = run_data[-5:]
        data = []

        for recent_run in n_recent_runs:
            run_response = await self._rich_run_async_table.get_rich_run(flow_id, recent_run['run_number'])
            rich_run_data = run_response.body

            data.append({
                "success": rich_run_data['success'],
                "finished": rich_run_data['finished'],
                "finished_at": rich_run_data['finished_at'],
                "created_at": recent_run['ts_epoch'],
                "run_id": recent_run['run_number'],
                "flow": flow_id,
                "user": recent_run['user_name']
            })

        return web.Response(status=run_response.response_code, body=json.dumps(data))

