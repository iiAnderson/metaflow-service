import asyncio
import json
from aiohttp import web
from .utils import read_body
from ..data.models import RichRunRow
from ..data.postgres_async_db import AsyncPostgresDB


class RichRunApi(object):
    _run_table = None
    lock = asyncio.Lock()

    def __init__(self, app):
        app.router.add_route("GET", "/rich/flows/{flow_id}/runs", self.get_all_rich_runs)
        app.router.add_route("GET", "/rich/flows/{flow_id}/runs/{run_number}", self.get_rich_run)
        app.router.add_route("POST", "/rich/flows/{flow_id}/run/{run_number}", self.create_rich_run)
        self._async_table = AsyncPostgresDB.get_instance().rich_run_table_postgres

    async def get_rich_run(self, request):
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
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        run_response = await self._async_table.get_rich_run(flow_name, run_number)
        return web.Response(
            status=run_response.response_code, body=json.dumps(run_response.body)
        )

    async def get_all_rich_runs(self, request):
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
        runs = await self._async_table.get_all_rich_runs(flow_name)

        return web.Response(status=runs.response_code, body=json.dumps(runs.body))

    async def get_rich_run_since(self, request):
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
        since_ts = request.match_info.get("since_ts")

        runs = await self._async_table.get_rich_run_since(flow_name, since_ts)

        return web.Response(status=runs.response_code, body=json.dumps(runs.body))

    async def create_rich_run(self, request):
        """
        ---
        description: create run and generate run id
        tags:
        - Run
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "body"
          in: "body"
          description: "body"
          required: true
          schema:
            type: object
            properties:
                user_name:
                    type: string
                tags:
                    type: object
                system_tags:
                    type: object
        produces:
        - 'text/plain'
        responses:
            "200":
                description: successful operation. Return newly registered run
            "405":
                description: invalid HTTP Method
        """
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")

        body = await read_body(request.content)
        success = body.get("success")
        finished = body.get("finished")
        finished_at = body.get("finished_at")
        execution_length = body.get("execution_length")

        run_row = RichRunRow(
            flow_id=flow_name, run_number=run_number, success=success, 
            finished=finished, finished_at=finished_at, execution_length=execution_length
        )

        run_response = await self._async_table.add_rich_run(run_row)
        return web.Response(
            status=run_response.response_code, body=json.dumps(run_response.body)
        )
