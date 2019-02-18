import os

from jsonrpc import Dispatcher, JSONRPCResponseManager

from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.task.base import ConfiguredTask
from dbt.task.compile import CompileTask, RemoteCompileTask
from dbt.task.run import RemoteRunTask


class RPCServerTask(ConfiguredTask):
    def __init__(self, args, config, tasks=None):
        super(RPCServerTask, self).__init__(args, config)
        # compile locally
        self.compile_task = CompileTask(args, config)
        self.compile_task.run()
        self.dispatcher = Dispatcher()
        tasks = tasks or [RemoteCompileTask, RemoteRunTask]
        for cls in tasks:
            self.register(cls(args, config))

    def register(self, task):
        self.dispatcher.add_method(task.safe_handle_request,
                                   name=task.METHOD_NAME)

    @property
    def manifest(self):
        return self.compile_task.manifest

    def run(self):
        os.chdir(self.config.target_path)
        host = self.args.host
        port = self.args.port
        addr = (host, port)

        display_host = host
        if host == '0.0.0.0':
            display_host = 'localhost'

        logger.info(
            'Serving RPC server at {}:{}'.format(*addr)
        )

        logger.info(
            'Supported methods: {}'.format(list(self.dispatcher.keys()))
        )

        logger.info(
            'Send requests to http://{}:{}'.format(display_host, port)
        )

        run_simple(host, port, self.handle_request)

    @Request.application
    def handle_request(self, request):
        print('got request: {}'.format(request))
        print('request.data: {}'.format(request.data))
        # request_data is the request as a parsedjson object
        response = JSONRPCResponseManager.handle(
            request.data, self.dispatcher
        )
        return Response(response.json, mimetype='application/json')
