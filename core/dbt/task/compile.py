import os

from dbt.adapters.factory import get_adapter
from dbt.compilation import compile_manifest
from dbt.loader import load_all_projects
from dbt.node_runners import CompileRunner
from dbt.node_types import NodeType
from dbt.parser.models import ModelParser
from dbt.parser.util import ParserUtils
import dbt.ui.printer

from dbt.task.runnable import ManifestTask, GraphRunnableTask, RemoteCallable


class CompileTask(GraphRunnableTask):

    def raise_on_first_error(self):
        return True

    def build_query(self):
        return {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": NodeType.executable(),
            "tags": [],
        }

    def get_runner_type(self):
        return CompileRunner

    def task_end_messages(self, results):
        dbt.ui.printer.print_timestamped_line('Done.')


class RemoteCompileTask(CompileTask, RemoteCallable):
    METHOD_NAME = 'compileNode'

    def __init__(self, args, config):
        super(CompileTask, self).__init__(args, config)
        self.parser = None

    def _runtime_initialize(self):
        ManifestTask._runtime_initialize(self)
        self.parser = ModelParser(
            self.config,
            all_projects=load_all_projects(self.config),
            macro_manifest=self.manifest
        )

    def runtime_cleanup(self, selected_uids):
        """Do some pre-run cleanup that is usually performed in Task __init__.
        """
        self.run_count = 0
        self.num_nodes = len(selected_uids)
        self.node_results = []
        self._skipped_children = {}
        self._skipped_children = {}
        self._raise_next_tick = None

    def handle_request(self, name, sql, timeout=None):
        if self.manifest is None:
            self._runtime_initialize()

        sql = self.decode_sql(sql)
        request_path = os.path.join(self.config.target_path, 'rpc', name)
        node_dict = {
            'name': name,
            'root_path': request_path,
            'resource_type': NodeType.Model,
            'path': name+'.sql',
            'original_file_path': request_path + '.sql',
            'package_name': self.config.project_name,
            'raw_sql': sql,
        }
        unique_id, node = self.parser.parse_sql_node(node_dict)

        # build a new graph + job queue
        manifest = ParserUtils.add_new_refs(self.manifest, self.config, node)
        linker = compile_manifest(self.config, manifest)
        selected_uids = [node.unique_id]
        self.runtime_cleanup(selected_uids)
        self.job_queue = linker.as_graph_queue(manifest, selected_uids)

        # TODO: how can we get a timeout in here? timeouts + threads = sadness!
        result = self.execute_with_hooks(selected_uids)

        # remove the unique ID we added in the run
        del self.manifest.nodes[unique_id]
        return result.serialize()
