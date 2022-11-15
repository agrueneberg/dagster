import inspect
import sys
from contextlib import contextmanager
from types import ModuleType
from typing import Any, Iterable, Mapping, Optional, Union

from dagster import _check as check
from dagster._core.execution.with_resources import with_resources

from .assets import AssetsDefinition, SourceAsset
from .cacheable_assets import CacheableAssetsDefinition
from .decorators import repository
from .job_definition import JobDefinition
from .repository_definition import PendingRepositoryDefinition, RepositoryDefinition
from .resource_definition import ResourceDefinition
from .schedule_definition import ScheduleDefinition
from .sensor_definition import SensorDefinition

MAGIC_REPO_GLOBAL_KEY = "__dagster_repository"

NO_STACK_FRAME_ERROR_MSG = "Python interpreter must support Python stack frames. Python interpreters that are not CPython do not necessarily implement the necessary APIs."


RepositoryDefinitionIsh = Union[PendingRepositoryDefinition, RepositoryDefinition]


class GlobalRepoSingleton:
    _repo_instance: Optional[RepositoryDefinitionIsh] = None

    @classmethod
    def set_instance(cls, repo_instance: RepositoryDefinitionIsh) -> RepositoryDefinitionIsh:
        check.invariant(cls._repo_instance is None, "Cannot set already set global instance")
        cls._repo_instance = repo_instance
        return cls._repo_instance

    @classmethod
    def has_instance(cls) -> bool:
        return cls._repo_instance is not None

    @classmethod
    def get_instance(cls) -> RepositoryDefinitionIsh:
        # forced to tell mypy to ignore because of https://github.com/python/mypy/issues/12009
        return check.not_none(
            cls._repo_instance, "Instance must be set via set_instance"
        )  # type:ignore

    @classmethod
    def clear(cls):
        check.invariant(cls._repo_instance is not None, "Cannot clear empty global repo")
        cls._repo_instance = None


global_repo_singleton = GlobalRepoSingleton()

# invoke this function to get the module name the function that called the current
# scope
def get_module_name_of_caller() -> str:
    # based on https://stackoverflow.com/questions/2000861/retrieve-module-object-from-stack-frame
    # two f_backs to get past get_module_name_of_caller frame

    # Need to do none checking because of:
    # https://docs.python.org/3/library/inspect.html#inspect.currentframe
    # CPython implementation detail: This function relies on Python stack frame
    # support in the interpreter, which isn’t guaranteed to exist in all
    # implementations of Python. If running in an implementation without
    # Python stack frame support this function returns None.

    frame = check.not_none(inspect.currentframe(), NO_STACK_FRAME_ERROR_MSG)
    back_frame = check.not_none(frame.f_back, NO_STACK_FRAME_ERROR_MSG)
    back_back_frame = check.not_none(back_frame.f_back, NO_STACK_FRAME_ERROR_MSG)
    return back_back_frame.f_globals["__name__"]
    # without error checking
    # return inspect.currentframe().f_back.f_back.f_globals["__name__"]


def get_python_env_global_dagster_repository() -> RepositoryDefinitionIsh:
    return global_repo_singleton.get_instance()


@contextmanager
def definitions_test_scope(dundername):
    parent_mod = sys.modules[dundername]
    assert MAGIC_REPO_GLOBAL_KEY not in parent_mod.__dict__
    assert not GlobalRepoSingleton.has_instance()
    try:
        yield
    finally:
        try:
            if MAGIC_REPO_GLOBAL_KEY in parent_mod.__dict__:
                del parent_mod.__dict__[MAGIC_REPO_GLOBAL_KEY]
        finally:
            if GlobalRepoSingleton.has_instance():
                GlobalRepoSingleton.clear()


class DefinitionsAlreadyCalledError(Exception):
    pass


def get_dagster_definitions_in_module(mod: ModuleType):
    return mod.__dict__[MAGIC_REPO_GLOBAL_KEY]


# TODO: Add a new Definitions class to wrap RepositoryDefinition?
def definitions(
    *,
    assets: Optional[
        Iterable[Union[AssetsDefinition, SourceAsset, CacheableAssetsDefinition]]
    ] = None,
    schedules: Optional[Iterable[ScheduleDefinition]] = None,
    sensors: Optional[Iterable[SensorDefinition]] = None,
    jobs: Optional[Iterable[JobDefinition]] = None,
    resources: Optional[Mapping[str, Any]] = None,
) -> RepositoryDefinitionIsh:

    module_name = get_module_name_of_caller()
    mod = sys.modules[module_name]

    if MAGIC_REPO_GLOBAL_KEY in mod.__dict__:
        raise DefinitionsAlreadyCalledError()

    if global_repo_singleton.has_instance():
        raise DefinitionsAlreadyCalledError()

    # This is likely fairly fragile, but this grabs
    # the last component of a module name (typically the name
    # of the file) and uses it for the repository name
    if "." in module_name:
        repo_name = module_name.split(".")[-1]
    else:
        repo_name = module_name

    resource_defs = coerce_resources_to_defs(resources or {})

    # in this case where this is invoked by the raw python interpreter
    # (rather than through dagster CLI or dagit)
    # the name can be "__main__".
    @repository(name=repo_name)
    def global_repo():
        return [
            *with_resources(assets or [], resource_defs),
            *(schedules or []),
            *(sensors or []),
            *(jobs or []),
        ]

    mod.__dict__[MAGIC_REPO_GLOBAL_KEY] = global_repo
    global_repo_singleton.set_instance(global_repo)

    return global_repo


def coerce_resources_to_defs(resources: Mapping[str, Any]) -> Mapping[str, ResourceDefinition]:
    resource_defs = {}
    for key, resource_obj in resources.items():
        resource_defs[key] = (
            resource_obj
            if isinstance(resource_obj, ResourceDefinition)
            else ResourceDefinition.hardcoded_resource(resource_obj)
        )
    return resource_defs
