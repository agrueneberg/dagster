from datetime import datetime
from typing import List, NamedTuple, Optional, Set

from dagster import (
    _check as check,
    asset,
)
from dagster._core.definitions.asset_daemon_context import build_run_requests
from dagster._core.definitions.asset_subset import AssetSubset, ValidAssetSubset
from dagster._core.definitions.definitions_class import Definitions
from dagster._core.definitions.events import AssetKey, CoercibleToAssetKey
from dagster._core.definitions.internal_asset_graph import InternalAssetGraph
from dagster._core.definitions.materialize import materialize
from dagster._core.definitions.partition import PartitionsDefinition, StaticPartitionsDefinition
from dagster._core.definitions.repository_definition.repository_definition import (
    RepositoryDefinition,
)
from dagster._core.definitions.run_request import RunRequest
from dagster._core.instance import DagsterInstance
from dagster._core.reactive_scheduling.scheduling_policy import (
    AssetPartition,
    RequestReaction,
    SchedulingExecutionContext,
    SchedulingPolicy,
    SchedulingResult,
)


def test_include_scheduling_policy() -> None:
    assert SchedulingPolicy


def test_scheduling_policy_parameter() -> None:
    scheduling_policy = SchedulingPolicy()

    @asset(scheduling_policy=scheduling_policy)
    def an_asset() -> None:
        raise Exception("never executed")

    assert an_asset.scheduling_policies_by_key[AssetKey(["an_asset"])] is scheduling_policy

    defs = Definitions([an_asset])
    ak = AssetKey(["an_asset"])
    assert defs.get_assets_def(ak).scheduling_policies_by_key[ak] is scheduling_policy


class ReactiveAssetInfo(NamedTuple):
    asset_key: AssetKey
    scheduling_policy: SchedulingPolicy
    partitions_def: Optional[PartitionsDefinition]


class ReactiveSchedulingGraph(NamedTuple):
    context: SchedulingExecutionContext

    @property
    def instance(self) -> DagsterInstance:
        return self.context.instance

    @property
    def repository_def(self) -> RepositoryDefinition:
        return self.context.repository_def

    @property
    def asset_graph(self) -> InternalAssetGraph:
        return self.context.repository_def.asset_graph

    def get_asset_info(self, asset_key: AssetKey) -> Optional[ReactiveAssetInfo]:
        assets_def = self.asset_graph.get_assets_def(asset_key)
        return (
            ReactiveAssetInfo(
                asset_key=asset_key,
                scheduling_policy=assets_def.scheduling_policies_by_key[asset_key],
                partitions_def=assets_def.partitions_def,
            )
            if asset_key in assets_def.scheduling_policies_by_key
            else None
        )

    def make_valid_subset(
        self,
        asset_key: AssetKey,
        asset_partitions: Optional[Set[AssetPartition]] = None,
    ) -> ValidAssetSubset:
        asset_info = self.get_asset_info(asset_key)
        assert asset_info
        if asset_partitions is not None:
            # explicit partitions. do as you are told
            check.invariant(
                asset_info.partitions_def is not None,
                "If you pass in asset_partitions it must be partitioned asset",
            )
            return AssetSubset.from_asset_partitions_set(
                asset_key=asset_key,
                asset_partitions_set=asset_partitions,
                partitions_def=asset_info.partitions_def,
            )
        else:
            # I think this business logic should be farther up the stack really
            if asset_info.partitions_def is None:
                return AssetSubset(asset_key, True).as_valid(asset_info.partitions_def)
            else:
                return AssetSubset.all(
                    asset_key,
                    asset_info.partitions_def,
                    self.instance,
                    current_time=self.context.evaluation_time,
                )

    def get_parent_asset_subset(
        self, asset_subset: ValidAssetSubset, parent_asset_key: AssetKey
    ) -> ValidAssetSubset:
        parent_assets_def = self.repository_def.assets_defs_by_key[parent_asset_key]
        return self.asset_graph.get_parent_asset_subset(
            child_asset_subset=asset_subset,
            parent_asset_key=parent_asset_key,
            dynamic_partitions_store=self.instance,
            current_time=self.context.evaluation_time,
        ).as_valid(parent_assets_def.partitions_def)


class ReactionSchedulingPlan(NamedTuple):
    requested_partitions: Set[AssetPartition]


def make_asset_partitions(ak: AssetKey, partition_keys: Set[str]) -> Set[AssetPartition]:
    return {AssetPartition(ak, partition_key) for partition_key in partition_keys}


def build_reactive_scheduling_plan(
    scheduling_graph: ReactiveSchedulingGraph,
    starting_key: AssetKey,  # starting asset key
    scheduling_result: SchedulingResult,
) -> ReactionSchedulingPlan:
    starting_subset = scheduling_graph.make_valid_subset(
        starting_key,
        None
        if scheduling_result.partition_keys is None
        else make_asset_partitions(starting_key, scheduling_result.partition_keys),
    )

    # import code

    # code.interact(local=locals())

    upward_requested_partitions = upward_ascent(scheduling_graph, starting_subset)
    return ReactionSchedulingPlan(requested_partitions=upward_requested_partitions)


def upward_ascent(
    graph: ReactiveSchedulingGraph,
    starting_subset: ValidAssetSubset,
) -> Set[AssetPartition]:
    visited: Set[AssetPartition] = set()
    to_execute: Set[AssetPartition] = set()

    def _ascend(current: ValidAssetSubset):
        print(f"_ascend: current: ({current})")
        to_execute.update(current.asset_partitions)
        visited.update(current.asset_partitions)

        for parent_asset_key in graph.asset_graph.get_parents(current.asset_key):
            parent_info = graph.get_asset_info(parent_asset_key)
            if not parent_info:
                continue

            parent_subset = graph.get_parent_asset_subset(current, parent_asset_key)
            included: Set[AssetPartition] = set()
            for asset_partition in parent_subset.asset_partitions:
                parent_reaction = parent_info.scheduling_policy.request_from_downstream(
                    graph.context, asset_partition
                )
                if parent_reaction.include and asset_partition not in visited:
                    included.add(asset_partition)

            requested_subset = graph.make_valid_subset(parent_info.asset_key, included)

            if requested_subset.asset_partitions:
                _ascend(requested_subset)

    _ascend(starting_subset)

    return to_execute


def pulse_policy_on_asset(
    asset_key: CoercibleToAssetKey,
    repository_def: RepositoryDefinition,
    evaluation_time: datetime,
    instance: DagsterInstance,
) -> List[RunRequest]:
    ak = AssetKey.from_coercible(asset_key)
    context = SchedulingExecutionContext(
        repository_def=repository_def, instance=instance, evaluation_time=evaluation_time
    )
    scheduling_graph = ReactiveSchedulingGraph(context=context)
    asset_info = scheduling_graph.get_asset_info(ak)
    if not asset_info:
        return []

    scheduling_result = asset_info.scheduling_policy.schedule(context)

    check.invariant(scheduling_result, "Scheduling policy must return a SchedulingResult")

    if not scheduling_result.launch:
        return []

    scheduling_plan = build_reactive_scheduling_plan(
        scheduling_graph=scheduling_graph, starting_key=ak, scheduling_result=scheduling_result
    )

    return list(
        build_run_requests(
            asset_partitions=scheduling_plan.requested_partitions,
            asset_graph=scheduling_graph.asset_graph,
            run_tags={},
        )
    )


def run_scheduling_pulse_on_asset(
    defs: Definitions,
    asset_key: CoercibleToAssetKey,
    instance: Optional[DagsterInstance] = None,
    evaluation_time: Optional[datetime] = None,
) -> List[RunRequest]:
    return pulse_policy_on_asset(
        asset_key=asset_key,
        repository_def=defs.get_repository_def(),
        instance=instance or DagsterInstance.ephemeral(),
        evaluation_time=evaluation_time or datetime.now(),
    )


def test_never_launch() -> None:
    @asset()
    def never_launching() -> None:
        ...

    definitions = Definitions(assets=[never_launching])

    assert run_scheduling_pulse_on_asset(defs=definitions, asset_key="never_launching") == []


class AlwaysLaunchSchedulingPolicy(SchedulingPolicy):
    def schedule(self, context: SchedulingExecutionContext) -> SchedulingResult:
        return SchedulingResult(launch=True)


class AlwaysDeferSchedulingPolicy(SchedulingPolicy):
    def schedule(self, context: SchedulingExecutionContext) -> SchedulingResult:
        return SchedulingResult(launch=False)

    def request_from_downstream(
        self, context: SchedulingExecutionContext, asset_partition: AssetPartition
    ) -> RequestReaction:
        return RequestReaction(include=True)


def run_request_assets(run_request: RunRequest) -> Set[AssetKey]:
    return set(run_request.asset_selection) if run_request.asset_selection else set()


def asset_key_set(*aks: CoercibleToAssetKey) -> Set[AssetKey]:
    return {AssetKey.from_coercible(ak) for ak in aks}


def test_launch_on_every_tick() -> None:
    @asset(scheduling_policy=AlwaysLaunchSchedulingPolicy())
    def always_launching() -> None:
        ...

    definitions = Definitions(assets=[always_launching])

    assert definitions.get_assets_def("always_launching").scheduling_policies_by_key[
        AssetKey.from_coercible("always_launching")
    ]

    run_requests = run_scheduling_pulse_on_asset(definitions, "always_launching")
    assert len(run_requests) == 1
    assert run_request_assets(run_requests[0]) == asset_key_set("always_launching")


def test_launch_on_every_tick_with_partitioned_upstream() -> None:
    static_partitions_def = StaticPartitionsDefinition(["1", "2"])

    @asset(partitions_def=static_partitions_def, scheduling_policy=AlwaysDeferSchedulingPolicy())
    def up() -> None:
        ...

    @asset(
        partitions_def=static_partitions_def,
        deps=[up],
        scheduling_policy=AlwaysLaunchSchedulingPolicy(),
    )
    def down() -> None:
        ...

    assert materialize([up, down], partition_key="1").success

    defs = Definitions(assets=[up, down])

    run_requests = run_scheduling_pulse_on_asset(defs, "down")

    assert len(run_requests) == 2

    assert run_request_assets(run_requests[0]) == asset_key_set("up", "down")
    assert run_requests[0].partition_key == "1"
    assert run_request_assets(run_requests[1]) == asset_key_set("up", "down")
    assert run_requests[1].partition_key == "2"
    assert not run_scheduling_pulse_on_asset(defs, "up")
