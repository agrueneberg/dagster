from dagster import AutoMaterializeRule
from dagster._core.definitions.auto_materialize_rule import DiscardOnMaxMaterializationsExceededRule

from dagster_tests.definitions_tests.auto_materialize_tests.updated_scenarios.cron_scenarios import (
    basic_hourly_cron_rule,
    get_cron_policy,
)

from ..asset_daemon_scenario import (
    AssetDaemonScenario,
    AssetRuleEvaluationSpec,
    day_partition_key,
    hour_partition_key,
)
from ..base_scenario import (
    run_request,
)
from .asset_daemon_scenario_states import (
    daily_partitions_def,
    hourly_partitions_def,
    one_asset,
    three_assets_in_sequence,
    time_partitions_start_str,
)

cursor_migration_scenarios = [
    AssetDaemonScenario(
        id="one_asset_daily_partitions_never_materialized_respect_discards_migrate_after_discard",
        initial_state=one_asset.with_asset_properties(partitions_def=daily_partitions_def)
        .with_current_time(time_partitions_start_str)
        .with_current_time_advanced(days=30, hours=4)
        .with_all_eager(),
        execution_fn=lambda state: state.evaluate_tick()
        .assert_requested_runs(
            run_request(asset_keys=["A"], partition_key=day_partition_key(state.current_time))
        )
        .assert_evaluation(
            "A",
            [
                AssetRuleEvaluationSpec(
                    AutoMaterializeRule.materialize_on_missing(),
                    [day_partition_key(state.current_time, delta=-i) for i in range(30)],
                ),
                AssetRuleEvaluationSpec(
                    DiscardOnMaxMaterializationsExceededRule(limit=1),
                    [day_partition_key(state.current_time, delta=-i) for i in range(1, 30)],
                ),
            ],
            num_requested=1,
        )
        .with_serialized_cursor(
            # this cursor was generated by running the above scenario before the cursor changes
            """{"latest_storage_id": null, "handled_root_asset_keys": [], "handled_root_partitions_by_asset_key": {"A": "{\\"version\\": 1, \\"time_windows\\": [[1357344000.0, 1359936000.0]], \\"num_partitions\\": 30}"}, "evaluation_id": 1, "last_observe_request_timestamp_by_asset_key": {}, "latest_evaluation_by_asset_key": {"A": "{\\"__class__\\": \\"AutoMaterializeAssetEvaluation\\", \\"asset_key\\": {\\"__class__\\": \\"AssetKey\\", \\"path\\": [\\"A\\"]}, \\"num_discarded\\": 29, \\"num_requested\\": 1, \\"num_skipped\\": 0, \\"partition_subsets_by_condition\\": [[{\\"__class__\\": \\"AutoMaterializeRuleEvaluation\\", \\"evaluation_data\\": null, \\"rule_snapshot\\": {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"MaterializeOnMissingRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.MATERIALIZE\\"}, \\"description\\": \\"materialization is missing\\"}}, {\\"__class__\\": \\"SerializedPartitionsSubset\\", \\"serialized_partitions_def_class_name\\": \\"DailyPartitionsDefinition\\", \\"serialized_partitions_def_unique_id\\": \\"809725ad60ffac0302d5c81f6e45865e21ec0b85\\", \\"serialized_subset\\": \\"{\\\\\\"version\\\\\\": 1, \\\\\\"time_windows\\\\\\": [[1357344000.0, 1359936000.0]], \\\\\\"num_partitions\\\\\\": 30}\\"}], [{\\"__class__\\": \\"AutoMaterializeRuleEvaluation\\", \\"evaluation_data\\": null, \\"rule_snapshot\\": {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"DiscardOnMaxMaterializationsExceededRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.DISCARD\\"}, \\"description\\": \\"exceeds 1 materialization(s) per minute\\"}}, {\\"__class__\\": \\"SerializedPartitionsSubset\\", \\"serialized_partitions_def_class_name\\": \\"DailyPartitionsDefinition\\", \\"serialized_partitions_def_unique_id\\": \\"809725ad60ffac0302d5c81f6e45865e21ec0b85\\", \\"serialized_subset\\": \\"{\\\\\\"version\\\\\\": 1, \\\\\\"time_windows\\\\\\": [[1357344000.0, 1359849600.0]], \\\\\\"num_partitions\\\\\\": 29}\\"}]], \\"rule_snapshots\\": [{\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"MaterializeOnMissingRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.MATERIALIZE\\"}, \\"description\\": \\"materialization is missing\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"SkipOnParentMissingRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.SKIP\\"}, \\"description\\": \\"waiting on upstream data to be present\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"MaterializeOnRequiredForFreshnessRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.MATERIALIZE\\"}, \\"description\\": \\"required to meet this or downstream asset's freshness policy\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"SkipOnParentOutdatedRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.SKIP\\"}, \\"description\\": \\"waiting on upstream data to be up to date\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"SkipOnRequiredButNonexistentParentsRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.SKIP\\"}, \\"description\\": \\"required parent partitions do not exist\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"MaterializeOnParentUpdatedRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.MATERIALIZE\\"}, \\"description\\": \\"upstream data has changed since latest materialization\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"SkipOnBackfillInProgressRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.SKIP\\"}, \\"description\\": \\"targeted by an in-progress backfill\\"}], \\"run_ids\\": {\\"__set__\\": []}}"}, "latest_evaluation_timestamp": 1359950400.0}
            """
        )
        .evaluate_tick("a")
        # the new cursor "remembers" that a bunch of partitions were discarded
        .assert_requested_runs(),
    ),
    AssetDaemonScenario(
        id="one_asset_daily_partitions_two_years_never_materialized_migrate_after_run_requested",
        initial_state=one_asset.with_asset_properties(partitions_def=daily_partitions_def)
        .with_current_time(time_partitions_start_str)
        .with_current_time_advanced(years=2, hours=4)
        .with_all_eager(),
        execution_fn=lambda state: state.evaluate_tick()
        .assert_requested_runs(
            run_request(asset_keys=["A"], partition_key=day_partition_key(state.current_time))
        )
        .with_serialized_cursor(
            # this cursor was generate by running the above scenario before the cursor changes
            """
{"latest_storage_id": null, "handled_root_asset_keys": [], "handled_root_partitions_by_asset_key": {"A": "{\\"version\\": 1, \\"time_windows\\": [[1357344000.0, 1420416000.0]], \\"num_partitions\\": 730}"}, "evaluation_id": 1, "last_observe_request_timestamp_by_asset_key": {}, "latest_evaluation_by_asset_key": {"A": "{\\"__class__\\": \\"AutoMaterializeAssetEvaluation\\", \\"asset_key\\": {\\"__class__\\": \\"AssetKey\\", \\"path\\": [\\"A\\"]}, \\"num_discarded\\": 729, \\"num_requested\\": 1, \\"num_skipped\\": 0, \\"partition_subsets_by_condition\\": [[{\\"__class__\\": \\"AutoMaterializeRuleEvaluation\\", \\"evaluation_data\\": null, \\"rule_snapshot\\": {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"MaterializeOnMissingRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.MATERIALIZE\\"}, \\"description\\": \\"materialization is missing\\"}}, {\\"__class__\\": \\"SerializedPartitionsSubset\\", \\"serialized_partitions_def_class_name\\": \\"DailyPartitionsDefinition\\", \\"serialized_partitions_def_unique_id\\": \\"809725ad60ffac0302d5c81f6e45865e21ec0b85\\", \\"serialized_subset\\": \\"{\\\\\\"version\\\\\\": 1, \\\\\\"time_windows\\\\\\": [[1357344000.0, 1420416000.0]], \\\\\\"num_partitions\\\\\\": 730}\\"}], [{\\"__class__\\": \\"AutoMaterializeRuleEvaluation\\", \\"evaluation_data\\": null, \\"rule_snapshot\\": {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"DiscardOnMaxMaterializationsExceededRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.DISCARD\\"}, \\"description\\": \\"exceeds 1 materialization(s) per minute\\"}}, {\\"__class__\\": \\"SerializedPartitionsSubset\\", \\"serialized_partitions_def_class_name\\": \\"DailyPartitionsDefinition\\", \\"serialized_partitions_def_unique_id\\": \\"809725ad60ffac0302d5c81f6e45865e21ec0b85\\", \\"serialized_subset\\": \\"{\\\\\\"version\\\\\\": 1, \\\\\\"time_windows\\\\\\": [[1357344000.0, 1420329600.0]], \\\\\\"num_partitions\\\\\\": 729}\\"}]], \\"rule_snapshots\\": [{\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"SkipOnParentMissingRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.SKIP\\"}, \\"description\\": \\"waiting on upstream data to be present\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"MaterializeOnRequiredForFreshnessRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.MATERIALIZE\\"}, \\"description\\": \\"required to meet this or downstream asset's freshness policy\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"MaterializeOnParentUpdatedRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.MATERIALIZE\\"}, \\"description\\": \\"upstream data has changed since latest materialization\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"SkipOnBackfillInProgressRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.SKIP\\"}, \\"description\\": \\"targeted by an in-progress backfill\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"SkipOnParentOutdatedRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.SKIP\\"}, \\"description\\": \\"waiting on upstream data to be up to date\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"MaterializeOnMissingRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.MATERIALIZE\\"}, \\"description\\": \\"materialization is missing\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"SkipOnRequiredButNonexistentParentsRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.SKIP\\"}, \\"description\\": \\"required parent partitions do not exist\\"}], \\"run_ids\\": {\\"__set__\\": []}}"}, "latest_evaluation_timestamp": 1420430400.0}
"""
        )
        .evaluate_tick()
        # the new cursor should not kick off a new run because the previous tick already requested one
        .assert_requested_runs(),
    ),
    AssetDaemonScenario(
        id="partitioned_non_root_asset_missing_after_migrate",
        initial_state=three_assets_in_sequence.with_asset_properties(
            partitions_def=daily_partitions_def
        )
        .with_current_time(time_partitions_start_str)
        .with_current_time_advanced(days=10, hours=4)
        .with_all_eager(),
        execution_fn=lambda state: state.evaluate_tick()
        .assert_requested_runs(
            run_request(
                asset_keys=["A", "B", "C"], partition_key=day_partition_key(state.current_time)
            )
        )
        # materialize the previous day's partitions manually
        .with_runs(
            run_request(
                asset_keys=["A", "B", "C"],
                partition_key=day_partition_key(state.current_time, delta=-1),
            )
        )
        .evaluate_tick()
        .assert_requested_runs()
        # now update the cursor -- this serialized cursor does not contain any information about
        # the missing partitions for B or C, because we used to only track this information for
        # root assets. B or C also has not been materialized since the previous tick
        .with_serialized_cursor(
            """{"latest_storage_id": 24, "handled_root_asset_keys": [], "handled_root_partitions_by_asset_key": {"A": "{\\"version\\": 1, \\"time_windows\\": [[1357344000.0, 1358208000.0]], \\"num_partitions\\": 10}", "B": "{\\"version\\": 1, \\"time_windows\\": [], \\"num_partitions\\": 0}", "C": "{\\"version\\": 1, \\"time_windows\\": [], \\"num_partitions\\": 0}"}, "evaluation_id": 2, "last_observe_request_timestamp_by_asset_key": {}, "latest_evaluation_by_asset_key": {}, "latest_evaluation_timestamp": 1358222400.164996}"""
        )
        .evaluate_tick()
        # when getting the new cursor, we should realize that B and C are not missing any partitions
        # that can be materialized
        .assert_requested_runs(),
    ),
    AssetDaemonScenario(
        id="basic_hourly_cron_unpartitioned_migrate",
        initial_state=one_asset.with_asset_properties(
            auto_materialize_policy=get_cron_policy(basic_hourly_cron_rule)
        ).with_current_time("2020-01-01T00:05"),
        execution_fn=lambda state: state.evaluate_tick()
        .assert_requested_runs(run_request(["A"]))
        .assert_evaluation("A", [AssetRuleEvaluationSpec(basic_hourly_cron_rule)])
        # next tick should not request any more runs
        .with_current_time_advanced(seconds=30)
        .evaluate_tick()
        .assert_requested_runs()
        # still no runs should be requested
        .with_current_time_advanced(minutes=50)
        .evaluate_tick()
        .assert_requested_runs()
        # moved to a new cron schedule tick, request another run
        .with_current_time_advanced(minutes=10)
        .evaluate_tick()
        .assert_requested_runs(run_request(["A"]))
        .assert_evaluation("A", [AssetRuleEvaluationSpec(basic_hourly_cron_rule)])
        # next tick should not request any more runs
        .with_serialized_cursor(
            """{"latest_storage_id": null, "handled_root_asset_keys": ["A"], "handled_root_partitions_by_asset_key": {}, "evaluation_id": 4, "last_observe_request_timestamp_by_asset_key": {}, "latest_evaluation_by_asset_key": {"A": "{\\"__class__\\": \\"AutoMaterializeAssetEvaluation\\", \\"asset_key\\": {\\"__class__\\": \\"AssetKey\\", \\"path\\": [\\"A\\"]}, \\"num_discarded\\": 0, \\"num_requested\\": 1, \\"num_skipped\\": 0, \\"partition_subsets_by_condition\\": [[{\\"__class__\\": \\"AutoMaterializeRuleEvaluation\\", \\"evaluation_data\\": null, \\"rule_snapshot\\": {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"MaterializeOnCronRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.MATERIALIZE\\"}, \\"description\\": \\"not materialized since last cron schedule tick of '0 * * * *' (timezone: UTC)\\"}}, null]], \\"rule_snapshots\\": [{\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"MaterializeOnCronRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.MATERIALIZE\\"}, \\"description\\": \\"not materialized since last cron schedule tick of '0 * * * *' (timezone: UTC)\\"}, {\\"__class__\\": \\"AutoMaterializeRuleSnapshot\\", \\"class_name\\": \\"SkipOnNotAllParentsUpdatedRule\\", \\"decision_type\\": {\\"__enum__\\": \\"AutoMaterializeDecisionType.SKIP\\"}, \\"description\\": \\"waiting on upstream data to be updated\\"}], \\"run_ids\\": {\\"__set__\\": []}}"}, "latest_evaluation_timestamp": 1577840730.0}"""
        )
        .with_current_time_advanced(seconds=30)
        .evaluate_tick()
        .assert_requested_runs(),
    ),
    AssetDaemonScenario(
        id="basic_hourly_cron_partitioned_migrate",
        initial_state=one_asset.with_asset_properties(
            partitions_def=hourly_partitions_def,
            auto_materialize_policy=get_cron_policy(basic_hourly_cron_rule),
        )
        .with_current_time(time_partitions_start_str)
        .with_current_time_advanced(days=1, minutes=5),
        execution_fn=lambda state: state.evaluate_tick()
        .assert_requested_runs(run_request(["A"], hour_partition_key(state.current_time)))
        .assert_evaluation(
            "A",
            [
                AssetRuleEvaluationSpec(
                    basic_hourly_cron_rule, [hour_partition_key(state.current_time)]
                )
            ],
        )
        # next tick should not request any more runs
        .with_current_time_advanced(seconds=30)
        .evaluate_tick()
        .assert_requested_runs()
        # still no runs should be requested
        .with_current_time_advanced(minutes=50)
        .evaluate_tick()
        .assert_requested_runs()
        # moved to a new cron schedule tick, request another run for the new partition
        .with_current_time_advanced(minutes=10)
        .evaluate_tick(
            """{"latest_storage_id": null, "handled_root_asset_keys": [], "handled_root_partitions_by_asset_key": {"A": "{\"version\": 1, \"time_windows\": [[1357426800.0, 1357430400.0]], \"num_partitions\": 1}"}, "evaluation_id": 2, "last_observe_request_timestamp_by_asset_key": {}, "latest_evaluation_by_asset_key": {}, "latest_evaluation_timestamp": 1357430730.0}"""
        )
        .assert_requested_runs(run_request(["A"], hour_partition_key(state.current_time, 1))),
    ),
]
