from typing import Optional, Union

from dagster import (
    AssetsDefinition,
    AssetSpec,
    _check as check,
)
from dagster._core.definitions.utils import VALID_NAME_REGEX

MIGRATED_TAG = "airlift/task_migrated"
DAG_ID_TAG = "airlift/dag_id"
TASK_ID_TAG = "airlift/task_id"


def convert_to_valid_dagster_name(name: str) -> str:
    """Converts a name to a valid dagster name by replacing invalid characters with underscores. / is converted to a double underscore."""
    return "".join(c if VALID_NAME_REGEX.match(c) else "__" if c == "/" else "_" for c in name)


def get_task_id_from_asset(assets_def: AssetsDefinition) -> Optional[str]:
    return _get_prop_from_asset(assets_def, TASK_ID_TAG, 1)


def get_dag_id_from_asset(assets_def: AssetsDefinition) -> Optional[str]:
    return _get_prop_from_asset(assets_def, DAG_ID_TAG, 0)


def _get_prop_from_asset(
    assets_def: AssetsDefinition, prop_tag: str, position: int
) -> Optional[str]:
    prop_from_asset_tags = prop_from_tags(assets_def, prop_tag)
    if not assets_def.is_executable:
        return prop_from_asset_tags

    prop_from_op_tags = None
    if assets_def.node_def.tags and prop_tag in assets_def.node_def.tags:
        prop_from_op_tags = assets_def.node_def.tags[prop_tag]
    prop_from_name = None
    if len(assets_def.node_def.name.split("__")) == 2:
        prop_from_name = assets_def.node_def.name.split("__")[position]
    if prop_from_asset_tags and prop_from_op_tags:
        check.invariant(
            prop_from_asset_tags == prop_from_op_tags,
            f"ID mismatch between asset tags and op tags: {prop_from_asset_tags} != {prop_from_op_tags}",
        )
    if prop_from_asset_tags and prop_from_name:
        check.invariant(
            prop_from_asset_tags == prop_from_name,
            f"ID mismatch between tags and name: {prop_from_asset_tags} != {prop_from_name}",
        )
    if prop_from_op_tags and prop_from_name:
        check.invariant(
            prop_from_op_tags == prop_from_name,
            f"ID mismatch between op tags and name: {prop_from_op_tags} != {prop_from_name}",
        )
    return prop_from_asset_tags or prop_from_op_tags or prop_from_name


def prop_from_tags(asset: Union[AssetsDefinition, AssetSpec], prop_tag: str) -> Optional[str]:
    specs = asset.specs if isinstance(asset, AssetsDefinition) else [asset]
    if any(prop_tag in spec.tags for spec in specs):
        prop = None
        for spec in specs:
            if prop is None:
                prop = spec.tags[prop_tag]
            else:
                if spec.tags.get(prop_tag) is None:
                    asset_name = (
                        asset.node_def.name
                        if isinstance(asset, AssetsDefinition) and asset.is_executable
                        else asset.key.to_user_string()
                    )
                    check.failed(
                        f"Missing {prop_tag} tag in spec {spec.key} for asset {asset_name}."
                    )
                check.invariant(
                    prop == spec.tags[prop_tag],
                    f"Task ID mismatch within same AssetsDefinition: {prop} != {spec.tags[prop_tag]}",
                )
        return prop
    return None
