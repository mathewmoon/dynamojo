"""``fail_on_missing`` on update() — the counterpart to save()'s ``fail_on_exists``.

UpdateItem upserts by default. Without a condition that a missing item cannot
satisfy, DynamoDB creates a row holding only the key and the SET targets, which
then fails model validation on the next read. These tests pin that the guard is
present by default, that it composes with a caller-supplied condition, and that
it still bites when the caller's own condition is one a missing item *would*
satisfy.
"""

import asyncio
import re

from unittest.mock import MagicMock

from boto3.dynamodb.conditions import Attr

from dynamojo.base import DynamojoBase
from dynamojo.config import DynamojoConfig
from dynamojo.index import IndexList, IndexMap, TableIndex

_composite_index = TableIndex(name="table", partitionkey="PK", sortkey="SK")
_pk_only_index = TableIndex(name="table", partitionkey="PK")


def make_item(*, composite=True):
    """Return a (model instance, mock client) pair."""
    table_index = _composite_index if composite else _pk_only_index
    index_map = (
        IndexMap(index=table_index, partitionkey="entity_id", sortkey="sort_key")
        if composite
        else IndexMap(index=table_index, partitionkey="entity_id")
    )
    mock_client = MagicMock()
    config = DynamojoConfig(
        indexes=IndexList(table_index),
        index_maps=[index_map],
        joined_attributes=[],
        table="test-table",
        dynamo_client=mock_client,
    )

    class TestItem(DynamojoBase):
        entity_id: str
        sort_key: str = "sk-1"
        name: str
        count: int = 0

        @classmethod
        def _config(cls):
            return config

    instance = TestItem(entity_id="eid-1", sort_key="sk-1", name="Alice")
    return instance, mock_client


def run(coro):
    return asyncio.run(coro)


def call_kwargs(mock_client):
    return mock_client.update_item.call_args.kwargs


def resolved_condition(mock_client):
    """The rendered ConditionExpression with placeholders substituted back.

    boto3 numbers its own placeholders (``#n0``, ``:v0``) from zero per render,
    so asserting on the literal rendered string would pin an implementation
    detail. Resolving through ``ExpressionAttributeNames`` asserts the meaning
    and simultaneously proves every placeholder is actually declared.
    """
    kw = call_kwargs(mock_client)
    cond = kw["ConditionExpression"]
    for placeholder, attr in sorted(
        kw.get("ExpressionAttributeNames", {}).items(),
        key=lambda kv: -len(kv[0]),
    ):
        cond = cond.replace(placeholder, attr)
    return cond


class TestDefaultGuard:
    def test_composite_key_guard_is_added_by_default(self):
        item, client = make_item()
        item.name = "Bob"
        run(item.update())
        assert resolved_condition(client) == (
            "(attribute_exists(PK) AND attribute_exists(SK))"
        )

    def test_key_names_are_placeholdered_not_inlined(self):
        """Reserved-word safety: boto3 must alias the key names, not inline them."""
        item, client = make_item()
        item.name = "Bob"
        run(item.update())
        kw = call_kwargs(client)
        assert "attribute_exists(PK)" not in kw["ConditionExpression"]
        assert set(kw["ExpressionAttributeNames"].values()) >= {"PK", "SK"}

    def test_pk_only_table_guards_only_the_partition_key(self):
        item, client = make_item(composite=False)
        item.name = "Bob"
        run(item.update())
        assert resolved_condition(client) == "attribute_exists(PK)"
        assert "SK" not in call_kwargs(client)["ExpressionAttributeNames"].values()

    def test_guard_applies_to_atomic_ops_too(self):
        item, client = make_item()
        run(item.update(number_add={"count": 1}))
        assert "attribute_exists(PK)" in resolved_condition(client)


class TestOptOut:
    def test_no_guard_when_disabled(self):
        item, client = make_item()
        item.name = "Bob"
        run(item.update(fail_on_missing=False))
        assert "ConditionExpression" not in call_kwargs(client)

    def test_disabled_leaves_a_caller_condition_untouched(self):
        item, client = make_item()
        item.name = "Bob"
        run(
            item.update(
                ConditionExpression=Attr("name").eq("Alice"), fail_on_missing=False
            )
        )
        cond = resolved_condition(client)
        assert "attribute_exists" not in cond
        assert "name = " in cond

    def test_disabled_is_not_leaked_into_the_boto3_call(self):
        item, client = make_item()
        item.name = "Bob"
        run(item.update(fail_on_missing=False))
        assert "fail_on_missing" not in call_kwargs(client)


class TestComposition:
    def test_guard_is_anded_with_a_caller_condition(self):
        item, client = make_item()
        item.name = "Bob"
        run(item.update(ConditionExpression=Attr("count").eq(0)))
        cond = resolved_condition(client)
        assert "attribute_exists(PK)" in cond
        assert "attribute_exists(SK)" in cond
        assert "count = " in cond
        assert cond.count("attribute_exists") == 2

    def test_every_placeholder_is_declared(self):
        """A single render is what guarantees this; two renders would collide."""
        item, client = make_item()
        item.name = "Bob"
        run(item.update(ConditionExpression=Attr("count").eq(0)))
        kw = call_kwargs(client)
        for token in re.findall(r"[#:][A-Za-z0-9_]+", kw["ConditionExpression"]):
            table = (
                kw["ExpressionAttributeNames"]
                if token.startswith("#")
                else kw.get("ExpressionAttributeValues", {})
            )
            assert token in table, f"{token} referenced but not declared"

    def test_caller_placeholders_are_not_clobbered_by_the_guard(self):
        """The collision the single-render composition exists to prevent.

        Rendering the guard separately would restart boto3's counter at ``#n0``
        and overwrite the caller's first placeholder, silently retargeting their
        condition at a key attribute.
        """
        item, client = make_item()
        item.name = "Bob"
        run(item.update(ConditionExpression=Attr("count").eq(0)))
        kw = call_kwargs(client)
        # "count" must still be reachable through the name map — if the guard had
        # overwritten its placeholder, only PK/SK would remain.
        assert "count" in kw["ExpressionAttributeNames"].values()
        assert len(set(kw["ExpressionAttributeNames"].values())) >= 3

    def test_guard_bites_when_the_caller_condition_admits_a_missing_item(self):
        """The regression this parameter exists for.

        A condition built only from ``attribute_not_exists`` arms is *satisfied*
        by an item that does not exist, so on its own it authorises the very
        create it was written to prevent. The guard has to be present for the
        composed expression to fail closed.
        """
        item, client = make_item()
        item.name = "Bob"
        run(item.update(ConditionExpression=Attr("count").not_exists()))
        cond = resolved_condition(client)
        assert "attribute_exists(PK)" in cond
        assert "attribute_not_exists(count)" in cond


class TestUnchangedPaths:
    def test_no_write_at_all_when_nothing_changed(self):
        item, client = make_item()
        assert run(item.update()) is None
        client.update_item.assert_not_called()

    def test_save_still_guards_on_existence_the_other_way(self):
        item, client = make_item()
        run(item.save())
        cond = client.put_item.call_args.kwargs["ConditionExpression"]
        assert "attribute_not_exists" in cond
