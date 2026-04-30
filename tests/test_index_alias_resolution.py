"""Index-alias resolution across atomic-op kwargs.

The library lets a model declare a logical attribute (the "alias") that maps
onto one or more underlying DynamoDB index keys via IndexMap. When a user
passes an alias name into an atomic-op kwarg (e.g., set, number_add), the
helper expands the clause to write each underlying index key — and, when
store_aliases=True, the alias attribute itself — so GSIs cannot silently go
stale.
"""

import asyncio

import pytest
from unittest.mock import MagicMock

from dynamojo.base import DynamojoBase
from dynamojo.config import DynamojoConfig
from dynamojo.index import Gsi, IndexList, IndexMap, TableIndex

_table_index = TableIndex(name="table", partitionkey="PK", sortkey="SK")
_gsi0 = Gsi(name="gsi0", partitionkey="gsi0_pk", sortkey="gsi0_sk")
_gsi1 = Gsi(name="gsi1", partitionkey="gsi1_pk", sortkey="gsi1_sk")
_indexes = IndexList(_table_index, _gsi0, _gsi1)


def make_model(*, store_aliases: bool = True, double_alias_pk: bool = False):
    """Build a model whose ``user_id`` attribute aliases gsi0_pk (and gsi1_pk
    when ``double_alias_pk`` is set), and whose ``event_at`` aliases gsi0_sk.

    ``entity_id`` / ``sort_key`` alias the table PK / SK — used to test the
    instance-update path where the atomic op touches the table key.
    """
    mock_client = MagicMock()
    mock_client.update_item.return_value = {}

    index_maps = [
        IndexMap(index=_table_index, partitionkey="entity_id", sortkey="sort_key"),
        IndexMap(index=_gsi0, partitionkey="user_id", sortkey="event_at"),
    ]
    if double_alias_pk:
        index_maps.append(
            IndexMap(index=_gsi1, partitionkey="user_id", sortkey="gsi1_sk_field")
        )

    config = DynamojoConfig(
        indexes=_indexes,
        index_maps=index_maps,
        joined_attributes=[],
        table="test-table",
        dynamo_client=mock_client,
        store_aliases=store_aliases,
    )

    class TestItem(DynamojoBase):
        entity_id: str
        sort_key: str
        user_id: str
        event_at: str
        keys: list[str]
        count: int
        name: str

        @classmethod
        def _config(cls):
            return config

    return TestItem, mock_client


def run(coro):
    return asyncio.run(coro)


def update_kwargs(client):
    return client.update_item.call_args.kwargs


# ===========================================================================
# _resolve_alias_targets — the resolver in isolation
# ===========================================================================


class TestResolveAliasTargets:
    def test_non_aliased_field_passthrough(self):
        Model, _ = make_model()
        assert Model._resolve_alias_targets("count") == ["count"]

    def test_aliased_field_with_store_aliases_true(self):
        Model, _ = make_model(store_aliases=True)
        targets = Model._resolve_alias_targets("user_id")
        assert set(targets) == {"gsi0_pk", "user_id"}

    def test_aliased_field_with_store_aliases_false(self):
        Model, _ = make_model(store_aliases=False)
        targets = Model._resolve_alias_targets("user_id")
        assert targets == ["gsi0_pk"]

    def test_field_aliasing_multiple_indexes(self):
        Model, _ = make_model(double_alias_pk=True, store_aliases=True)
        targets = Model._resolve_alias_targets("user_id")
        assert set(targets) == {"gsi0_pk", "gsi1_pk", "user_id"}

    def test_field_aliasing_multiple_indexes_no_alias_storage(self):
        Model, _ = make_model(double_alias_pk=True, store_aliases=False)
        targets = Model._resolve_alias_targets("user_id")
        assert set(targets) == {"gsi0_pk", "gsi1_pk"}

    def test_index_key_supplied_directly_returns_as_is(self):
        """Passing the index key name itself (an unusual pattern) — not expanded."""
        Model, _ = make_model()
        assert Model._resolve_alias_targets("gsi0_pk") == ["gsi0_pk"]


# ===========================================================================
# `set` kwarg — the canonical case for alias resolution
# ===========================================================================


class TestSetWithAliasResolution:
    def test_set_aliased_field_writes_both_alias_and_index_key(self):
        Model, client = make_model(store_aliases=True)
        run(Model.update_by_key("eid-1", "sk-1", set={"user_id": "u-42"}))
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        assert "#gsi0_pk = :_set_gsi0_pk" in expr
        assert "#user_id = :_set_user_id" in expr
        # both placeholders carry the same value
        assert kw["ExpressionAttributeValues"][":_set_gsi0_pk"] == {"S": "u-42"}
        assert kw["ExpressionAttributeValues"][":_set_user_id"] == {"S": "u-42"}

    def test_set_aliased_field_with_store_aliases_false(self):
        Model, client = make_model(store_aliases=False)
        run(Model.update_by_key("eid-1", "sk-1", set={"user_id": "u-42"}))
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        assert "#gsi0_pk = :_set_gsi0_pk" in expr
        # the alias attribute itself must NOT be written
        assert "#user_id" not in kw["ExpressionAttributeNames"]
        assert ":_set_user_id" not in kw["ExpressionAttributeValues"]

    def test_set_non_aliased_field_unchanged(self):
        Model, client = make_model()
        run(Model.update_by_key("eid-1", "sk-1", set={"name": "Alice"}))
        kw = update_kwargs(client)
        assert kw["UpdateExpression"].strip() == "SET #name = :_set_name"

    def test_set_field_aliasing_multiple_gsis(self):
        Model, client = make_model(double_alias_pk=True, store_aliases=True)
        run(Model.update_by_key("eid-1", "sk-1", set={"user_id": "u-42"}))
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        # all three targets get a SET clause
        assert "#gsi0_pk = :_set_gsi0_pk" in expr
        assert "#gsi1_pk = :_set_gsi1_pk" in expr
        assert "#user_id = :_set_user_id" in expr
        for placeholder in (":_set_gsi0_pk", ":_set_gsi1_pk", ":_set_user_id"):
            assert kw["ExpressionAttributeValues"][placeholder] == {"S": "u-42"}


# ===========================================================================
# Other atomic ops — alias resolution applies uniformly
# ===========================================================================


class TestOtherAtomicOpsWithAliasResolution:
    def test_set_if_not_exists_aliased_field(self):
        Model, client = make_model(store_aliases=True)
        run(Model.update_by_key("eid-1", "sk-1", set_if_not_exists={"user_id": "u-42"}))
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        assert "#gsi0_pk = if_not_exists(#gsi0_pk, :_ine_gsi0_pk)" in expr
        assert "#user_id = if_not_exists(#user_id, :_ine_user_id)" in expr

    def test_number_add_aliased_field(self):
        """Unusual but legal — alias resolution still applies."""
        Model, client = make_model(store_aliases=True)
        run(Model.update_by_key("eid-1", "sk-1", number_add={"user_id": 1}))
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        assert "ADD #gsi0_pk :_na_gsi0_pk" in expr
        assert "#user_id :_na_user_id" in expr

    def test_list_append_aliased_field(self):
        Model, client = make_model(store_aliases=True)
        run(Model.update_by_key("eid-1", "sk-1", list_append={"user_id": ["x"]}))
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        assert "#gsi0_pk = list_append(#gsi0_pk, :_la_gsi0_pk)" in expr
        assert "#user_id = list_append(#user_id, :_la_user_id)" in expr


# ===========================================================================
# Instance update() — alias resolution flows through make_update_opts
# ===========================================================================


class TestInstanceUpdateWithAliasResolution:
    def _instance(self, **overrides):
        defaults = dict(store_aliases=True)
        defaults.update(overrides)
        Model, client = make_model(**defaults)
        item = Model(
            entity_id="eid-1",
            sort_key="sk-1",
            user_id="u-1",
            event_at="2026-01-01",
            keys=["a"],
            count=1,
            name="Alice",
        )
        return item, client

    def test_instance_update_set_alias(self):
        item, client = self._instance()
        run(item.update(set={"user_id": "u-42"}))
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        assert "#gsi0_pk = :_set_gsi0_pk" in expr
        assert "#user_id = :_set_user_id" in expr
        # local state on the alias is synced
        assert item.user_id == "u-42"

    def test_instance_update_number_add_on_non_aliased_field(self):
        """Backward-compat: existing behavior unchanged for non-aliased fields."""
        item, client = self._instance()
        run(item.update(number_add={"count": 3}))
        kw = update_kwargs(client)
        assert "ADD #count :_na_count" in kw["UpdateExpression"]
        assert item.count == 4


# ===========================================================================
# ConditionExpression / FilterExpression — alias canonicalization
#
# Without this, conditions targeting an alias would silently fail closed
# under store_aliases=False (the alias attribute isn't on the row), and
# only "happen to work" under store_aliases=True due to dual-writing.
# Translation makes the behavior intentional rather than coincidental.
# ===========================================================================


class TestConditionExpressionAliasCanonicalization:
    def test_resolver_returns_canonical_for_alias(self):
        Model, _ = make_model()
        # `user_id` aliases `gsi0_pk` — canonical form is the underlying key.
        assert Model._resolve_alias_to_canonical("user_id") == "gsi0_pk"

    def test_resolver_passes_non_aliased_through(self):
        Model, _ = make_model()
        assert Model._resolve_alias_to_canonical("count") == "count"

    def test_resolver_picks_first_target_when_alias_maps_to_multiple(self):
        """Iteration order of _index_aliases follows IndexMap declaration order,
        so the GSI declared first wins — deterministic, table-key-first when
        the table key shares the alias."""
        Model, _ = make_model(double_alias_pk=True)
        # Both gsi0_pk and gsi1_pk alias `user_id`. gsi0 was declared first.
        assert Model._resolve_alias_to_canonical("user_id") == "gsi0_pk"

    def test_condition_expression_translates_alias_to_underlying(self):
        from boto3.dynamodb.conditions import Attr

        Model, client = make_model(store_aliases=False)
        run(
            Model.update_by_key(
                "eid-1",
                "sk-1",
                set={"name": "Alice"},
                ConditionExpression=Attr("user_id").eq("u-42"),
            )
        )
        kw = update_kwargs(client)
        # The alias `user_id` must be translated to `gsi0_pk` in the condition
        names = kw["ExpressionAttributeNames"]
        # The condition placeholder should map to the physical key, not the alias
        condition_targeted_names = {
            v for k, v in names.items() if k.startswith("#condition_attribute_name")
        }
        assert "gsi0_pk" in condition_targeted_names
        assert "user_id" not in condition_targeted_names

    def test_condition_expression_works_with_store_aliases_true(self):
        """With store_aliases=True the prior behavior 'happened to work' by
        coincidence; the translation makes it intentional."""
        from boto3.dynamodb.conditions import Attr

        Model, client = make_model(store_aliases=True)
        run(
            Model.update_by_key(
                "eid-1",
                "sk-1",
                set={"name": "Alice"},
                ConditionExpression=Attr("user_id").eq("u-42"),
            )
        )
        kw = update_kwargs(client)
        names = kw["ExpressionAttributeNames"]
        condition_targeted_names = {
            v for k, v in names.items() if k.startswith("#condition_attribute_name")
        }
        assert "gsi0_pk" in condition_targeted_names

    def test_condition_expression_non_aliased_passthrough(self):
        from boto3.dynamodb.conditions import Attr

        Model, client = make_model()
        run(
            Model.update_by_key(
                "eid-1",
                "sk-1",
                set={"count": 1},
                ConditionExpression=Attr("name").eq("Alice"),
            )
        )
        kw = update_kwargs(client)
        names = kw["ExpressionAttributeNames"]
        condition_targeted_names = {
            v for k, v in names.items() if k.startswith("#condition_attribute_name")
        }
        assert condition_targeted_names == {"name"}

    def test_delete_by_key_condition_translates_alias(self):
        from boto3.dynamodb.conditions import Attr

        Model, client = make_model(store_aliases=False)
        run(
            Model.delete_by_key(
                "eid-1",
                "sk-1",
                ConditionExpression=Attr("user_id").eq("u-42"),
            )
        )
        kw = client.delete_item.call_args.kwargs
        names = kw["ExpressionAttributeNames"]
        condition_targeted_names = {
            v for k, v in names.items() if k.startswith("#condition_attribute_name")
        }
        assert "gsi0_pk" in condition_targeted_names
        assert "user_id" not in condition_targeted_names

    def test_filter_expression_in_query_translates_alias(self):
        """FilterExpressions in queries go through the same code path."""
        from boto3.dynamodb.conditions import Attr, Key

        Model, client = make_model()
        client.query.return_value = {
            "Items": [],
            "Count": 0,
            "ScannedCount": 0,
            "ResponseMetadata": {},
        }
        run(
            Model.query(
                KeyConditionExpression=Key("entity_id").eq("eid-1"),
                FilterExpression=Attr("user_id").eq("u-42"),
            )
        )
        kw = client.query.call_args.kwargs
        names = kw["ExpressionAttributeNames"]
        filter_targeted_names = {
            v for k, v in names.items() if k.startswith("#attribute_name")
        }
        assert "gsi0_pk" in filter_targeted_names
        assert "user_id" not in filter_targeted_names
