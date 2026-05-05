"""drop_null_index_keys — sparse-index null elision.

When a model's index-key alias is set to None, the underlying DynamoDB index
key must be dropped from the write so that sparse-index validation does not
trip on a NULL. The alias attribute itself, however, is *not* an index key
and so remains on the row as the user declared it. The behaviour is gated by
``DynamojoConfig.drop_null_index_keys`` (default True).
"""

import asyncio
from typing import Optional

from unittest.mock import MagicMock

from dynamojo.base import DynamojoBase
from dynamojo.config import DynamojoConfig
from dynamojo.index import Gsi, IndexList, IndexMap, TableIndex


_table_index = TableIndex(name="table", partitionkey="PK", sortkey="SK")
_gsi0 = Gsi(name="gsi0", partitionkey="gsi0_pk", sortkey="gsi0_sk")
_indexes = IndexList(_table_index, _gsi0)


def make_model(*, store_aliases: bool = True, drop_null_index_keys: bool = True):
    mock_client = MagicMock()
    mock_client.update_item.return_value = {}
    mock_client.put_item.return_value = {}

    config = DynamojoConfig(
        indexes=_indexes,
        index_maps=[
            IndexMap(index=_table_index, partitionkey="entity_id", sortkey="sort_key"),
            IndexMap(index=_gsi0, partitionkey="user_id", sortkey="event_at"),
        ],
        joined_attributes=[],
        table="test-table",
        dynamo_client=mock_client,
        store_aliases=store_aliases,
        drop_null_index_keys=drop_null_index_keys,
    )

    class TestItem(DynamojoBase):
        entity_id: str
        sort_key: str
        user_id: Optional[str] = None
        event_at: Optional[str] = None
        name: Optional[str] = None

        @classmethod
        def _config(cls):
            return config

    return TestItem, mock_client


def run(coro):
    return asyncio.run(coro)


def update_kwargs(client):
    return client.update_item.call_args.kwargs


def put_kwargs(client):
    return client.put_item.call_args.kwargs


# ===========================================================================
# index_attributes() — the foundation
# ===========================================================================


class TestIndexAttributesNullDrop:
    def test_null_alias_omits_index_key(self):
        Model, _ = make_model()
        item = Model(entity_id="eid-1", sort_key="sk-1", user_id=None, event_at=None)
        attrs = item.index_attributes()
        assert "PK" in attrs and "SK" in attrs
        # gsi0_pk and gsi0_sk are sourced from None aliases — must be omitted
        assert "gsi0_pk" not in attrs
        assert "gsi0_sk" not in attrs

    def test_set_alias_present_keeps_index_key(self):
        Model, _ = make_model()
        item = Model(
            entity_id="eid-1", sort_key="sk-1", user_id="u-1", event_at="2026-01-01"
        )
        attrs = item.index_attributes()
        assert attrs["gsi0_pk"] == "u-1"
        assert attrs["gsi0_sk"] == "2026-01-01"

    def test_disabled_flag_keeps_null_index_key(self):
        Model, _ = make_model(drop_null_index_keys=False)
        item = Model(entity_id="eid-1", sort_key="sk-1", user_id=None, event_at=None)
        attrs = item.index_attributes()
        # With the safety net off, prior behaviour stands — None propagates
        assert attrs["gsi0_pk"] is None
        assert attrs["gsi0_sk"] is None


# ===========================================================================
# PutItem — Item dict elides null index keys, alias survives
# ===========================================================================


class TestPutItemNullDrop:
    def test_alias_null_drops_index_key_keeps_alias(self):
        Model, client = make_model(store_aliases=True)
        item = Model(entity_id="eid-1", sort_key="sk-1", user_id=None, name="Alice")
        run(item.save())
        kw = put_kwargs(client)
        body = kw["Item"]
        assert "gsi0_pk" not in body
        # The alias attribute itself remains — user's explicit null is preserved
        assert body["user_id"] == {"NULL": True}
        assert body["name"] == {"S": "Alice"}

    def test_alias_null_with_store_aliases_false_drops_both(self):
        """store_aliases=False already strips aliases regardless; combined with
        drop_null_index_keys=True the row carries neither the alias nor key."""
        Model, client = make_model(store_aliases=False)
        item = Model(entity_id="eid-1", sort_key="sk-1", user_id=None)
        run(item.save())
        body = put_kwargs(client)["Item"]
        assert "gsi0_pk" not in body
        assert "user_id" not in body

    def test_disabled_flag_writes_null_to_index_key(self):
        Model, client = make_model(drop_null_index_keys=False)
        item = Model(entity_id="eid-1", sort_key="sk-1", user_id=None)
        run(item.save())
        body = put_kwargs(client)["Item"]
        # Without the safety net, the NULL goes through verbatim
        assert body["gsi0_pk"] == {"NULL": True}


# ===========================================================================
# update_by_key — atomic `set` rewrites null index targets to REMOVE
# ===========================================================================


class TestUpdateByKeySetNull:
    def test_set_alias_null_removes_index_key_and_nulls_alias(self):
        Model, client = make_model(store_aliases=True)
        run(Model.update_by_key("eid-1", "sk-1", set={"user_id": None}))
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        # index key target → REMOVE
        assert "REMOVE" in expr and "#gsi0_pk" in expr
        # alias target → SET to NULL (user's explicit intent retained)
        assert "#user_id = :_set_user_id" in expr
        assert kw["ExpressionAttributeValues"][":_set_user_id"] == {"NULL": True}
        # No bogus value placeholder for the index key
        assert ":_set_gsi0_pk" not in kw.get("ExpressionAttributeValues", {})

    def test_set_alias_null_store_aliases_false_only_removes_index_key(self):
        Model, client = make_model(store_aliases=False)
        run(Model.update_by_key("eid-1", "sk-1", set={"user_id": None}))
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        assert "REMOVE #gsi0_pk" in expr
        # The alias is not a stored field at all
        assert "#user_id" not in kw["ExpressionAttributeNames"]
        assert "SET" not in expr

    def test_set_index_key_directly_null_removes(self):
        """Passing the underlying key name itself is unusual but should
        still be honoured: NULL → REMOVE."""
        Model, client = make_model()
        run(Model.update_by_key("eid-1", "sk-1", set={"gsi0_pk": None}))
        kw = update_kwargs(client)
        assert "REMOVE #gsi0_pk" in kw["UpdateExpression"]

    def test_set_non_index_field_null_still_sets_null(self):
        """Plain attributes are not index keys — preserve the user's intent."""
        Model, client = make_model()
        run(Model.update_by_key("eid-1", "sk-1", set={"name": None}))
        kw = update_kwargs(client)
        assert kw["UpdateExpression"].strip() == "SET #name = :_set_name"
        assert kw["ExpressionAttributeValues"][":_set_name"] == {"NULL": True}

    def test_set_non_null_alias_unchanged(self):
        """Null-drop logic must not perturb the happy path."""
        Model, client = make_model()
        run(Model.update_by_key("eid-1", "sk-1", set={"user_id": "u-42"}))
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        assert "#gsi0_pk = :_set_gsi0_pk" in expr
        assert "#user_id = :_set_user_id" in expr
        assert "REMOVE" not in expr

    def test_disabled_flag_passes_null_through(self):
        Model, client = make_model(drop_null_index_keys=False)
        run(Model.update_by_key("eid-1", "sk-1", set={"user_id": None}))
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        # No REMOVE — original SET-to-NULL behaviour preserved
        assert "REMOVE" not in expr
        assert "#gsi0_pk = :_set_gsi0_pk" in expr
        assert kw["ExpressionAttributeValues"][":_set_gsi0_pk"] == {"NULL": True}


# ===========================================================================
# update_by_key — set_if_not_exists with null on an index target
# ===========================================================================


class TestUpdateByKeySetIfNotExistsNull:
    def test_null_index_target_emits_no_clause(self):
        """if_not_exists(idx_key, NULL) is incoherent on a sparse index — drop it."""
        Model, client = make_model(store_aliases=False)
        # Pair with a non-null op so the call still has at least one clause
        run(
            Model.update_by_key(
                "eid-1",
                "sk-1",
                set_if_not_exists={"user_id": None},
                set={"name": "Default"},
            )
        )
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        assert "if_not_exists(#gsi0_pk" not in expr
        # The accompanying set still functions
        assert "#name = :_set_name" in expr

    def test_null_alias_target_still_emitted_when_store_aliases(self):
        """The alias side is not an index key, so the (silly) clause stands."""
        Model, client = make_model(store_aliases=True)
        run(Model.update_by_key("eid-1", "sk-1", set_if_not_exists={"user_id": None}))
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        assert "if_not_exists(#gsi0_pk" not in expr
        assert "#user_id = if_not_exists(#user_id, :_ine_user_id)" in expr


# ===========================================================================
# Instance update() via diff — alias→null routes to REMOVE on the index key
# ===========================================================================


class TestInstanceUpdateDiffNullDrop:
    def _instance(self, **overrides):
        defaults = dict(store_aliases=True, drop_null_index_keys=True)
        defaults.update(overrides)
        Model, client = make_model(**defaults)
        item = Model(
            entity_id="eid-1",
            sort_key="sk-1",
            user_id="u-1",
            event_at="2026-01-01",
            name="Alice",
        )
        return item, client

    def test_alias_set_to_none_emits_remove_for_index_key(self):
        item, client = self._instance()
        item.user_id = None
        run(item.update())
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        # gsi0_pk vanished from _db_item → diff.removed → REMOVE
        assert "REMOVE" in expr and "#gsi0_pk" in expr
        # user_id (alias) is still in _db_item with None → diff.changed → SET NULL
        assert "#user_id = :user_id" in expr
        assert kw["ExpressionAttributeValues"][":user_id"] == {"NULL": True}

    def test_alias_set_to_none_store_aliases_false(self):
        item, client = self._instance(store_aliases=False)
        item.user_id = None
        run(item.update())
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        # Only the index key — alias is stripped before serialisation
        assert "REMOVE #gsi0_pk" in expr
        assert "#user_id" not in kw["ExpressionAttributeNames"]

    def test_disabled_flag_writes_null_via_set(self):
        item, client = self._instance(drop_null_index_keys=False)
        item.user_id = None
        run(item.update())
        kw = update_kwargs(client)
        expr = kw["UpdateExpression"]
        assert "REMOVE" not in expr
        assert "#gsi0_pk = :gsi0_pk" in expr
        assert kw["ExpressionAttributeValues"][":gsi0_pk"] == {"NULL": True}
