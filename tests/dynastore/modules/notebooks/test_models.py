from datetime import datetime
from dynastore.modules.notebooks.models import (
    Notebook,
    PlatformNotebookCreate,
    PlatformNotebook,
    OwnerType,
)


def test_owner_type_enum():
    assert OwnerType.MODULE == "module"
    assert OwnerType.SYSADMIN == "sysadmin"


def test_platform_notebook_create():
    nb = PlatformNotebookCreate(
        notebook_id="test-nb",
        title={"en": "Test"},
        registered_by="test_module",
        owner_type=OwnerType.MODULE,
        content={"cells": [], "metadata": {}},
    )
    assert nb.notebook_id == "test-nb"
    assert nb.owner_type == OwnerType.MODULE
    assert nb.registered_by == "test_module"


def test_platform_notebook_includes_timestamps():
    now = datetime.now()
    nb = PlatformNotebook(
        notebook_id="test-nb",
        title={"en": "Test"},
        registered_by="test_module",
        owner_type=OwnerType.MODULE,
        content={"cells": []},
        created_at=now,
        updated_at=now,
    )
    assert nb.deleted_at is None
    assert nb.created_at == now


def test_tenant_notebook_has_new_fields():
    now = datetime.now()
    nb = Notebook(
        notebook_id="test-nb",
        title={"en": "Test"},
        content={"cells": []},
        catalog_id="cat-1",
        created_at=now,
        updated_at=now,
        owner_id="user-123",
        copied_from="platform-nb-1",
    )
    assert nb.deleted_at is None
    assert nb.owner_id == "user-123"
    assert nb.copied_from == "platform-nb-1"


def test_tenant_notebook_defaults():
    now = datetime.now()
    nb = Notebook(
        notebook_id="test-nb",
        title={"en": "Test"},
        content={"cells": []},
        catalog_id="cat-1",
        created_at=now,
        updated_at=now,
    )
    assert nb.owner_id is None
    assert nb.copied_from is None
    assert nb.deleted_at is None


def test_platform_notebook_create_carries_default_catalog_and_applies_to():
    from dynastore.modules.notebooks.models import (
        OwnerType,
        PlatformNotebookCreate,
    )
    from dynastore.models.localization import LocalizedText

    nb = PlatformNotebookCreate(
        notebook_id="x",
        title=LocalizedText(en="X"),
        content={"cells": []},
        registered_by="t",
        owner_type=OwnerType.MODULE,
        default_catalog_id="demo-catalog",
        applies_to=["demo-catalog", "spanner-catalog"],
    )
    assert nb.default_catalog_id == "demo-catalog"
    assert nb.applies_to == ["demo-catalog", "spanner-catalog"]


def test_platform_notebook_create_defaults_for_targeting_fields_are_none():
    from dynastore.modules.notebooks.models import (
        OwnerType,
        PlatformNotebookCreate,
    )
    from dynastore.models.localization import LocalizedText

    nb = PlatformNotebookCreate(
        notebook_id="x",
        title=LocalizedText(en="X"),
        content={"cells": []},
        registered_by="t",
        owner_type=OwnerType.MODULE,
    )
    assert nb.default_catalog_id is None
    assert nb.applies_to is None
