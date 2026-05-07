from dynastore.models.localization import LocalizedText
from dynastore.models.protocols.notebook_contributor import NotebookContributorProtocol
from dynastore.modules.notebooks.contribution import NotebookContribution


class _Contributor:
    def get_notebooks(self) -> list[NotebookContribution]:
        return [
            NotebookContribution(
                notebook_id="t",
                title=LocalizedText(en="T"),
                registered_by="t",
                notebook_content={},
            )
        ]


class _NonContributor:
    pass


def test_runtime_isinstance_accepts_contributor():
    assert isinstance(_Contributor(), NotebookContributorProtocol)


def test_runtime_isinstance_rejects_non_contributor():
    assert not isinstance(_NonContributor(), NotebookContributorProtocol)
