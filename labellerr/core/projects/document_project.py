from typing import TYPE_CHECKING

from ..schemas import DatasetDataType
from .base import LabellerrProject, LabellerrProjectMeta

if TYPE_CHECKING:
    from ..client import LabellerrClient  # noqa:F401


class DocucmentProject(LabellerrProject):

    def fetch_datasets(self):
        print("Yo I am gonna fetch some datasets!")


LabellerrProjectMeta._register(DatasetDataType.document, DocucmentProject)
