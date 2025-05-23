import os

from .get_local_output_dir_for_development import get_local_development_output_dir
from hhnk_threedi_tools.git_model_repo.utils.dump_xlsx import ExcelDump
from hhnk_threedi_tools.git_model_repo.utils.restore_xlsx import ExcelRestore

dir = get_local_development_output_dir(clean=True)


class TestDumpAndRestoreExcel:
    test_excel = os.path.join(os.path.dirname(__file__), "data", "test_for_dump.xlsx")

    text_dumped_dir = os.path.join(os.path.dirname(__file__), "data", "test_excel")

    def test_dump_excel(self, tmp_path):
        tmp_path = dir

        dumper = ExcelDump(self.test_excel, tmp_path)
        dumper.dump_schema()
        assert os.path.exists(os.path.join(tmp_path, "schema.json"))

        dumper.dump_sheets()

        assert os.path.exists(os.path.join(tmp_path, "Sheet1.json"))

    def test_restore_excel(self, tmp_path):
        tmp_path = dir

        os.makedirs(tmp_path, exist_ok=True)
        tmp_file_path = os.path.join(tmp_path, "excel_restored.xlsx")

        restorer = ExcelRestore(self.text_dumped_dir, tmp_file_path)
        restorer.restore()
        assert os.path.exists(os.path.join(tmp_path, "excel_restored.xlsx"))

        # check if the restored file is the same as the original
