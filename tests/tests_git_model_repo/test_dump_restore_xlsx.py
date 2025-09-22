from pathlib import Path

from hhnk_threedi_tools.git_model_repo.utils.dump_xlsx import ExcelDump
from hhnk_threedi_tools.git_model_repo.utils.restore_xlsx import ExcelRestore

from .helpers import get_local_development_output_dir

directory = get_local_development_output_dir(clean=True)


class TestDumpAndRestoreExcel:
    test_excel = Path(__file__).parent / "data" / "test_for_dump.xlsx"
    text_dumped_dir = Path(__file__).parent / "data" / "test_excel"

    def test_dump_excel(self, tmp_path):
        tmp_path = directory

        dumper = ExcelDump(self.test_excel, tmp_path)
        dumper.dump_schema()
        assert (tmp_path / "schema.json").exists()

        dumper.dump_sheets()
        assert (tmp_path / "Sheet1.json").exists()

    def test_restore_excel(self, tmp_path):
        tmp_path = directory

        tmp_path.mkdir(parents=True, exist_ok=True)
        tmp_file_path = tmp_path / "excel_restored.xlsx"

        restorer = ExcelRestore(self.text_dumped_dir, tmp_file_path)
        restorer.restore()
        assert tmp_file_path.exists()
