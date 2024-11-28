import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../sendqsarpy')))
import pytest
from get_mi_score import get_mi_score # Adjust this import based on your actual module location

@pytest.fixture
def setup_paths():
    """Fixture to set up database paths"""
    return {
        "fake_sqlite_db_path": "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/fake_merged_liver_not_liver.db",
        "fake_xpt_db_path": "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/single_fake_xpt_folder/FAKE28738",
        "real_sqlite_db_path": "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/TestDB.db",
        "real_xpt_db_path": "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/real_xpt_dir/IND051292_1017-3581"
    }

def test_fake_sqlite(setup_paths):
    """Test the function for fake SQLite database"""
    fake_T_xpt_F_mi_score = get_mi_score(
        studyid="28738",
        path_db=setup_paths["fake_sqlite_db_path"],
        fake_study=True,
        use_xpt_file=False,
        master_compiledata=None,
        return_individual_scores=False,
        return_zscore_by_USUBJID=False
    )
    # Check if the output is a dictionary and contains the expected keys
    assert isinstance(fake_T_xpt_F_mi_score, dict)
    assert "MI_Score" in fake_T_xpt_F_mi_score

def test_fake_xpt(setup_paths):
    """Test the function for fake XPT data"""
    fake_T_xpt_T_mi_score = get_mi_score(
        studyid=None,
        path_db=setup_paths["fake_xpt_db_path"],
        fake_study=True,
        use_xpt_file=True,
        master_compiledata=None,
        return_individual_scores=False,
        return_zscore_by_USUBJID=False
    )
    # Check if the output is a dictionary and contains the expected keys
    assert isinstance(fake_T_xpt_T_mi_score, dict)
    assert "MI_Score" in fake_T_xpt_T_mi_score

def test_real_sqlite(setup_paths):
    """Test the function for real SQLite database"""
    real_sqlite_mi_score = get_mi_score(
        studyid="5003635",
        path_db=setup_paths["real_sqlite_db_path"],
        fake_study=False,
        use_xpt_file=False,
        master_compiledata=None,
        return_individual_scores=False,
        return_zscore_by_USUBJID=False
    )
    # Check if the output is a dictionary and contains the expected keys
    assert isinstance(real_sqlite_mi_score, dict)
    assert "MI_Score" in real_sqlite_mi_score

def test_real_xpt(setup_paths):
    """Test the function for real XPT data"""
    real_XPT_mi_score = get_mi_score(
        studyid=None,
        path_db=setup_paths["real_xpt_db_path"],
        fake_study=False,
        use_xpt_file=True,
        master_compiledata=None,
        return_individual_scores=False,
        return_zscore_by_USUBJID=False
    )
    # Check if the output is a dictionary and contains the expected keys
    assert isinstance(real_XPT_mi_score, dict)
    assert "MI_Score" in real_XPT_mi_score

def test_invalid_paths():
    """Test the function with invalid paths"""
    invalid_db_path = "C:/invalid/path/to/database.db"
    with pytest.raises(FileNotFoundError):
        get_mi_score(
            studyid="28738",
            path_db=invalid_db_path,
            fake_study=True,
            use_xpt_file=False,
            master_compiledata=None,
            return_individual_scores=False,
            return_zscore_by_USUBJID=False
        )

def test_invalid_studyid(setup_paths):
    """Test the function with an invalid study ID"""
    db_path = setup_paths["real_sqlite_db_path"]
    invalid_studyid = "9999999"  # Assuming this study ID doesn't exist
    result = get_mi_score(
        studyid=invalid_studyid,
        path_db=db_path,
        fake_study=False,
        use_xpt_file=False,
        master_compiledata=None,
        return_individual_scores=False,
        return_zscore_by_USUBJID=False
    )
    # Check if the result is empty or handled properly (you can adjust based on expected behavior)
    assert result is None
