import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from calculator import add, subtract, multiply, divide
import pytest


def test_addition():
    assert add(1, 1) == 2
    assert add(5, 3) == 8
    assert add(-1, 1) == 0


def test_subtraction():
    assert subtract(5, 3) == 2
    assert subtract(10, 5) == 5


def test_multiplication():
    assert multiply(3, 4) == 12
    assert multiply(0, 5) == 0


def test_division():
    assert divide(10, 2) == 5
    assert divide(9, 3) == 3


def test_division_by_zero():
    with pytest.raises(ValueError, match="Cannot divide by zero"):
        divide(10, 0)


def test_app_main(capsys):
    """Test app.py main function."""
    import app
    app.main()
    captured = capsys.readouterr()
    assert "Calculator App" in captured.out
    assert "10 + 5 = 15" in captured.out
    assert "10 - 5 = 5" in captured.out
    assert "10 * 5 = 50" in captured.out
    assert "10 / 5 = 2.0" in captured.out
    assert "Calculator is working!" in captured.out