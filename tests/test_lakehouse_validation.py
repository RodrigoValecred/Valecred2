import pytest

def validate_lakehouse(name):
    """
    Valida se o nome do lakehouse está na lista de permitidos para evitar SQL Injection.
    """
    allowed_lakehouses = {"LH_Bronze", "LH_Silver", "LH_Gold"}
    if name not in allowed_lakehouses:
        raise ValueError(f"Security Alert: Nome de lakehouse não autorizado: {name}")
    return name

def test_validate_lakehouse_valid():
    assert validate_lakehouse("LH_Bronze") == "LH_Bronze"
    assert validate_lakehouse("LH_Silver") == "LH_Silver"
    assert validate_lakehouse("LH_Gold") == "LH_Gold"

def test_validate_lakehouse_invalid():
    with pytest.raises(ValueError, match="Security Alert: Nome de lakehouse não autorizado:"):
        validate_lakehouse("LH_Insecure")

def test_validate_lakehouse_injection_attempt():
    with pytest.raises(ValueError, match="Security Alert: Nome de lakehouse não autorizado:"):
        validate_lakehouse("LH_Bronze; DROP TABLE LH_Silver.staging_clientes_limpa")
