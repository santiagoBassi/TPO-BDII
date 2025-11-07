from datetime import datetime

# --- Funciones de Ayuda (Helpers) ---

def parse_date(date_str, default=None):
    """Convierte un string dd/mm/YYYY a un objeto datetime."""
    if not date_str:
        return default
    try:
        return datetime.strptime(date_str, '%d/%m/%Y')
    except ValueError:
        print(f"Advertencia: Formato de fecha inválido '{date_str}'.")
        return default

def parse_bool(bool_str):
    """Convierte un string 'True'/'False' a un booleano."""
    return bool_str.lower() == 'true'

def parse_int(val_str, default=0):
    """Convierte un string a int, con un valor por defecto."""
    if not val_str:
        return default
    try:
        return int(val_str)
    except ValueError:
        return default

def parse_float(val_str, default=0.0):
    """Convierte un string a float, con un valor por defecto."""
    if not val_str:
        return default
    try:
        return float(val_str)
    except ValueError:
        return default
