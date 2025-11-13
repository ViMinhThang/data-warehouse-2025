
import os
import importlib
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def init_services(services_to_init):
    """
    Initializes and returns a dictionary of services.
    
    Args:
        services_to_init (list): A list of strings representing the services to initialize.
                                 e.g., ['config_extract_db', 'log_db', 'email_service']
    
    Returns:
        dict: A dictionary where keys are service names and values are service instances.
    """
    
    db_params = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME_CONFIG"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }
    
    db_params_staging = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME_STAGING", "staging"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    db_params_dw = {
        "host": os.getenv("DB_HOST"),
        "dbname": "dw",
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    
    initialized_services = {}
    
    if 'config_extract_db' in services_to_init:
        module = importlib.import_module('db.config_extract_db')
        class_ = getattr(module, 'ConfigExtractDatabase')
        initialized_services['config_extract_db'] = class_(**db_params)
        
    if 'config_load_db' in services_to_init:
        module = importlib.import_module('db.config_load_db')
        class_ = getattr(module, 'ConfigLoadDatabase')
        initialized_services['config_load_db'] = class_(**db_params)

    if 'config_load_staging_db' in services_to_init:
        module = importlib.import_module('db.config_load_staging_db')
        class_ = getattr(module, 'ConfigLoadStagingDatabase')
        initialized_services['config_load_staging_db'] = class_(**db_params)
        
    if 'config_transform_db' in services_to_init:
        module = importlib.import_module('db.config_transform_db')
        class_ = getattr(module, 'ConfigTransformDatabase')
        initialized_services['config_transform_db'] = class_(**db_params)

    if 'config_transform_staging_db' in services_to_init:
        module = importlib.import_module('db.config_transform_staging_db')
        class_ = getattr(module, 'ConfigTransformStagingDatabase')
        initialized_services['config_transform_staging_db'] = class_(**db_params)
        
    if 'log_db' in services_to_init:
        module = importlib.import_module('db.log_db')
        class_ = getattr(module, 'LogDatabase')
        initialized_services['log_db'] = class_(**db_params)
        
    if 'staging_db' in services_to_init:
        module = importlib.import_module('db.staging_db')
        class_ = getattr(module, 'StagingDatabase')
        initialized_services['staging_db'] = class_(**db_params_staging)
        
    if 'email_service' in services_to_init:
        module = importlib.import_module('email_service.email_service')
        class_ = getattr(module, 'EmailService')
        initialized_services['email_service'] = class_(
            username=os.getenv("EMAIL_USERNAME"),
            password=os.getenv("EMAIL_PASSWORD"),
            simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
        )
        
    if 'staging_engine' in services_to_init:
        engine = create_engine(
            f"postgresql://{db_params_staging['user']}:{db_params_staging['password']}@{db_params_staging['host']}:{db_params_staging['port']}/{db_params_staging['dbname']}"
        )
        initialized_services['staging_engine'] = engine

    if 'dw_engine' in services_to_init:
        engine = create_engine(
            f"postgresql://{db_params_dw['user']}:{db_params_dw['password']}@{db_params_dw['host']}:{db_params_dw['port']}/{db_params_dw['dbname']}"
        )
        initialized_services['dw_engine'] = engine
        
    print("Đã khởi tạo thành công các service.")
    return initialized_services
