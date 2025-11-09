from typing import Optional


def log_message(
    log_db,
    stage: str,
    config_id: Optional[int],
    status: str,
    message: Optional[str] = None,
    error_message: Optional[str] = None,
    print_console: bool = True,
):

    if log_db:
        try:
            log_db.insert_log(
                stage=stage,
                config_id=config_id,
                status=status,
                log_level="INFO" if status not in ["FAILURE", "ERROR"] else "ERROR",
                message=message,
                error_message=error_message,
            )
        except Exception as e:
            print(f"Lỗi khi ghi log vào DB: {e}")
