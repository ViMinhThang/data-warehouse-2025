from typing import Optional


def log_message(
    log_db,
    stage: str,
    config_id: Optional[int],
    status: str,
    ticker: Optional[str] = None,
    message: Optional[str] = None,
    error_message: Optional[str] = None,
    print_console: bool = True,
):

    try:
        log_db.insert_log(
            stage=stage,
            config_id=config_id,
            ticker=ticker,
            status=status,
            log_level="INFO" if status not in ["FAILURE", "ERROR"] else "ERROR",
            message=message,
            error_message=error_message,
        )

        if print_console:
            prefix = (
                "[SUCCESS]"
                if status == "SUCCESS"
                else "[FAILURE]" if status == "FAILURE" else "[INFO]"
            )
            print(
                f"{prefix} [{stage}] config_id={config_id} "
                f"{ticker or ''} → {message or error_message}"
            )

    except Exception as e:
        print(f"Lỗi khi ghi log vào DB: {e}")
