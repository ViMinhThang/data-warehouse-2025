import logging
import smtplib
from email.mime.text import MIMEText
from typing import List, Optional


class EmailService:
    def __init__(
        self,
        smtp_host: str = "smtp.gmail.com",
        smtp_port: int = 587,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_tls: bool = True,
        simulate: bool = True,
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.simulate = simulate
        logging.info(f"EmailService initialized (simulate={self.simulate})")

    def send_email(
        self,
        to_addrs: List[str],
        subject: str,
        body: str,
        from_addr: str = "noreply@extractor.local",
    ):
        """Gửi email thật hoặc giả lập tuỳ chế độ simulate"""
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)

        if self.simulate:
            logging.warning(f"[SIMULATE] Gửi email tới {to_addrs}: {subject}\n{body}")
            return

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=10) as smtp:
                if self.use_tls:
                    smtp.starttls()
                if self.username and self.password:
                    smtp.login(self.username, self.password)
                smtp.sendmail(from_addr, to_addrs, msg.as_string())
                logging.info(f"Đã gửi email tới {to_addrs}: {subject}")
        except Exception as e:
            logging.error(f"Lỗi khi gửi email tới {to_addrs}: {e}")
            raise
