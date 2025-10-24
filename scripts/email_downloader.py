# scripts/fetch_csv_attachments_fixed.py
import imaplib
import email
from email.header import decode_header
import os
from typing import Optional
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def _decode_filename(raw_filename):
    """Decodifica o nome do arquivo de anexos."""
    if not raw_filename:
        return None
    parts = decode_header(raw_filename)
    decoded_fragments = []
    for fragment, encoding in parts:
        if isinstance(fragment, bytes):
            decoded_fragments.append(fragment.decode(encoding or "utf-8", errors="ignore"))
        else:
            decoded_fragments.append(fragment)
    return "".join(decoded_fragments)

logger = logging.getLogger(__name__)

def fetch_csv_attachments(email_config, sender_filter: Optional[str] = None, subject_filter: Optional[str] = None):
    
    email_user = email_config["email_user"]
    email_pass = email_config["email_pass"]
    mailbox = email_config["mailbox"]
    imap_server = email_config["imap_server"]
    output_dir = email_config["output_dir"]
    
    # Validating credentials
    if not email_user or not email_pass:
        logger.error("email_user and email_pass should be defined in the file .env")
        sys.exit(1)
    
    os.makedirs(output_dir, exist_ok=True)

    # IMAP
    try:
        mail = imaplib.IMAP4_SSL(imap_server)
        logger.info("Connected %s", imap_server)
    except Exception as e:
        logger.error("Fail while trying to connect to IMAP server: %s", e)
        return

    # Login
    try:
        mail.login(email_user, email_pass)
        logger.info("Logged in: %s", email_user)
    except imaplib.IMAP4.error as e:
        logger.error("Login fail: %s", e)
        return

    status, response = mail.select(mailbox)
    if status != "OK":
        logger.error("Was nos possible to select the mailbox %s: %s", mailbox, response)
        mail.logout()
        return

    criteria_parts = ['UNSEEN']
    if sender_filter:
        criteria_parts.append(f'FROM "{sender_filter}"')
    if subject_filter:
        criteria_parts.append(f'SUBJECT "{subject_filter}"')

    search_criteria = " ".join(criteria_parts)
    logger.info("Searching emails...: %s", search_criteria)

    status, data = mail.search(None, search_criteria)
    if status != "OK":
        logger.error("Search failled: %s", status)
        mail.close()
        mail.logout()
        return

    if not data or not data[0]:
        logger.info("✅ No emails found")
        mail.close()
        mail.logout()
        return

    message_ids = data[0].split()
    logger.info("Found %d emails unseen", len(message_ids))

    downloaded_count = 0
    
    for msg_id in message_ids:
        try:
            status, msg_data = mail.fetch(msg_id, "(RFC822)")
            
            if status != "OK":
                logger.warning("Status NOK while searching message ID %s: %s", msg_id, status)
                continue
                
            if not msg_data:
                logger.warning("msg_data is None to message ID %s", msg_id)
                continue
                
            if not isinstance(msg_data, (list, tuple)) or len(msg_data) == 0:
                logger.warning("msg_data has unexpected structure for ID %s: %s", msg_id, type(msg_data))
                continue
                
            if not msg_data[0]:
                logger.warning("msg_data[0] is None for message ID %s", msg_id)
                continue
                
            if not isinstance(msg_data[0], (list, tuple)) or len(msg_data[0]) < 2:
                logger.warning("msg_data[0] has unexpected structure for ID %s: %s", msg_id, msg_data[0])
                continue

            raw_email = msg_data[0][1]
            
            if not isinstance(raw_email, bytes):
                logger.warning("raw_email is not bytes to ID %s, é %s", msg_id, type(raw_email))
                # Tenta converter se for string
                if isinstance(raw_email, str):
                    raw_email = raw_email.encode('utf-8')
                else:
                    logger.error("Was not possible to process raw_email to ID %s", msg_id)
                    continue
   
            msg = email.message_from_bytes(raw_email)

            for part in msg.walk():
                content_disposition = part.get_content_disposition()
                filename = part.get_filename()
                
                if content_disposition == "attachment" or filename:
                    decoded_name = _decode_filename(filename) if filename else None
                    
                    if decoded_name and decoded_name.lower().endswith(".csv"):

                        safe_name = decoded_name.replace("/", "_").replace("\\", "_").replace("..", "_")
                        filepath = os.path.join(output_dir, safe_name)

                        payload = part.get_payload(decode=True)
                        
                        if payload is None:
                            logger.warning("Payload is None to attachment %s in the message %s", safe_name, msg_id)
                            continue
                            
                        if not isinstance(payload, bytes):
                            logger.warning("Payload não é bytes para %s, é %s", safe_name, type(payload))
                            # Tenta converter se for string
                            if isinstance(payload, str):
                                payload = payload.encode('utf-8')
                            else:
                                logger.error("Não foi possível processar payload para %s", safe_name)
                                continue
                        
                        if len(payload) == 0:
                            logger.warning("Payload vazio para anexo %s", safe_name)
                            continue

                        # Salva o arquivo
                        try:
                            with open(filepath, "wb") as f:
                                f.write(payload)
                            
                            logger.info("✅ Downloaded: %s (%.2f KB)", safe_name, len(payload) / 1024)
                            downloaded_count += 1
                        except Exception as write_error:
                            logger.error("Error saving file %s: %s", safe_name, write_error)
                            continue

            # Marca como lido APENAS após processar com sucesso
            mail.store(msg_id, '+FLAGS', '\\Seen')
            
        except Exception as e:
            logger.exception("Error processing message %s: %s", msg_id, e)
            continue

    logger.info("✅ Process concluded: %d CSV attachments downloaded", downloaded_count)
    
    mail.close()
    mail.logout()
