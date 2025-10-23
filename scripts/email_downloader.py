# scripts/fetch_csv_attachments_fixed.py
import imaplib
import email
from email.header import decode_header
import os
from dotenv import load_dotenv
from typing import Optional
import logging
import sys

load_dotenv()

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
MAILBOX = "INBOX"
OUTPUT_DIR = "data/incoming"

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


def fetch_csv_attachments(sender_filter: Optional[str] = None, subject_filter: Optional[str] = None):
    """
    Busca e baixa anexos CSV de emails não lidos.
    
    Args:
        sender_filter: Filtrar por remetente (ex: "relatorio@empresa.com")
        subject_filter: Filtrar por assunto
    """
    # Validação de credenciais
    if not EMAIL_USER or not EMAIL_PASS:
        logging.error("EMAIL_USER e EMAIL_PASS devem estar definidos no arquivo .env")
        sys.exit(1)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Conexão com servidor IMAP
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        logging.info("Conectado ao servidor %s", IMAP_SERVER)
    except Exception as e:
        logging.error("Falha ao conectar ao servidor IMAP: %s", e)
        return

    # Login
    try:
        mail.login(EMAIL_USER, EMAIL_PASS)
        logging.info("Login bem-sucedido para %s", EMAIL_USER)
    except imaplib.IMAP4.error as e:
        logging.error("❌ Falha no login: %s", e)
        logging.error("\n🔧 SOLUÇÕES:")
        logging.error("1. Verifique se IMAP está habilitado em gmail.com → Configurações → Encaminhamento e POP/IMAP")
        logging.error("2. Se a conta é nova (< 1h), aguarde ~30 minutos")
        logging.error("3. Use senha de app: myaccount.google.com/apppasswords (requer 2FA)")
        logging.error("4. Verifique se EMAIL_USER e EMAIL_PASS estão corretos no .env")
        return

    # Seleciona mailbox
    status, response = mail.select(MAILBOX)
    if status != "OK":
        logging.error("Não foi possível selecionar mailbox %s: %s", MAILBOX, response)
        mail.logout()
        return

    # Monta critério de busca
    criteria_parts = ['UNSEEN']
    if sender_filter:
        criteria_parts.append(f'FROM "{sender_filter}"')
    if subject_filter:
        criteria_parts.append(f'SUBJECT "{subject_filter}"')

    search_criteria = " ".join(criteria_parts)
    logging.info("Buscando emails com critério: %s", search_criteria)

    # Busca por emails
    status, data = mail.search(None, search_criteria)
    if status != "OK":
        logging.error("Busca falhou: %s", status)
        mail.close()
        mail.logout()
        return

    # Verifica se data é None ou vazio
    if not data or not data[0]:
        logging.info("✅ Nenhum email novo encontrado com os critérios especificados")
        mail.close()
        mail.logout()
        return

    # Decodifica IDs de mensagens (vêm como bytes)
    message_ids = data[0].split()
    logging.info("Encontrados %d emails não lidos", len(message_ids))

    downloaded_count = 0
    
    for msg_id in message_ids:
        try:
            # Busca a mensagem (msg_id já é bytes, não precisa codificar)
            status, msg_data = mail.fetch(msg_id, "(RFC822)")
            
            # VALIDAÇÃO ROBUSTA: verifica estrutura completa
            if status != "OK":
                logging.warning("Status NOK ao buscar mensagem ID %s: %s", msg_id, status)
                continue
                
            if not msg_data:
                logging.warning("msg_data é None para mensagem ID %s", msg_id)
                continue
                
            if not isinstance(msg_data, (list, tuple)) or len(msg_data) == 0:
                logging.warning("msg_data tem estrutura inesperada para ID %s: %s", msg_id, type(msg_data))
                continue
                
            if not msg_data[0]:
                logging.warning("msg_data[0] é None para mensagem ID %s", msg_id)
                continue
                
            if not isinstance(msg_data[0], (list, tuple)) or len(msg_data[0]) < 2:
                logging.warning("msg_data[0] tem estrutura inesperada para ID %s: %s", msg_id, msg_data[0])
                continue

            # Extrai o conteúdo bruto do email
            raw_email = msg_data[0][1]
            
            # VALIDAÇÃO EXPLÍCITA: garante que raw_email é bytes
            if not isinstance(raw_email, bytes):
                logging.warning("raw_email não é bytes para ID %s, é %s", msg_id, type(raw_email))
                # Tenta converter se for string
                if isinstance(raw_email, str):
                    raw_email = raw_email.encode('utf-8')
                else:
                    logging.error("Não foi possível processar raw_email para ID %s", msg_id)
                    continue
            
            # Parse do email
            msg = email.message_from_bytes(raw_email)

            # Processa cada parte do email
            for part in msg.walk():
                content_disposition = part.get_content_disposition()
                filename = part.get_filename()
                
                # Verifica se é anexo
                if content_disposition == "attachment" or filename:
                    decoded_name = _decode_filename(filename) if filename else None
                    
                    # Verifica se é arquivo CSV
                    if decoded_name and decoded_name.lower().endswith(".csv"):
                        # Remove caracteres problemáticos do nome do arquivo
                        safe_name = decoded_name.replace("/", "_").replace("\\", "_").replace("..", "_")
                        filepath = os.path.join(OUTPUT_DIR, safe_name)

                        # Extrai o conteúdo do anexo
                        payload = part.get_payload(decode=True)
                        
                        # VALIDAÇÃO EXPLÍCITA: garante que payload é bytes
                        if payload is None:
                            logging.warning("Payload é None para anexo %s na mensagem %s", safe_name, msg_id)
                            continue
                            
                        if not isinstance(payload, bytes):
                            logging.warning("Payload não é bytes para %s, é %s", safe_name, type(payload))
                            # Tenta converter se for string
                            if isinstance(payload, str):
                                payload = payload.encode('utf-8')
                            else:
                                logging.error("Não foi possível processar payload para %s", safe_name)
                                continue
                        
                        if len(payload) == 0:
                            logging.warning("Payload vazio para anexo %s", safe_name)
                            continue

                        # Salva o arquivo
                        try:
                            with open(filepath, "wb") as f:
                                f.write(payload)
                            
                            logging.info("✅ Baixado: %s (%.2f KB)", safe_name, len(payload) / 1024)
                            downloaded_count += 1
                        except Exception as write_error:
                            logging.error("Erro ao salvar arquivo %s: %s", safe_name, write_error)
                            continue

            # Marca como lido APENAS após processar com sucesso
            mail.store(msg_id, '+FLAGS', '\\Seen')
            
        except Exception as e:
            logging.exception("Erro ao processar mensagem %s: %s", msg_id, e)
            continue

    logging.info("✅ Processo concluído: %d anexos CSV baixados", downloaded_count)
    
    mail.close()
    mail.logout()

fetch_csv_attachments()

# if __name__ == "__main__":
#     # Exemplo de uso: busca emails de um remetente específico
#     fetch_csv_attachments()
    
#     # Ou sem filtros (busca todos os não lidos):
#     # fetch_csv_attachments()